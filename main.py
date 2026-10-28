"""大肥鱼·私聊消息聚合插件

私聊连发多条时不再逐条抢答：
- 第一条消息进入等待窗（默认9秒），期间每来一条就重置倒计时
- 窗口耗尽（用户停手超过窗口时长）后，把所有消息合并成一条交给正常管线，只回一次
- 命令消息（默认 / 开头，可配置）直通不等待
- 群聊消息直接放行，完全不受影响
"""

import asyncio

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, register


@register(
    "astrbot_plugin_dafeiyu_merge",
    "29624",
    "私聊连发消息聚合：等待窗口内合并成一条再回复，指令豁免",
    "1.0.1",
)
class DafeiyuMergePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config or {}
        self.window = float(self.config.get("merge_window_seconds", 9))
        self.max_hold = float(self.config.get("max_hold_seconds", 30))
        self.max_buffer = int(self.config.get("max_buffer_messages", 10))
        self.command_prefixes_cfg = self.config.get("command_prefixes", ["/"])
        # unified_msg_origin -> {"events": [...], "signal": asyncio.Event}
        self._batches: dict = {}

    def _command_prefixes(self) -> tuple:
        prefixes = [str(p) for p in (self.command_prefixes_cfg or []) if str(p)]
        if not prefixes:
            # 未配置时回退到平台唤醒前缀，保持与指令体系一致
            try:
                wp = self.context.astrbot_config.get("platform_settings", {}).get(
                    "wake_prefix", []
                )
                if isinstance(wp, str):
                    wp = [wp]
                prefixes = [str(p) for p in wp if str(p)]
            except Exception:
                prefixes = []
        return tuple(prefixes) or ("/",)

    @filter.event_message_type(filter.EventMessageType.ALL, priority=100)
    async def on_private_message(self, event: AstrMessageEvent):
        umo = event.unified_msg_origin
        if "FriendMessage" not in umo:
            return  # 只管私聊，群聊原样放行

        text = (event.message_str or "").strip()
        if text.startswith(self._command_prefixes()):
            return  # 指令直通，不进批次

        batch = self._batches.get(umo)
        if batch is not None:
            # 已有等待中的批次：并入并重置倒计时，然后终止本条的独立处理
            batch["events"].append(event)
            batch["signal"].set()
            event.stop_event()
            return

        # 本条开新批次：阻塞在处理器里等后续消息
        signal = asyncio.Event()
        batch = {"events": [event], "signal": signal}
        self._batches[umo] = batch
        try:
            start = asyncio.get_running_loop().time()
            deadline = start + self.max_hold
            while True:
                count_before = len(batch["events"])
                signal.clear()
                if len(batch["events"]) == count_before:
                    # 清信号瞬间没有新消息并入，正常等窗口
                    try:
                        await asyncio.wait_for(signal.wait(), timeout=self.window)
                    except asyncio.TimeoutError:
                        break  # 窗口内没有新消息 = 用户停手，冲刷
                # 走到这里 = 收到信号，或清信号瞬间恰好有消息并入（信号被误清）
                # 两种情况都视作"有新消息"，倒计时照常重置
                now = asyncio.get_running_loop().time()
                if now >= deadline or len(batch["events"]) >= self.max_buffer:
                    break  # 超过最长等待或条数上限，提前冲刷

            events = batch["events"]
            merged = events[0]
            if len(events) > 1:
                chain = []
                texts = []
                for ev in events:
                    chain.extend(ev.message_obj.message or [])
                    t = (ev.message_str or "").strip()
                    if t:
                        texts.append(t)
                merged.message_obj.message = chain
                merged.message_str = "\n".join(texts)
                logger.info(
                    f"[私聊聚合] 合并 {len(events)} 条消息（窗口{self.window}s），一次性回复"
                )
        finally:
            # 只清理自己持有的批次，避免误删随后新开的批次
            if self._batches.get(umo) is batch:
                self._batches.pop(umo, None)
        # 故意不调用 stop_event：合并后的首条事件继续走正常管线（人设/记忆/LLM）
