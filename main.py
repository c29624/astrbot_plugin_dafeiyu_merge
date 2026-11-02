"""大肥鱼·私聊消息聚合插件

私聊连发多条时不再逐条抢答；附带私聊分段回复（框架对私聊默认路径不分段，这里补上）：
- 第一条消息进入等待窗（默认12秒），期间每来一条就重置倒计时
- 窗口耗尽（用户停手超过窗口时长）后，把所有消息合并成一条交给正常管线，只回一次
- 命令消息（默认 / ! 开头，可配置）直通不等待——按消息链原文判定
  （框架 waking_check 会先剥掉 message_str 的唤醒前缀，直接读 message_str 会误判）
- 连发合并轮随 LLM 请求一次性注入"简短回应"提示（可关）——治"你发两条她回六七条"
- v1.3.0：分段切分括号感知——颜文字 (・_・。)/(？_？) 内部的句末标点不再把脸劈成两段；
  每次私聊请求注入"回复长度纪律"（可关）——治模型被上下文里自己往日长回复锚定
- 群聊消息直接放行，完全不受影响
"""

import asyncio
import random
import re

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, register

try:
    from astrbot.core.agent.message import TextPart
except ImportError:  # 版本变动时的兜底（提示注入失败不影响消息处理）
    TextPart = None

DEFAULT_LEN_HINT = (
    "[回复长度·私聊] 像随手回消息，别像写作文：平常一两句短句就够，"
    "只有对方明确想深聊或事情复杂才多说几条；（）动作描写一条回复最多带一个，"
    "不是每条都要；宁可短，别刷屏。"
)

_BRACKETS = "()（）"


def _bracket_near(text: str, i: int, j: int) -> bool:
    """标点串 [i,j) 前后各 2 字符内是否有括号——有则视作颜文字的组成部分"""
    return any(c in _BRACKETS for c in text[max(0, i - 2):i]) or \
        any(c in _BRACKETS for c in text[j:j + 2])


@register(
    "astrbot_plugin_dafeiyu_merge",
    "c29624",
    "私聊连发聚合+分段回复（括号感知不劈颜文字）+回复长度纪律注入",
    "1.3.0",
)
class DafeiyuMergePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config or {}
        self.window = float(self.config.get("merge_window_seconds", 12))
        self.max_hold = float(self.config.get("max_hold_seconds", 30))
        self.max_buffer = int(self.config.get("max_buffer_messages", 10))
        self.command_prefixes_cfg = self.config.get("command_prefixes", ["/", "!"])
        self.compact_on = bool(self.config.get("merged_compact_enabled", True))
        self.compact_tpl = str(
            self.config.get(
                "merged_compact_template",
                "[系统信息-私聊聚合] 对方刚连发了 {count} 条消息，你已一次性全部看到。"
                "像真人回消息那样简短：一两句就好，别长篇大论。",
            )
        )
        # unified_msg_origin -> {"events": [...], "signal": asyncio.Event}
        self._batches: dict = {}
        # umo -> 刚冲刷的合并条数（on_llm_request 消费一次即弹）
        self._merge_counts: dict = {}
        # 私聊分段回复（v1.2.0）：框架对私聊默认 LLM 路径不分段，这里补
        self.seg_on = bool(self.config.get("seg_enabled", True))
        raw_iv = str(self.config.get("seg_interval", "1.2,3.0"))
        try:
            iv = [float(t) for t in raw_iv.replace(" ", "").split(",")]
            if len(iv) != 2 or iv[0] > iv[1] or iv[0] < 0:
                raise ValueError
        except Exception:
            iv = [1.2, 3.0]
        self.seg_iv = iv
        # v1.3.0 回复长度纪律：每请求注入（不进历史），治上下文长回复范文的自锚定
        self.len_on = bool(self.config.get("seg_len_enabled", True))
        _lh = self.config.get("seg_len_hint")
        self.len_hint = (
            DEFAULT_LEN_HINT if _lh is None or not str(_lh).strip() else str(_lh).strip()
        )
        # 已知脸索引缓存：(faces 的 _pool_of 对象, first_char → [脸按长度降序])
        self._face_index_cache = None

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

    def _chain_head_text(self, event: AstrMessageEvent) -> str:
        """消息链第一个 Plain 的原文。框架会剥掉 message_str 的唤醒前缀（如 /help → help），
        指令豁免必须看链原文，否则私聊指令会被误扣窗口"""
        try:
            for comp in (event.message_obj.message or []):
                if type(comp).__name__ == "Plain":
                    return (getattr(comp, "text", "") or "").lstrip()
        except Exception:
            pass
        return (event.message_str or "").strip()

    @filter.event_message_type(filter.EventMessageType.ALL, priority=100)
    async def on_private_message(self, event: AstrMessageEvent):
        umo = event.unified_msg_origin
        if "FriendMessage" not in umo:
            return  # 只管私聊，群聊原样放行

        text = (event.message_str or "").strip()
        head = self._chain_head_text(event)
        if text.startswith(self._command_prefixes()) or head.startswith(
            self._command_prefixes()
        ):
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
                self._merge_counts[umo] = len(events)
                logger.info(
                    f"[私聊聚合] 合并 {len(events)} 条消息（窗口{self.window}s），一次性回复"
                )
        finally:
            # 只清理自己持有的批次，避免误删随后新开的批次
            if self._batches.get(umo) is batch:
                self._batches.pop(umo, None)
        # 故意不调用 stop_event：合并后的首条事件继续走正常管线（人设/记忆/LLM）

    @filter.on_llm_request()
    async def on_llm_request(self, event: AstrMessageEvent, req=None):
        """合并轮的紧凑提示：只在刚发生合并（≥2条）的下一次 LLM 请求注入一次，
        mark_as_temp 不进历史——她平时单条消息的回复风格不受影响"""
        try:
            if TextPart is None or req is None:
                return
            umo = event.unified_msg_origin
            if "FriendMessage" not in umo:
                return
            # v1.3.0 长度纪律：每次私聊请求都带（mark_as_temp 不进历史）。人设里的短句
            # 规则是软的——上下文里躺着自己几十条长回复时，模型照"范文"学不照规则学
            # （与颜文字锚定同病）；这条每请求在场，跟人设解耦
            if self.len_on and self.len_hint:
                req.extra_user_content_parts.append(TextPart(text=self.len_hint).mark_as_temp())
            n = self._merge_counts.pop(umo, 0)
            if n < 2 or not self.compact_on or not self.compact_tpl:
                return
            try:
                hint = self.compact_tpl.format(count=n)
            except Exception:
                hint = self.compact_tpl
            req.extra_user_content_parts.append(TextPart(text=hint).mark_as_temp())
            logger.info(f"[私聊聚合] 合并轮紧凑提示已注入（{n} 条）")
        except Exception:
            logger.warning("[私聊聚合] 紧凑提示注入失败（不影响消息处理）", exc_info=True)

    # ---------- 私聊分段回复（v1.2.0；v1.3.0 切分护脸） ----------

    def _face_index(self):
        """已知脸索引（软耦合 faces 插件，与 dm 取风格注入同款通道）：命中即整脸
        原子跳过，1558 张词典脸零劈（含启发式够不着的装饰脸。・゚・(ノД`)・゚・。、
        鱼脸 ω~~~＞°))))彡、词脸组合 おやすみ！(˘ω˘)zzz）。faces 没装 → None，
        走括号深度+贴身启发式"""
        try:
            meta = self.context.get_registered_star("astrbot_plugin_dafeiyu_faces")
            pools = getattr(getattr(meta, "star_cls", None), "_pool_of", None)
        except Exception:
            pools = None
        if not isinstance(pools, dict) or not pools:
            return None
        if self._face_index_cache and self._face_index_cache[0] is pools:
            return self._face_index_cache[1]
        idx = {}
        for f in pools:
            if f:
                idx.setdefault(f[:1], []).append(f)
        for v in idx.values():
            v.sort(key=len, reverse=True)
        self._face_index_cache = (pools, idx)
        return idx

    def _split_segments(self, text: str) -> list:
        """按句切分：换行/句末标点串（。？！~…）后切一刀。三层护脸：
        ①faces 已知脸整脸原子跳过；②括号（）()内的标点不切；③括号没闭合时
        后半不切（宁可不切，不劈脸）。旧正则不认括号，(・_・。)/(？_？)/(~_~)
        这类脸内部带标点的会被拦腰劈成两段气泡"""
        idx = self._face_index()
        segs, buf, depth = [], [], 0
        i, n = 0, len(text)
        while i < n:
            if idx:
                hit = next(
                    (f for f in idx.get(text[i], ()) if len(f) > 1 and text.startswith(f, i)),
                    None,
                )
                if hit:
                    buf.append(hit)
                    i += len(hit)
                    continue
            ch = text[i]
            if ch in "(（":
                depth += 1
                buf.append(ch)
                i += 1
            elif ch in ")）":
                depth = max(0, depth - 1)
                buf.append(ch)
                i += 1
            elif depth == 0 and (ch in "。？！~…" or ch in "\r\n"):
                j = i + 1
                while j < n and text[j] in ("。？！~…" if ch not in "\r\n" else "\r\n"):
                    j += 1
                if ch in "。？！~…" and _bracket_near(text, i, j):
                    # 贴身保护：标点前后2字符内有括号 → 多半是脸的组成部分
                    # （。゚(ﾟ´Д`ﾟ)ﾟ。首尾的。/(_ _).。o○的睡眠泡泡/词脸组合"おやすみ！(˘ω˘)"）
                    buf.append(text[i:j])
                else:
                    buf.append(text[i:j])
                    segs.append("".join(buf))
                    buf = []
                i = j
            else:
                buf.append(ch)
                i += 1
        if buf:
            segs.append("".join(buf))
        return [s.strip() for s in segs if s.strip()]

    @filter.on_decorating_result()
    async def segment_private_reply(self, event: AstrMessageEvent, *args, **kwargs):
        """框架的分段回复对私聊默认 LLM 路径不生效（实测单条长文本整发），
        这里在私聊范围内补齐：LLM 回复按句切分、逐条发送、条间随机间隔。
        只动 LLM 结果（is_model_result），指令/工具消息原样走框架。"""
        if not self.seg_on:
            return
        try:
            if "FriendMessage" not in event.unified_msg_origin:
                return
            result = event.get_result()
            if not result or not getattr(result, "chain", None):
                return
            if not result.is_model_result():
                return
            plains = [c for c in result.chain if isinstance(c, Plain)]
            if not plains:
                return
            text = "\n".join((p.text or "") for p in plains).strip()
            if not text:
                return
            segs = self._split_segments(text)
            if len(segs) <= 1:
                return
            others = [c for c in result.chain if not isinstance(c, Plain)]
            result.chain = []  # 清空：respond 阶段跳过，避免整条重发
            for i, seg in enumerate(segs):
                if i:
                    await asyncio.sleep(random.uniform(*self.seg_iv))
                try:
                    await event.send(MessageChain().message(seg))
                except Exception:
                    logger.warning(f"[私聊聚合] 分段第{i + 1}条发送失败", exc_info=True)
            if others:  # 表情包等非文本组件放最后单独一条
                try:
                    chain = MessageChain()
                    chain.chain = list(others)
                    await event.send(chain)
                except Exception:
                    logger.warning("[私聊聚合] 分段尾部组件发送失败", exc_info=True)
            logger.info(f"[私聊聚合] 分段回复：{len(segs)} 条（间隔{self.seg_iv[0]}~{self.seg_iv[1]}s）")
        except Exception:
            logger.warning("[私聊聚合] 分段处理异常（回退框架整发需结果未清空）", exc_info=True)
