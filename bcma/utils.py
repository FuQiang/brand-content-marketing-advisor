"""Utility helpers for field value handling and simple heuristics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


def get_text_field(fields: Dict[str, Any], field_name: str, default: str = "") -> str:
    """Extract plain text from a Text field read shape.

    - Text: `[{"text": "...", "type": "text"}]` → join text
    - String: return as-is
    - Missing/other types: return default
    """

    if field_name not in fields or fields[field_name] is None:
        return default

    value = fields[field_name]
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: List[str] = []
        for item in value:
            if isinstance(item, dict) and "text" in item:
                parts.append(str(item.get("text", "")))
            else:
                parts.append(str(item))
        return "".join(parts) if parts else default

    return default


def get_number_field(fields: Dict[str, Any], field_name: str, default: float = 0.0) -> float:
    if field_name not in fields or fields[field_name] is None:
        return default
    value = fields[field_name]
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except Exception:
        return default


def get_multi_select_field(fields: Dict[str, Any], field_name: str) -> List[str]:
    """Return option names for MultiSelect field.

    Read shape: `string[]` (see record-fields.md)。
    """

    value = fields.get(field_name)
    if isinstance(value, list) and all(isinstance(x, str) for x in value):
        return list(value)
    return []


def now_ts_ms() -> int:
    """Current timestamp in milliseconds (UTC)."""

    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def parse_ts_to_ms(raw: str | int | float | None) -> int:
    """Best-effort parse of input timestamp into ms.

    支持：
    - 毫秒/秒整数
    - ISO 字符串（`YYYY-MM-DD` / `YYYY-MM-DD HH:MM[:SS]`）
    解析失败时回退为当前时间。
    """

    if raw is None:
        return now_ts_ms()

    # Numeric path
    if isinstance(raw, (int, float)):
        v = int(raw)
        # 粗略判断：> 1e12 视为毫秒，其余视为秒
        if v > 10**12:
            return v
        return v * 1000

    s = str(raw).strip()
    if not s:
        return now_ts_ms()

    # Try pure integer string
    if s.isdigit():
        v = int(s)
        if v > 10**12:
            return v
        return v * 1000

    # Try ISO-like formats
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
        except Exception:
            continue

    return now_ts_ms()


# --------------------------- Persona heuristics ---------------------------

PERSONA_KEYWORDS = {
    "新锐白领": ["白领", "通勤", "职场", "写字楼", "办公室"],
    "精致妈妈": ["妈妈", "宝妈", "育儿", "亲子", "遛娃"],
    "学生党": ["学生", "大学", "校园", "上课", "考研"],
    "资深打工人": ["打工人", "社畜", "加班", "搬砖"],
    "户外玩家": ["户外", "露营", "徒步", "滑雪", "登山"],
    "品质中产": ["中产", "品质生活", "精致生活"],
    "潮流青年": ["潮流", "街头", "酷", "嘻哈"],
}


def infer_persona(text: str) -> Optional[str]:
    """Infer persona tag from topic/raw_text using simple keyword rules."""

    haystack = text.lower()
    # 粗暴中文匹配：统一用原始文本做包含判断
    for persona, keywords in PERSONA_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in haystack:
                return persona
    return None


@dataclass
class CandidateTopic:
    topic: str
    source: str
    timestamp: str
    raw_text: str

    def to_debug_str(self) -> str:
        return f"[{self.source}] {self.topic}"
