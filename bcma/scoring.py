"""Local regex filtering and 4R scoring logic.

v5.3.0 改动：
- 4R 打分改为 1-5 分制（与品牌模板一致），总分 = R1+R2+R3+R4（满分 20）
- 支持从 BrandTopicRules 表读取品牌专属 prompt 进行 LLM 打分
- 新增 R4 否决机制：R4 <= r4_veto_threshold 时无论总分一律 PASS
- 决策阈值：总分 >= 16 且 R4 >= 4 -> 主推; 13-15 且 R4 >= 3 -> 备选
- LLM 返回的 JSON 同时包含 content_direction（内容方向建议）
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .llm_client import call_llm_text
from .utils import CandidateTopic

logger = logging.getLogger("bcma.scoring")


@dataclass
class RuleResult:
    allowed: bool
    reason: str
    hits: List[str]


def apply_regex_rules(candidate: CandidateTopic, cfg: Dict[str, Any]) -> RuleResult:
    """Apply local regex-based pre-filter."""

    text = f"{candidate.topic}\n{candidate.raw_text}"

    blacklist = cfg.get("blacklist_patterns", []) or []
    whitelist = cfg.get("whitelist_patterns", []) or []
    allow_patterns = cfg.get("allow_patterns", []) or []

    def _safe_search(pattern: str, text: str) -> bool:
        try:
            return bool(re.search(pattern, text, flags=re.IGNORECASE))
        except re.error as e:
            logger.warning("regex invalid, skipped pattern='%s': %s", pattern, e)
            return False

    hits: List[str] = []

    for pattern in blacklist:
        if _safe_search(pattern, text):
            return RuleResult(allowed=False, reason=f"blacklist: {pattern}", hits=[pattern])

    whitelist_hits: List[str] = []
    for pattern in whitelist:
        if _safe_search(pattern, text):
            whitelist_hits.append(pattern)

    if allow_patterns:
        allow_hit = False
        for pattern in allow_patterns:
            if _safe_search(pattern, text):
                allow_hit = True
                hits.append(pattern)
        if not allow_hit:
            return RuleResult(allowed=False, reason="no allow_patterns hit", hits=hits)

    reason_parts: List[str] = []
    if whitelist_hits:
        hits.extend(whitelist_hits)
        reason_parts.append("whitelist hit")
    if not reason_parts:
        reason_parts.append("passed")

    return RuleResult(allowed=True, reason="; ".join(reason_parts), hits=hits)


@dataclass
class FourRScore:
    relevance: float   # R1 (1-5)
    resonance: float   # R2 (1-5)
    reach: float       # R3 (1-5)
    revenue: float     # R4 / Risk (1-5)
    total: float       # R1+R2+R3+R4
    decision_label: Optional[str]    # "✅ 主推" / "🟡 备选" / None
    content_direction: str = ""      # LLM 给出的内容方向建议
    one_line_reason: str = ""        # 一句话理由


def _apply_decision(
    r1: float, r2: float, r3: float, r4: float, total: float,
    scoring_cfg: Dict[str, Any],
) -> Optional[str]:
    """Apply decision rules based on new 1-5 scoring system."""
    main_threshold = float(scoring_cfg.get("threshold_main", 16))
    candidate_threshold = float(scoring_cfg.get("threshold_candidate", 13))
    r4_veto = float(scoring_cfg.get("r4_veto_threshold", 2))

    # R4 veto: any topic with R4 <= threshold is PASS regardless of total
    if r4 <= r4_veto:
        return None

    if total >= main_threshold and r4 >= 4:
        return "✅ 主推"
    if total >= candidate_threshold and r4 >= 3:
        return "🟡 备选"
    return None


# ---------------------------------------------------------------------------
#  Heuristic fallback (1-5 scale)
# ---------------------------------------------------------------------------

def _deterministic_seed(text: str) -> int:
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


def compute_4r_score(
    candidate: CandidateTopic,
    rule_result: RuleResult,
    scoring_cfg: Dict[str, Any],
) -> FourRScore:
    """Compute 4R scores in [1,5] using deterministic heuristics as fallback."""

    seed = _deterministic_seed(candidate.topic + "\n" + candidate.raw_text)

    r1 = 1 + (seed % 5)
    r2 = 1 + ((seed // 7) % 5)
    r3 = 1 + ((seed // 13) % 5)
    r4 = 1 + ((seed // 17) % 5)

    # whitelist hit gives a small boost
    if rule_result.hits:
        r1 = min(5, r1 + 1)

    total = r1 + r2 + r3 + r4
    decision = _apply_decision(r1, r2, r3, r4, total, scoring_cfg)

    return FourRScore(
        relevance=r1,
        resonance=r2,
        reach=r3,
        revenue=r4,
        total=total,
        decision_label=decision,
        one_line_reason=f"heuristic, rule_hits={rule_result.hits}",
    )


# ---------------------------------------------------------------------------
#  LLM-assisted scoring with brand-specific prompt
# ---------------------------------------------------------------------------

_BRAND_SCORING_PROMPT = """\
{brand_rules_prompt}

---

请对以下候选话题进行 4R 打分。严格按 JSON 返回，不要加多余说明：

{{
  "relevance": <1-5 整数>,
  "resonance": <1-5 整数>,
  "reach": <1-5 整数>,
  "risk": <1-5 整数>,
  "total": <四项之和>,
  "decision": "✅ 主推 / 🟡 备选 / ❌ PASS",
  "one_line_reason": "一句话理由",
  "content_direction": "如果是主推或备选，给出内容方向建议（100字以内）；如果 PASS 则留空"
}}

[候选话题]
{topic}

[原始文本]
{raw_text}
"""

_GENERIC_SCORING_PROMPT = """\
你是一名资深品牌内容营销顾问，需要基于 4R 体系对社会话题进行打分（1-5 分）。

- R1 Relevance：与消费/品牌营销场景的关联度
- R2 Resonance：能否激发目标人群共鸣与讨论
- R3 Reach：潜在传播广度与话题扩散空间
- R4 Risk：舆情风险（5=安全，1=高危）

决策规则：
- 总分 >= 16 且 R4 >= 4 -> 主推
- 总分 13-15 且 R4 >= 3 -> 备选
- R4 <= 2 或总分 < 13 -> PASS

严格按 JSON 返回，不要加多余说明：

{{
  "relevance": <1-5 整数>,
  "resonance": <1-5 整数>,
  "reach": <1-5 整数>,
  "risk": <1-5 整数>,
  "total": <四项之和>,
  "decision": "✅ 主推 / 🟡 备选 / ❌ PASS",
  "one_line_reason": "一句话理由",
  "content_direction": "内容方向建议（主推/备选时填写，PASS 留空）"
}}

[候选话题]
{topic}

[原始文本]
{raw_text}
"""


def _call_llm_for_4r(
    candidate: CandidateTopic,
    model_cfg: Dict[str, Any],
    brand_rules_prompt: str = "",
) -> Optional[Dict[str, Any]]:
    """Call LLM for 4R scoring via unified llm_client."""

    topic_text = candidate.topic.strip()
    raw_text = (candidate.raw_text or "").strip()

    if brand_rules_prompt:
        prompt = _BRAND_SCORING_PROMPT.format(
            brand_rules_prompt=brand_rules_prompt,
            topic=topic_text,
            raw_text=raw_text,
        )
    else:
        prompt = _GENERIC_SCORING_PROMPT.format(
            topic=topic_text,
            raw_text=raw_text,
        )

    text = call_llm_text(prompt, model_cfg, max_tokens=1024, temperature=0.0)
    if text:
        return _parse_4r_json(text)
    return None


def _parse_4r_json(text: str) -> Optional[Dict[str, Any]]:
    """Parse 4R JSON from LLM response."""
    if not text:
        return None
    try:
        # Handle markdown code blocks
        m = re.search(r"```(?:json)?\s*\n?([\s\S]*?)```", text)
        if m:
            text = m.group(1).strip()
        m = re.search(r"\{[\s\S]*\}", text)
        if not m:
            return None
        data = json.loads(m.group(0))
        if not isinstance(data, dict):
            return None

        result = {
            "relevance": int(data.get("relevance", 0)),
            "resonance": int(data.get("resonance", 0)),
            "reach": int(data.get("reach", 0)),
            "risk": int(data.get("risk", data.get("revenue", 0))),
            "one_line_reason": str(data.get("one_line_reason", "")),
            "content_direction": str(data.get("content_direction", "")),
        }
        total = data.get("total")
        if total is None:
            total = result["relevance"] + result["resonance"] + result["reach"] + result["risk"]
        result["total"] = int(total)
        return result
    except Exception as e:
        logger.warning("Failed to parse 4R JSON: %s", e)
        return None


class LLMScoringError(RuntimeError):
    """LLM 4R 打分失败（请求/解析/返回 None）。调用方应当丢弃该候选，
    而不是回落到基于 SHA256 的伪随机启发式——伪随机 R4 有 ~40% 概率落入
    veto 区间，会污染 Top K 结果。"""


def compute_4r_score_with_model(
    candidate: CandidateTopic,
    rule_result: RuleResult,
    scoring_cfg: Dict[str, Any],
    model_cfg: Optional[Dict[str, Any]] = None,
    brand_rules_prompt: str = "",
    heuristic_fallback: bool = True,
) -> FourRScore:
    """LLM 4R scoring with brand-specific prompt.

    Args:
        brand_rules_prompt: BrandTopicRules 表中该品牌的筛选逻辑 prompt。
            传入非空字符串时，会将其作为 system context 注入 LLM 请求。
        heuristic_fallback: LLM 不可用/失败时是否回落启发式打分。
            True（默认）= 兼容旧 upstream 流水线行为；
            False = 直接 raise LLMScoringError，由调用方丢弃候选。
            daily_topics (Step 5) 必须传 False，避免伪随机结果进入 Top K。
    """

    if not model_cfg:
        if heuristic_fallback:
            return compute_4r_score(candidate, rule_result, scoring_cfg)
        raise LLMScoringError("model_cfg is empty; no usable LLM configured")

    scores = _call_llm_for_4r(candidate, model_cfg, brand_rules_prompt=brand_rules_prompt)

    if not scores:
        if heuristic_fallback:
            return compute_4r_score(candidate, rule_result, scoring_cfg)
        raise LLMScoringError(f"LLM returned no parseable 4R scores for topic={candidate.topic!r}")

    r1 = float(scores.get("relevance", 0))
    r2 = float(scores.get("resonance", 0))
    r3 = float(scores.get("reach", 0))
    r4 = float(scores.get("risk", 0))
    total = float(scores.get("total", r1 + r2 + r3 + r4))

    decision = _apply_decision(r1, r2, r3, r4, total, scoring_cfg)

    return FourRScore(
        relevance=r1,
        resonance=r2,
        reach=r3,
        revenue=r4,
        total=total,
        decision_label=decision,
        content_direction=str(scores.get("content_direction", "")),
        one_line_reason=str(scores.get("one_line_reason", "")),
    )
