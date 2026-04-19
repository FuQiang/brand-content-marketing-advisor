"""Upstream pipeline: ingest candidates -> regex filter -> 4R scoring -> write TopicSelection.

v5.3.0:
- 支持 --brand 参数，从 BrandTopicRules 表读取品牌专属 prompt 注入 LLM 打分
- 4R 改为 1-5 分制，决策逻辑对齐品牌模板
- 写入 content_direction（内容方向建议）和 brand（适用品牌）字段
"""

from __future__ import annotations

import csv
import json
import logging
import re
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from .bitable import add_single, search_all_records
from .config import Config
from .product_assets import cleanup_empty_columns_for_products
from .scoring import (
    FourRScore,
    RuleResult,
    apply_regex_rules,
    compute_4r_score_with_model,
)
from .utils import CandidateTopic, get_text_field, infer_persona, now_ts_ms, parse_ts_to_ms

logger = logging.getLogger("bcma.upstream")


def _load_candidates_from_csv(path: Path) -> List[CandidateTopic]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        result: List[CandidateTopic] = []
        for row in reader:
            topic = (row.get("topic") or "").strip()
            if not topic:
                continue
            result.append(
                CandidateTopic(
                    topic=topic,
                    source=(row.get("source") or "").strip(),
                    timestamp=(row.get("timestamp") or "").strip(),
                    raw_text=(row.get("raw_text") or "").strip(),
                )
            )
    return result


def _load_candidates_from_json(path: Path) -> List[CandidateTopic]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        items = data.get("items", []) if isinstance(data.get("items"), list) else []
    else:
        items = data

    result: List[CandidateTopic] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        topic = (item.get("topic") or "").strip()
        if not topic:
            continue
        result.append(
            CandidateTopic(
                topic=topic,
                source=(item.get("source") or "").strip(),
                timestamp=str(item.get("timestamp") or "").strip(),
                raw_text=(item.get("raw_text") or "").strip(),
            )
        )
    return result


def load_candidates(input_path: str) -> List[CandidateTopic]:
    path = Path(input_path)
    if not path.is_file():
        raise FileNotFoundError(f"file not found: {input_path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _load_candidates_from_csv(path)
    if suffix == ".json":
        return _load_candidates_from_json(path)

    raise ValueError("only CSV or JSON supported")


# BrandTopicRules 中存的是一整份"周会批量筛选报告" prompt，含输出格式/反漏斗
# 等章节。但 run_upstream 是 per-topic JSON 打分，如果把这些段注入进去，LLM 会
# 在表格输出和 JSON 输出之间混乱。这里只抽取「品牌底层认知 + 4R 筛选法则 + 人设
# 审美一致性校验」三段作为 scoring context，完整 prompt 原样保留在 Bitable 供
# 周会手工 copy 使用。
_SCORING_SECTION_START_HEADERS = (
    "# Role",
    "# 品牌底层认知",
    "# 4R 筛选法则",
    "# 人设审美一致性校验",
)
# 反漏斗机制 / 输出格式 及之后章节在 per-topic 打分时会干扰 JSON 输出，一律丢弃。
_SCORING_SECTION_STOP_PATTERN = re.compile(r"^#\s+(反漏斗机制|输出格式)", re.MULTILINE)


def extract_scoring_sections(full_prompt: str) -> str:
    """从完整 BrandTopicRules prompt 中抽取打分相关章节。

    策略：找到第一个停止标记 (# 反漏斗机制 / # 输出格式) 并截断之前的内容。
    如果找不到停止标记，返回原文（向后兼容手工写入的短 prompt）。
    """
    if not full_prompt:
        return ""

    text = full_prompt
    m = _SCORING_SECTION_STOP_PATTERN.search(text)
    if m:
        text = text[: m.start()].rstrip()

    # 合法性兜底：至少要命中一个核心打分章节，否则回退到原文避免空注入。
    has_scoring_header = any(h in text for h in _SCORING_SECTION_START_HEADERS)
    if not has_scoring_header:
        return full_prompt.strip()

    return text.strip()


def load_brand_rules_prompt(cfg: Config, brand: str) -> str:
    """Load brand-specific 4R scoring prompt from BrandTopicRules table.

    只返回与「4R 打分」直接相关的章节（品牌底层认知 / 4R 法则 / 人设校验），
    剥离周会批量输出格式章节，避免 per-topic JSON 打分时格式冲突。
    Returns empty string if not found.
    """
    brand = (brand or "").strip()
    if not brand:
        return ""

    btr_cfg = cfg.tables.get("brand_topic_rules")
    if not btr_cfg:
        return ""
    tbl_id = btr_cfg.get("table_id", "")
    if not tbl_id:
        return ""

    f = cfg.fields.get("brand_topic_rules", {})
    name_field = f.get("name", "")
    rules_field = f.get("rules_prompt", "")

    try:
        records = search_all_records(cfg.app_token, tbl_id, page_size=50)
    except Exception as e:
        logger.warning("Failed to read BrandTopicRules: %s", e)
        return ""

    for item in records:
        fields = item.get("fields") or {}
        name = get_text_field(fields, name_field, "")
        if name == brand:
            full_prompt = get_text_field(fields, rules_field, "")
            return extract_scoring_sections(full_prompt)

    return ""


def _score_one_candidate(
    candidate: CandidateTopic,
    cfg: Config,
    brand_rules_prompt: str = "",
) -> Tuple[CandidateTopic, RuleResult, FourRScore]:
    rule_result = apply_regex_rules(candidate, cfg.regex_filters)
    if not rule_result.allowed:
        dummy_score = FourRScore(0, 0, 0, 0, 0, None)
        return candidate, rule_result, dummy_score

    model_cfg = cfg.select_model()
    score = compute_4r_score_with_model(
        candidate, rule_result, cfg.scoring, model_cfg,
        brand_rules_prompt=brand_rules_prompt,
    )
    return candidate, rule_result, score


def run_upstream_pipeline(
    cfg: Config,
    input_path: str,
    brand: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Run upstream pipeline: filter + score + write TopicSelection.

    Args:
        brand: Optional brand name. When provided, loads brand-specific 4R prompt
               from BrandTopicRules and writes brand field to TopicSelection.
    """

    # 0) Clean up empty columns
    products_fields_cfg = cfg.fields.get("products", {}) or {}
    non_removable_fields = products_fields_cfg.get("non_removable_fields")
    try:
        cleanup_empty_columns_for_products(
            app_token=cfg.app_token,
            tables_cfg=cfg.tables,
            fields_cfg=cfg.fields,
            non_removable_fields=non_removable_fields,
        )
    except Exception as e:
        logger.warning("Products column cleanup failed (non-blocking): %s", e)

    candidates = load_candidates(input_path)
    if not candidates:
        return []

    # Load brand-specific prompt if brand is provided
    brand_rules_prompt = ""
    if brand:
        brand_rules_prompt = load_brand_rules_prompt(cfg, brand)
        if brand_rules_prompt:
            logger.info(
                "Loaded brand-specific 4R rules for '%s' (scoring sections only, %d chars)",
                brand, len(brand_rules_prompt),
            )
        else:
            logger.info("No brand-specific rules found for '%s', using generic scoring", brand)

    concurrency_cfg = cfg.concurrency
    max_workers = int(concurrency_cfg.get("max_workers", 8)) or 1

    results: List[Tuple[CandidateTopic, RuleResult, FourRScore]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_score_one_candidate, c, cfg, brand_rules_prompt): c
            for c in candidates
        }
        for future in as_completed(future_map):
            try:
                candidate, rule_result, score = future.result()
                results.append((candidate, rule_result, score))
            except Exception as e:
                logger.warning("Scoring failed for a candidate: %s", e)

    app_token = cfg.app_token
    topic_tbl = cfg.tables["topic_selection"]["table_id"]
    f_cfg = cfg.fields["topic_selection"]

    created_records: List[Dict[str, Any]] = []

    for candidate, rule_result, score in results:
        if not score.decision_label:
            continue

        persona = infer_persona(candidate.topic + "\n" + candidate.raw_text) or ""
        now_ms = now_ts_ms()
        fetched_ms = parse_ts_to_ms(candidate.timestamp)

        one_line = score.one_line_reason or f"rule: {rule_result.reason}, total={score.total:.0f}"

        fields: Dict[str, Any] = {
            f_cfg["topic"]: candidate.topic,
            f_cfg["source"]: candidate.source,
            f_cfg["fetched_at"]: fetched_ms,
            f_cfg["created_at"]: now_ms,
            f_cfg["raw_text"]: candidate.raw_text,
            f_cfg["rule_hits"]: ",".join(rule_result.hits) if rule_result.hits else "",
            f_cfg["r1"]: round(score.relevance),
            f_cfg["r2"]: round(score.resonance),
            f_cfg["r3"]: round(score.reach),
            f_cfg["r4"]: round(score.revenue),
            f_cfg["total_score"]: round(score.total),
            f_cfg["decision"]: score.decision_label,
            f_cfg["one_line_reason"]: one_line,
        }

        # content_direction from LLM
        content_dir_field = f_cfg.get("content_direction")
        if content_dir_field and score.content_direction:
            fields[content_dir_field] = score.content_direction

        if persona:
            fields[f_cfg["audience"]] = persona

        # Write brand if provided
        brand_field = f_cfg.get("brand")
        if brand_field and brand:
            fields[brand_field] = brand

        try:
            resp = add_single(app_token, topic_tbl, fields)
        except Exception as e:
            logger.warning("Failed to write TopicSelection for '%s': %s", candidate.topic, e)
            continue

        record = None
        if isinstance(resp, dict):
            record = resp.get("record") or (resp.get("data") or {}).get("record")
        if record:
            created_records.append(record)

    return created_records
