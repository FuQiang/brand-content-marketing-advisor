"""Daily topic selection pipeline for v5.5.0 (Step 5).

第五步：从外部 Bitable「每日精选话题」表读取**过去 N 天**（默认 10 天）的热门
候选话题，使用 BrandTopicRules 中 Step 4 生成的品牌专属 4R prompt 进行并发打分，
按总分排序取 Top K（最匹配品牌的话题）写入 `TopicSelection` 表（打上"适用品牌"
字段），为第六步 `generate_brand_content`（文案/封面/视频）准备素材。

设计要点：
- 数据源跨 base：`daily_topics.app_token` 与主 base 的 `BCMA_APP_TOKEN` 不同，
  `bitable.py` 的读取函数天然支持按 app_token 参数切 base。
- 候选窗口：按 `daily_topics.lookback_days`（默认 10）回溯多天，扩大候选池，
  让 4R 打分从更丰富的话题中挑选最匹配品牌的内容。
- 去重窗口：仍按当日 [00:00, 次日 00:00) 查 TopicSelection，保证同一天重复
  跑 `select_topic` 不会重复写入。
- 4R 打分：完全复用 `upstream.py::load_brand_rules_prompt` + `scoring.py::
  compute_4r_score_with_model`，确保打分逻辑与第四步生成的 prompt 一致。
- 去重：按 (日期, 品牌, 话题名称) 三元组，查 `TopicSelection` 中当日该品牌
  已有记录，已存在则跳过，保证同一天重复跑 `select_topic` 不会重复写入。
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from .bitable import add_single, search_all_records
from .config import Config
from .scoring import FourRScore, RuleResult, compute_4r_score_with_model
from .upstream import load_brand_rules_prompt
from .utils import CandidateTopic, get_number_field, get_text_field, infer_persona, now_ts_ms

logger = logging.getLogger("bcma.daily_topics")


# ---------------------------------------------------------------------------
#  Time window helpers (Asia/Shanghai by default)
# ---------------------------------------------------------------------------

def _today_window_ms(tz_offset_hours: int, date_str: Optional[str] = None) -> Tuple[int, int, str]:
    """返回当日 [00:00, 次日 00:00) 的毫秒时间戳区间与日期字符串。

    Args:
        tz_offset_hours: 时区偏移（小时），北京时间为 8。
        date_str: 指定日期 'YYYY-MM-DD'；None 时使用当前时间所在日。

    Returns:
        (start_ms_inclusive, end_ms_exclusive, date_str)
    """
    tz = timezone(timedelta(hours=int(tz_offset_hours)))

    if date_str:
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
        except ValueError as e:
            raise ValueError(f"date 参数格式错误，应为 YYYY-MM-DD: {date_str}") from e
    else:
        now_local = datetime.now(tz=tz)
        day = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    start = day
    end = day + timedelta(days=1)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    return start_ms, end_ms, day.strftime("%Y-%m-%d")


def _lookback_window_ms(
    tz_offset_hours: int,
    lookback_days: int,
    date_str: Optional[str] = None,
) -> Tuple[int, int, str]:
    """返回 [N天前 00:00, 次日 00:00) 的毫秒时间戳区间，用于扩大候选话题池。

    Args:
        tz_offset_hours: 时区偏移（小时），北京时间为 8。
        lookback_days: 向前回溯天数（含当天）。10 = 当天 + 过去 9 天。
        date_str: 锚定日期 'YYYY-MM-DD'；None 时使用当前时间所在日。

    Returns:
        (start_ms_inclusive, end_ms_exclusive, anchor_date_str)
    """
    tz = timezone(timedelta(hours=int(tz_offset_hours)))

    if date_str:
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
        except ValueError as e:
            raise ValueError(f"date 参数格式错误，应为 YYYY-MM-DD: {date_str}") from e
    else:
        now_local = datetime.now(tz=tz)
        day = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    lookback = max(1, int(lookback_days))
    start = day - timedelta(days=lookback - 1)
    end = day + timedelta(days=1)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    return start_ms, end_ms, day.strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
#  Step A - Load candidates from external base (lookback window)
# ---------------------------------------------------------------------------

def _parse_date_field_to_ms(
    fields: Dict[str, Any],
    date_field: str,
    tz_offset_hours: int,
) -> int:
    """解析时间字段，支持多种格式：毫秒时间戳、秒时间戳、YYYY-MM-DD 文本。

    Returns:
        毫秒时间戳，解析失败返回 0。
    """
    # 1) 尝试数值型
    num_val = int(get_number_field(fields, date_field, default=0.0))
    if num_val:
        return num_val * 1000 if num_val < 10**12 else num_val

    # 2) 尝试文本型
    raw = get_text_field(fields, date_field, "").strip()
    if not raw:
        return 0

    # 2a) 纯数字（时间戳字符串）
    if raw.replace(".", "", 1).isdigit():
        try:
            ts = int(float(raw))
            return ts * 1000 if ts < 10**12 else ts
        except Exception:
            return 0

    # 2b) YYYY-MM-DD 日期字符串 → 当天 00:00 的毫秒时间戳
    tz = timezone(timedelta(hours=int(tz_offset_hours)))
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=tz)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue

    return 0


def _build_raw_text(fields: Dict[str, Any], f_map: Dict[str, str]) -> str:
    """从多个字段拼接 raw_text，供 4R 打分使用。

    当 config 中 raw_text 指向单一字段且有值时直接用；否则从扩展字段拼接。
    """
    # 如果 raw_text 配置了具体字段名且该字段有值，直接用
    raw_text_field = (f_map.get("raw_text") or "").strip()
    if raw_text_field:
        val = get_text_field(fields, raw_text_field, "").strip()
        if val:
            return val

    # 拼接扩展字段
    parts: List[str] = []
    label_map = [
        ("category", "归类"),
        ("content_direction", "承接方向"),
        ("suggested_action", "建议动作"),
        ("target_audience", "目标人群"),
        ("heat", "今日热度"),
        ("heat_delta", "日增幅"),
        ("rising_days", "连涨天数"),
    ]
    for key, label in label_map:
        field_name = f_map.get(key, "")
        if not field_name:
            continue
        val = get_text_field(fields, field_name, "").strip()
        if not val or val == "null":
            continue
        parts.append(f"{label}: {val}")

    return "\n".join(parts)


def _fetch_daily_topics(
    cfg: Config,
    start_ms: int,
    end_ms: int,
) -> List[CandidateTopic]:
    """从外部 base 读取时间窗口内的候选话题，字段映射由 config.daily_topics.fields 决定。

    支持多种时间字段格式（毫秒/秒时间戳、YYYY-MM-DD 文本）以及 raw_text 多字段拼接。
    若字段不存在或无法判定时间，该条记录会被跳过。
    """
    dt_cfg = cfg.raw.get("daily_topics") or {}
    app_token = (dt_cfg.get("app_token") or "").strip()
    table_id = (dt_cfg.get("table_id") or "").strip()
    view_id = (dt_cfg.get("view_id") or "").strip() or None
    tz_offset = int(dt_cfg.get("timezone_offset_hours", 8))

    if not app_token or not table_id:
        logger.warning(
            "daily_topics.app_token / daily_topics.table_id 未配置，无法读取每日精选话题表。"
            "请在 config.yaml 的 daily_topics 段填写外部数据源信息。"
        )
        return []

    f_map = dt_cfg.get("fields") or {}
    topic_field = f_map.get("topic", "话题")
    source_field = f_map.get("source", "平台")
    created_field = f_map.get("created_at", "日期")
    fetched_field = f_map.get("fetched_at", "日期")

    records = search_all_records(
        app_token=app_token,
        table_id=table_id,
        view_id=view_id,
        automatic_fields=True,
        page_size=200,
    )
    if not records:
        logger.info("每日精选话题表当前无记录")
        return []

    candidates: List[CandidateTopic] = []
    skipped_no_topic = 0
    skipped_out_of_window = 0
    skipped_no_time = 0

    for item in records:
        fields = item.get("fields") or {}

        topic_text = get_text_field(fields, topic_field, "")
        if not topic_text:
            skipped_no_topic += 1
            continue

        # 时间解析（支持时间戳 + YYYY-MM-DD 文本）
        created_ms = _parse_date_field_to_ms(fields, created_field, tz_offset)

        # 兜底：读自动字段 created_time
        if not created_ms:
            auto_ct = fields.get("created_time")
            if isinstance(auto_ct, (int, float)):
                created_ms = int(auto_ct)
                if created_ms < 10**12:
                    created_ms *= 1000

        if not created_ms:
            skipped_no_time += 1
            continue

        if not (start_ms <= created_ms < end_ms):
            skipped_out_of_window += 1
            continue

        source = get_text_field(fields, source_field, "")
        raw_text = _build_raw_text(fields, f_map)

        # 抓取时间
        fetched_ms = _parse_date_field_to_ms(fields, fetched_field, tz_offset)
        if not fetched_ms:
            fetched_ms = created_ms

        candidates.append(
            CandidateTopic(
                topic=topic_text,
                source=source,
                timestamp=str(fetched_ms),
                raw_text=raw_text,
            )
        )

    logger.info(
        "每日精选话题读取完成: total=%d, candidates=%d, skipped(no_topic=%d, no_time=%d, out_of_window=%d)",
        len(records),
        len(candidates),
        skipped_no_topic,
        skipped_no_time,
        skipped_out_of_window,
    )
    return candidates


# ---------------------------------------------------------------------------
#  Step B - 4R scoring with brand-specific prompt
# ---------------------------------------------------------------------------

def _score_one(
    candidate: CandidateTopic,
    cfg: Config,
    brand_rules_prompt: str,
) -> Tuple[CandidateTopic, FourRScore]:
    """单个候选话题的 4R 打分。每日精选话题表已是清洗过的数据源，不再走
    本地正则黑白名单，直接构造一个"全部允许"的 RuleResult 交给 LLM。
    """
    dummy_rule = RuleResult(allowed=True, reason="daily_topics pre-curated", hits=[])
    model_cfg = cfg.select_model()
    score = compute_4r_score_with_model(
        candidate,
        dummy_rule,
        cfg.scoring,
        model_cfg,
        brand_rules_prompt=brand_rules_prompt,
    )
    return candidate, score


def _score_candidates_concurrently(
    cfg: Config,
    candidates: List[CandidateTopic],
    brand_rules_prompt: str,
) -> List[Tuple[CandidateTopic, FourRScore]]:
    """并发打分，线程数由 concurrency.max_workers 控制。"""
    if not candidates:
        return []

    concurrency_cfg = cfg.concurrency
    max_workers = int(concurrency_cfg.get("max_workers", 8)) or 1

    results: List[Tuple[CandidateTopic, FourRScore]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_score_one, c, cfg, brand_rules_prompt): c
            for c in candidates
        }
        for future in as_completed(future_map):
            try:
                cand, score = future.result()
                results.append((cand, score))
            except Exception as e:
                logger.warning("每日话题 4R 打分失败，跳过一条: %s", e)
    return results


# ---------------------------------------------------------------------------
#  Step C - Dedup against TopicSelection (same date + same brand)
# ---------------------------------------------------------------------------

def _load_existing_topic_names_today(
    cfg: Config,
    brand: str,
    start_ms: int,
    end_ms: int,
) -> Set[str]:
    """查 TopicSelection 中当日该品牌已经存在的话题名集合，用于写入去重。"""
    app_token = cfg.app_token
    tbl_id = cfg.tables["topic_selection"]["table_id"]
    f_cfg = cfg.fields["topic_selection"]

    try:
        records = search_all_records(
            app_token=app_token,
            table_id=tbl_id,
            view_id=None,
            automatic_fields=False,
            page_size=200,
        )
    except Exception as e:
        logger.warning("查询 TopicSelection 去重失败（忽略，继续写入）: %s", e)
        return set()

    topic_field = f_cfg.get("topic", "话题名称")
    brand_field = f_cfg.get("brand", "适用品牌")
    created_field = f_cfg.get("created_at", "入库时间")

    existing: Set[str] = set()
    brand_lower = brand.strip().lower()

    for item in records:
        fields = item.get("fields") or {}

        created_ms = int(get_number_field(fields, created_field, default=0.0))
        if not (start_ms <= created_ms < end_ms):
            continue

        brand_val = get_text_field(fields, brand_field, "").strip().lower()
        if brand_val and brand_lower not in brand_val:
            continue

        topic_text = get_text_field(fields, topic_field, "").strip()
        if topic_text:
            existing.add(topic_text)

    return existing


# ---------------------------------------------------------------------------
#  Step D - Write Top K into TopicSelection
# ---------------------------------------------------------------------------

def _write_top_k(
    cfg: Config,
    brand: str,
    scored: List[Tuple[CandidateTopic, FourRScore]],
    top_k: int,
    existing_topic_names: Set[str],
) -> Tuple[List[Dict[str, Any]], int]:
    """按总分降序取 Top K 写入 TopicSelection，跳过已存在条目。

    Returns:
        (written_records, skipped_due_to_dedup)
    """
    # 仅保留打分成功且有决策结果的条目
    valid: List[Tuple[CandidateTopic, FourRScore]] = [
        (c, s) for c, s in scored if s and s.total is not None
    ]
    # 总分降序；相同总分按 R4 降序做 tie-break
    valid.sort(key=lambda cs: (cs[1].total, cs[1].revenue), reverse=True)

    selected = valid[: max(1, int(top_k))]

    app_token = cfg.app_token
    topic_tbl = cfg.tables["topic_selection"]["table_id"]
    f_cfg = cfg.fields["topic_selection"]

    created_records: List[Dict[str, Any]] = []
    skipped = 0
    now_ms = now_ts_ms()

    for candidate, score in selected:
        topic_text = candidate.topic.strip()
        if topic_text in existing_topic_names:
            logger.info("去重跳过: brand=%s topic=%s 当日已存在", brand, topic_text)
            skipped += 1
            continue

        # 优先使用 LLM 给出的决策；若 LLM 没返回，则按 scoring 阈值兜底
        decision_label = score.decision_label or ""

        persona = infer_persona(candidate.topic + "\n" + candidate.raw_text) or ""

        try:
            fetched_ms = int(candidate.timestamp) if candidate.timestamp else now_ms
        except Exception:
            fetched_ms = now_ms

        one_line = score.one_line_reason or f"daily_topics 4R total={score.total:.0f}"

        fields: Dict[str, Any] = {
            f_cfg["topic"]: topic_text,
            f_cfg["source"]: candidate.source,
            f_cfg["fetched_at"]: fetched_ms,
            f_cfg["created_at"]: now_ms,
            f_cfg["raw_text"]: candidate.raw_text,
            f_cfg["rule_hits"]: "",
            f_cfg["r1"]: round(score.relevance),
            f_cfg["r2"]: round(score.resonance),
            f_cfg["r3"]: round(score.reach),
            f_cfg["r4"]: round(score.revenue),
            f_cfg["total_score"]: round(score.total),
            f_cfg["decision"]: decision_label,
            f_cfg["one_line_reason"]: one_line,
        }

        content_dir_field = f_cfg.get("content_direction")
        if content_dir_field and score.content_direction:
            fields[content_dir_field] = score.content_direction

        if persona:
            # audience 是多选字段，需要传 list
            persona_list = [p.strip() for p in persona.split(",") if p.strip()] if isinstance(persona, str) else [persona]
            fields[f_cfg["audience"]] = persona_list

        brand_field = f_cfg.get("brand")
        if brand_field:
            fields[brand_field] = brand

        try:
            resp = add_single(app_token, topic_tbl, fields)
        except Exception as e:
            logger.warning("写入 TopicSelection 失败 topic='%s': %s", topic_text, e)
            continue

        record = None
        if isinstance(resp, dict):
            record = resp.get("record") or (resp.get("data") or {}).get("record")
        elif isinstance(resp, str) and resp:
            # add_single 可能直接返回 record_id 字符串
            record = {"record_id": resp}
        if record:
            created_records.append(record)
            # 本次写入成功的 topic 名加入集合，防止同一批内重名重复写入
            existing_topic_names.add(topic_text)

    return created_records, skipped


# ---------------------------------------------------------------------------
#  Entry point - Step 5
# ---------------------------------------------------------------------------

def run_brand_daily_selection(
    cfg: Config,
    brand: str,
    top_k: Optional[int] = None,
    date: Optional[str] = None,
) -> Dict[str, Any]:
    """第五步入口：每日精选话题 → 品牌 4R 打分 → Top K 写入 TopicSelection。

    Args:
        cfg: 全局配置
        brand: 品牌名（必须与 BrandTopicRules 表中记录一致，否则会使用通用 prompt）
        top_k: Top K 数量；None 时使用 config.daily_topics.top_k（默认 5）
        date: 指定日期 'YYYY-MM-DD'；None 时按北京时间当日

    Returns:
        结构化摘要 JSON
    """
    brand = (brand or "").strip()
    if not brand:
        raise ValueError("brand 不能为空")

    dt_cfg = cfg.raw.get("daily_topics") or {}
    tz_offset = int(dt_cfg.get("timezone_offset_hours", 8))
    effective_top_k = int(top_k if top_k is not None else dt_cfg.get("top_k", 5)) or 5
    lookback_days = int(dt_cfg.get("lookback_days", 10))

    # 1a) 计算候选话题的回溯窗口（过去 N 天）
    fetch_start_ms, fetch_end_ms, date_str = _lookback_window_ms(tz_offset, lookback_days, date)
    # 1b) 去重窗口与候选窗口对齐（v5.9.0: 从仅当日改为同样 lookback_days）
    dedup_start_ms, dedup_end_ms = fetch_start_ms, fetch_end_ms

    logger.info(
        "候选话题窗口: 回溯 %d 天 (%s ~ %s)，去重窗口: 同步",
        lookback_days,
        datetime.fromtimestamp(fetch_start_ms / 1000, tz=timezone(timedelta(hours=tz_offset))).strftime("%Y-%m-%d"),
        date_str,
    )

    # 2) 加载品牌专属 4R prompt（未找到时回落通用 prompt）
    brand_rules_prompt = load_brand_rules_prompt(cfg, brand)
    if brand_rules_prompt:
        logger.info(
            "Loaded brand 4R rules for '%s' (scoring sections only, %d chars)",
            brand,
            len(brand_rules_prompt),
        )
    else:
        logger.warning(
            "品牌 '%s' 未在 BrandTopicRules 中找到专属 prompt，回落通用 4R 规则。"
            "建议先跑 init_brand --brand '%s'。",
            brand,
            brand,
        )

    # 3) 从外部 base 读取过去 N 天候选话题
    dt_app_token = (dt_cfg.get("app_token") or "").strip()
    dt_table_id = (dt_cfg.get("table_id") or "").strip()
    daily_topics_configured = bool(dt_app_token and dt_table_id)

    candidates = _fetch_daily_topics(cfg, fetch_start_ms, fetch_end_ms)
    if not candidates:
        note = (
            "daily_topics 未配置（app_token/table_id 为空），请在 config.yaml 中填写外部数据源"
            if not daily_topics_configured
            else f"过去 {lookback_days} 天每日精选话题表为空，未产生筛选结果"
        )
        return {
            "brand": brand,
            "date": date_str,
            "lookback_days": lookback_days,
            "daily_topics_total": 0,
            "scored_count": 0,
            "top_k": effective_top_k,
            "written_count": 0,
            "skipped_dedup": 0,
            "selected_record_ids": [],
            "note": note,
        }

    # 3b) 预筛：按热度降序取 Top scoring_pool_size 进入 LLM 打分
    #     避免对所有候选话题都调 LLM，大幅缩短耗时
    scoring_pool_size = int(dt_cfg.get("scoring_pool_size", 0)) or max(effective_top_k * 3, 15)
    if len(candidates) > scoring_pool_size:
        # 从 raw_text 里提取热度数值做排序（"今日热度: 12345" 格式）
        def _extract_heat(c: CandidateTopic) -> float:
            for line in (c.raw_text or "").split("\n"):
                if "今日热度" in line:
                    parts = line.split(":")
                    if len(parts) >= 2:
                        try:
                            return float(parts[-1].strip())
                        except ValueError:
                            pass
            return 0.0

        candidates.sort(key=_extract_heat, reverse=True)
        logger.info(
            "预筛: %d 条候选按热度取 Top %d 进入 LLM 打分",
            len(candidates), scoring_pool_size,
        )
        candidates = candidates[:scoring_pool_size]

    # 4) 并发 4R 打分
    scored = _score_candidates_concurrently(cfg, candidates, brand_rules_prompt)

    # 5) 去重：查 TopicSelection 当日该品牌已存在话题名
    existing = _load_existing_topic_names_today(cfg, brand, dedup_start_ms, dedup_end_ms)

    # 6) 写入 Top K
    written, skipped = _write_top_k(cfg, brand, scored, effective_top_k, existing)

    # 构建入选话题摘要（供卡片展示）
    selected_topics: List[Dict[str, Any]] = []
    valid_scored = [(c, s) for c, s in scored if s and s.total is not None]
    valid_scored.sort(key=lambda cs: (cs[1].total, cs[1].revenue), reverse=True)
    for c, s in valid_scored[:effective_top_k]:
        selected_topics.append({
            "topic": c.topic.strip(),
            "score": round(s.total, 1) if s.total else 0,
            "decision": s.decision_label or "",
            "reason": (s.one_line_reason or "")[:60],
        })

    return {
        "brand": brand,
        "date": date_str,
        "lookback_days": lookback_days,
        "daily_topics_total": len(candidates),
        "scored_count": len(scored),
        "top_k": effective_top_k,
        "written_count": len(written),
        "skipped_dedup": skipped,
        "selected_topics": selected_topics,
        "selected_record_ids": [r.get("record_id") for r in written if isinstance(r, dict)],
    }
