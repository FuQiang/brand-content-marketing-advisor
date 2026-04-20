"""Downstream pipeline.

职责：
- 读取 TopicSelection 新增记录 / 指定记录
- 补全人群标签
- 匹配 1-2 款主推产品
- 生成旗舰级营销文案（自动追加「爆款逻辑拆解」）
- 写入 ContentMatrix 表

v5.0.0 起，新增：
- 将核心字段（含「爆款逻辑拆解」以及视觉/视频附件列）按需自动创建到 ContentMatrix
- 提供结构化的内容生成结果，供品牌维度 Top K 视觉资产生成复用
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

from .bitable import (
    add_single,
    ensure_field_exists,
    list_table_fields,
    search_all_records,
    search_records_with_filter,
    update_single,
)
from .config import Config
from .utils import (
    get_multi_select_field,
    get_number_field,
    get_text_field,
    infer_persona,
    now_ts_ms,
)

logger = logging.getLogger("bcma.downstream")


# --------------------------- Data models ---------------------------


@dataclass
class TopicRecord:
    record_id: str
    topic: str
    persona: Optional[str]
    raw_text: str
    fields: Dict[str, Any]


@dataclass
class ProductRecord:
    record_id: str
    name: str
    series: str
    selling_point: str
    selling_detail: str
    fields: Dict[str, Any]
    lifecycle_stage: str = ""   # 新品 / 经典爆品 / 常规款
    launch_date: str = ""
    source_url: str = ""


@dataclass
class GeneratedContentItem:
    """单条下游生成结果，包含 Topic / 产品 / 文案结构与写入后的记录。

    v5.6.0：copywriting dict 同时包含通用字段（hook/body/visual/logic_breakdown）
    与双平台字段（douyin_script/douyin_hook/douyin_visual/xhs_title/xhs_note/
    xhs_body/xhs_visual），供后续封面/视频生成阶段按平台读取。
    """

    topic: TopicRecord
    persona: Optional[str]
    products: List[ProductRecord]
    copywriting: Dict[str, Any]
    record: Dict[str, Any]


# --------------------------- Topic helpers ---------------------------


def load_topics_from_bitable(cfg: Config, record_ids: Optional[Sequence[str]] = None) -> List[TopicRecord]:
    """Load topics from TopicSelection table (optionally filtered by record_ids)."""

    app_token = cfg.app_token
    tbl_id = cfg.tables["topic_selection"]["table_id"]
    f_cfg = cfg.fields["topic_selection"]

    records = search_all_records(app_token, tbl_id, view_id=None, automatic_fields=False, page_size=200)

    record_id_set = set(record_ids) if record_ids else None

    result: List[TopicRecord] = []
    for item in records:
        rid = item.get("record_id")
        if not rid:
            continue
        if record_id_set and rid not in record_id_set:
            continue

        fields = item.get("fields") or {}
        topic = get_text_field(fields, f_cfg["topic"], "")
        if not topic:
            continue
        persona = get_text_field(fields, f_cfg["audience"], "") or None
        raw_text = get_text_field(fields, f_cfg["raw_text"], "")

        result.append(
            TopicRecord(
                record_id=rid,
                topic=topic,
                persona=persona,
                raw_text=raw_text,
                fields=fields,
            )
        )

    return result


def ensure_persona_for_topics(cfg: Config, topics: List[TopicRecord]) -> None:
    """Infer persona for topics missing persona and write back to TopicSelection."""

    app_token = cfg.app_token
    tbl_id = cfg.tables["topic_selection"]["table_id"]
    f_cfg = cfg.fields["topic_selection"]

    for t in topics:
        if t.persona:
            continue
        inferred = infer_persona(t.topic + "\n" + t.raw_text)
        if not inferred:
            continue
        try:
            update_single(
                app_token,
                tbl_id,
                t.record_id,
                {f_cfg["audience"]: [p.strip() for p in inferred.split(",") if p.strip()] if isinstance(inferred, str) else [inferred]},
            )
        except Exception as e:
            logger.warning("补全人群标签失败 record_id=%s: %s", t.record_id, e)
            continue
        t.persona = inferred


# --------------------------- Product helpers ---------------------------


def load_products(cfg: Config, brand: Optional[str] = None) -> List[ProductRecord]:
    """Load products from Products table.

    `brand` 不为空时**必须**按 f["brand"] 字段精确过滤——只返回该品牌的产品。
    这是硬约束：Step 6 生成品牌内容时，一旦跨品牌混入别家 SKU，会出现例如"双汇文案
    配悦鲜活/简醇"这种品牌穿帮事故（2026-04-20 双汇事件根因）。
    调用方负责处理空返回（上游应当停机，不应 fallback 到全表产品池）。
    """

    app_token = cfg.app_token
    tbl_id = cfg.tables["products"]["table_id"]
    f_cfg = cfg.fields["products"]

    records = search_all_records(app_token, tbl_id, view_id=None, automatic_fields=False, page_size=200)

    result: List[ProductRecord] = []
    brand_norm = (brand or "").strip()
    for item in records:
        rid = item.get("record_id")
        if not rid:
            continue
        fields = item.get("fields") or {}
        name = get_text_field(fields, f_cfg["name"], "")
        if not name:
            continue
        if brand_norm:
            item_brand = get_text_field(fields, f_cfg["brand"], "")
            if item_brand.strip() != brand_norm:
                continue
        series = get_text_field(fields, f_cfg["series"], "")
        selling_point = get_text_field(fields, f_cfg["selling_point"], "")
        selling_detail = get_text_field(fields, f_cfg["selling_point_detail"], "")
        lifecycle = get_text_field(fields, f_cfg.get("lifecycle_stage", ""), "")
        launch_date = get_text_field(fields, f_cfg.get("launch_date", ""), "")
        source_url = get_text_field(fields, f_cfg.get("source_url", ""), "")

        result.append(
            ProductRecord(
                record_id=rid,
                name=name,
                series=series,
                selling_point=selling_point,
                selling_detail=selling_detail,
                fields=fields,
                lifecycle_stage=lifecycle,
                launch_date=launch_date,
                source_url=source_url,
            )
        )

    return result


def _score_product_for_topic(
    cfg: Config,
    product: ProductRecord,
    persona: Optional[str],
    topic: TopicRecord,
) -> float:
    """Compute matching score between product and topic/persona.

    v5.9.0: 移除硬编码的品牌特化 persona×product boost，改为纯基于
    Products 表 persona_tags / selling_point / 功能点的动态匹配。
    """

    f_cfg = cfg.fields["products"]
    fields = product.fields

    base_weight = get_number_field(fields, f_cfg["base_weight"], default=50.0)
    score = base_weight

    # 人群标签匹配
    if persona:
        persona_tags = get_multi_select_field(fields, f_cfg["persona_tags"])
        if persona_tags:
            if persona in persona_tags:
                score += 40
            # 部分匹配：人群标签中包含 persona 子串
            elif any(persona in tag or tag in persona for tag in persona_tags):
                score += 20
        else:
            haystack = (product.name + product.selling_point + product.selling_detail).lower()
            if persona.lower() in haystack:
                score += 20

    # 功能点与话题文本的简单关键字匹配
    functions = get_multi_select_field(fields, f_cfg["functions"])
    topic_text = (topic.topic + "\n" + (topic.raw_text or "")).lower()

    for func in functions:
        f = func.lower()
        if "保暖" in f and ("冷" in topic_text or "降温" in topic_text or "寒潮" in topic_text):
            score += 10
        if "轻量" in f and ("通勤" in topic_text or "上班" in topic_text):
            score += 8
        if "城市户外" in f and ("露营" in topic_text or "户外" in topic_text):
            score += 8

    # 季节匹配
    season = get_text_field(fields, f_cfg["season"], "")
    if season:
        if "冬" in season and ("冬" in topic_text or "寒" in topic_text or "冷" in topic_text):
            score += 5

    # 生命周期加成:经典爆品更安全(复购/口碑),新品更有话题感(首发/稀缺)
    if product.lifecycle_stage == "经典爆品":
        score += 8
    elif product.lifecycle_stage == "新品":
        score += 6

    return score


def select_top_products(
    cfg: Config,
    persona: Optional[str],
    topic: TopicRecord,
    products: List[ProductRecord],
) -> List[ProductRecord]:
    """Select top-N products for a given topic/persona.

    v5.10.0: Top-K 选品必须兼顾「新品 + 经典爆品」。Top-K >= 2 时,
    若候选池里两类都有,至少各保留 1 款;不足时由其他高分产品补齐。
    """

    if not products:
        return []

    scored: List[tuple[float, ProductRecord]] = []
    for p in products:
        s = _score_product_for_topic(cfg, p, persona, topic)
        scored.append((s, p))

    scored.sort(key=lambda x: x[0], reverse=True)
    top_k = int(cfg.downstream.get("max_products_per_topic", 2)) or 2

    if top_k <= 1:
        return [p for _, p in scored[:top_k]]

    # Top-K >= 2:尽量保证至少 1 新品 + 1 经典爆品
    top_latest = next((p for _, p in scored if p.lifecycle_stage == "新品"), None)
    top_classic = next((p for _, p in scored if p.lifecycle_stage == "经典爆品"), None)

    picked: List[ProductRecord] = []
    picked_ids: set[str] = set()
    for p in (top_latest, top_classic):
        if p and p.record_id not in picked_ids:
            picked.append(p)
            picked_ids.add(p.record_id)

    for _, p in scored:
        if len(picked) >= top_k:
            break
        if p.record_id in picked_ids:
            continue
        picked.append(p)
        picked_ids.add(p.record_id)

    return picked[:top_k]


# --------------------------- Copywriting ---------------------------


def _truncate_to_range(text: str, min_len: int = 100, max_len: int = 200) -> str:
    """Ensure text length is within [min_len, max_len] by simple trimming/extension."""

    text = (text or "").strip()
    length = len(text)
    if length > max_len:
        return text[:max_len]
    if length < min_len:
        extra = (
            " 整体结构遵循“话题引发共鸣—痛点具体化—解决方案具象化”的链路，"
            "让用户在极短时间内完成从被吸引到愿意点开的决策。"
        )
        combined = (text + extra).strip()
        if len(combined) > max_len:
            return combined[:max_len]
        return combined
    return text


def _build_logic_breakdown(
    topic: TopicRecord,
    persona: Optional[str],
    products: List[ProductRecord],
    hook: str,
) -> str:
    """Construct a concise breakdown explaining why this copy可以成为爆款。"""

    persona_display = persona or "核心消费人群"
    main_product = products[0].name if products else "核心产品"

    persona_pain_points = {
        "新锐白领": "在高压通勤与加班节奏下，对体面又不失舒适的穿搭选择焦虑",
        "精致妈妈": "在照顾孩子与自我形象之间，经常忽略自己保暖和好看的需求",
        "学生党": "在预算有限又想保持体面时，对一件多场景通用单品格外敏感",
        "资深打工人": "长期熬夜加班，对能减轻通勤消耗的功能性装备有潜在需求",
        "户外玩家": "在城市与户外切换时，希望一件单品能兼顾保暖、防风与轻便",
        "品质中产": "希望通过更好的用料和设计，获得与身份相匹配的确定感",
        "潮流青年": "既要兼顾功能，又在意剪裁与辨识度带来的社交话题感",
    }
    pain_point = persona_pain_points.get(
        persona or "",
        "在真实生活场景中的隐性不适和决策压力，而非表面上的“好看保暖”",
    )

    text = (
        f"选择{main_product}作为切入口，是因为它与「{topic.topic}」这一话题在{persona_display}心智中的关联度最高，"
        "既能承接讨论热度，又方便落到清晰的购买理由；"
        f"文案聚焦{persona_display}的隐性痛点：{pain_point}，用具象场景把模糊情绪变成“说的就是我”；"
        f"钩子「{hook}」利用强对比与具体情境，在信息流首屏迅速锁住注意力，引导用户完成从问题识别到解决方案想象的心智跳转，自然抬高点击率和停留时长。"
    )

    return _truncate_to_range(text, min_len=100, max_len=200)


def _build_copywriting(
    cfg: Config,
    topic: TopicRecord,
    persona: Optional[str],
    products: List[ProductRecord],
    brand: Optional[str] = None,
) -> Dict[str, Any]:
    """Generate dual-platform copywriting via LLM (v5.6.0).

    改动说明：v5.6.0 起，文案生成从 v5.5.0 及以前的确定性模板切换为调用
    `bcma.copywriting.generate_dual_platform_copy`，同时产出两套平台差异化文案：

    - 抖音短视频分镜脚本（hook / 5 镜头 / CTA / 视觉基调）
    - 小红书种草笔记（emoji 标题 / 800-1200 字正文 / tags / 构图建议）

    两端 LLM 调用链沿用 scoring 模块的 Claude Opus 4.6 → Sonnet 4.6 → doubao 降级。
    LLM 失败时 copywriting 模块内部会回退到模板化兜底，不会阻塞批次。

    本函数在原双平台结果之上额外产出：
    - 通用字段 hook / body / visual：从抖音输出镜像，保持向后兼容 v5.5.0 的单平台列；
    - logic_breakdown：仍然用确定性启发式生成并追加到 body 末尾；
    - 新的双平台字段 douyin_script / xhs_title / xhs_note，供 `generate_content_items`
      写入 ContentMatrix 的 v5.6.0 新列。
    """

    from .copywriting import generate_dual_platform_copy

    result = generate_dual_platform_copy(
        cfg=cfg,
        brand=(brand or "").strip(),
        topic_title=topic.topic,
        raw_text=topic.raw_text or "",
        persona=persona or "",
        products=products,
    )

    douyin = result.get("douyin", {}) or {}
    xhs = result.get("xhs", {}) or {}

    # 通用字段镜像（v5.5.0 及以前的单平台列仍然要回填）
    hook = str(douyin.get("hook_title") or xhs.get("title") or "").strip()
    douyin_script_text = str(douyin.get("scenes_text") or "").strip()
    visual = str(douyin.get("visual_direction") or xhs.get("visual_direction") or "").strip()

    logic_breakdown = _build_logic_breakdown(topic, persona, products, hook)

    # body 追加爆款逻辑拆解段落，行为与 v5.5.0 保持一致
    body = douyin_script_text
    if logic_breakdown and logic_breakdown not in body:
        body = (body + "\n\n【爆款逻辑拆解】\n" + logic_breakdown).strip()

    return {
        # 通用字段（v5.5.0 及以前列，继续回填向后兼容）
        "hook": hook,
        "body": body,
        "visual": visual,
        "logic_breakdown": logic_breakdown,
        # v5.6.0 双平台字段
        "douyin_script": douyin_script_text,
        "douyin_hook": str(douyin.get("hook_title") or "").strip(),
        "douyin_visual": str(douyin.get("visual_direction") or "").strip(),
        "xhs_title": str(xhs.get("title") or "").strip(),
        "xhs_note": str(xhs.get("note_text") or "").strip(),
        "xhs_body": str(xhs.get("body") or "").strip(),
        "xhs_visual": str(xhs.get("visual_direction") or "").strip(),
    }


# --------------------------- Content creation helpers ---------------------------


def _ensure_content_matrix_fields(cfg: Config) -> None:
    """一次性检查并创建 ContentMatrix 表所需的扩展字段，避免循环中重复调用。

    v5.6.0：新增抖音短视频脚本 / 小红书标题 / 小红书种草笔记 三个文本列，
    以及抖音 9:16 封面 / 小红书 3:4 封面两个附件列。原 v5.2.0 的「视频封面(AI生成)」
    继续保留（回填与抖音 9:16 相同内容）以保证历史看板不断图。
    """

    app_token = cfg.app_token
    cm_tbl = cfg.tables["content_matrix"]["table_id"]
    f_cm = cfg.fields["content_matrix"]

    existing = list_table_fields(app_token, cm_tbl)

    field_specs = [
        (f_cm.get("logic_breakdown"), 1, "Text"),
        # v5.6.0 双平台文案字段
        (f_cm.get("douyin_script"), 1, "Text"),
        (f_cm.get("xhs_title"), 1, "Text"),
        (f_cm.get("xhs_note"), 1, "Text"),
        # v5.6.0 双平台封面字段
        (f_cm.get("douyin_cover"), 17, "Attachment"),
        (f_cm.get("xhs_cover"), 17, "Attachment"),
        # 旧字段保留（向后兼容）
        (f_cm.get("cover_image_ai_field"), 17, "Attachment"),
        (f_cm.get("video_asset_ai"), 17, "Attachment"),
    ]
    for field_name, type_code, ui_type in field_specs:
        if field_name and field_name not in existing:
            try:
                ensure_field_exists(app_token, cm_tbl, field_name, type_code=type_code, ui_type=ui_type, property_obj=None)
            except Exception as e:
                logger.warning("创建字段 '%s' 失败: %s", field_name, e)


def _load_existing_topic_brand_keys_in_content_matrix(cfg: Config) -> set[tuple[str, str]]:
    """读取 ContentMatrix 已有记录的 (话题, 品牌) 二元组集合，用于去重。

    v5.9.0: 去重 key 从单纯话题名改为 (话题, 品牌)，支持不同品牌对同一话题各自生成内容。
    """

    app_token = cfg.app_token
    cm_tbl = cfg.tables["content_matrix"]["table_id"]
    f_cm = cfg.fields["content_matrix"]
    topic_field = f_cm.get("topic", "匹配话题")
    brand_field = f_cm.get("brand", "适用品牌")

    records = search_all_records(app_token, cm_tbl, view_id=None, automatic_fields=False, page_size=200)
    existing: set[tuple[str, str]] = set()
    for item in records:
        fields = item.get("fields") or {}
        t = get_text_field(fields, topic_field, "")
        b = get_text_field(fields, brand_field, "")
        if t:
            existing.add((t, b))
    return existing


def generate_content_items(
    cfg: Config,
    topics: List[TopicRecord],
    products: List[ProductRecord],
    brand: Optional[str] = None,
) -> List[GeneratedContentItem]:
    """基于 Topic 列表与产品池生成内容，并写入 ContentMatrix。

    写入前会检查 ContentMatrix 中是否已存在同名话题，跳过已有记录以保证幂等性。
    返回包含 Topic/产品/文案/record 的结构化结果，方便上游或品牌维度逻辑复用。

    v5.6.0：
    - 新增 `brand` 显式参数，优先级高于从 TopicSelection 记录中读取的「适用品牌」。
      `run_brand_content_pipeline` 会显式传入品牌名，老入口 `run_downstream_pipeline`
      不传时则从 topic.fields 的 brand 列兜底。
    - 同时写入通用字段（hook/body/visuals）与双平台字段（douyin_script/xhs_title/
      xhs_note），保证 v5.5.0 及以前看板继续工作，同时把抖音脚本和小红书笔记落到新列。
    - ContentMatrix 记录显式回填品牌字段，这样话题 × 品牌 × 人群 × 产品的四元组追踪
      链路在表内一目了然，不需要反向解析。
    """

    if not topics or not products:
        return []

    app_token = cfg.app_token
    cm_tbl = cfg.tables["content_matrix"]["table_id"]
    f_cm = cfg.fields["content_matrix"]
    f_ts = cfg.fields.get("topic_selection", {}) or {}
    ts_brand_field = f_ts.get("brand", "适用品牌")

    # 一次性确保扩展字段存在
    _ensure_content_matrix_fields(cfg)

    # 去重：读取已有 (话题, 品牌) 二元组
    existing_keys = _load_existing_topic_brand_keys_in_content_matrix(cfg)

    created_items: List[GeneratedContentItem] = []

    explicit_brand = (brand or "").strip()

    for t in topics:
        dedup_key = (t.topic, explicit_brand or get_text_field(t.fields, ts_brand_field, "").strip())
        if dedup_key in existing_keys:
            logger.info("话题 '%s' + 品牌 '%s' 已存在于 ContentMatrix，跳过", dedup_key[0], dedup_key[1])
            continue

        persona = t.persona
        top_products = select_top_products(cfg, persona, t, products)
        if not top_products:
            continue

        # Brand 优先级：显式参数 > TopicSelection 中的「适用品牌」字段
        topic_brand = (
            explicit_brand
            or get_text_field(t.fields, ts_brand_field, "").strip()
        )

        copy = _build_copywriting(cfg, t, persona, top_products, brand=topic_brand)
        now_ms = now_ts_ms()

        product_str = " / ".join(p.name for p in top_products)
        fields = {
            f_cm["topic"]: t.topic,
            f_cm["audience"]: [p.strip() for p in persona.split(",") if p.strip()] if isinstance(persona, str) and persona else persona or [],
            f_cm["products"]: product_str,
            f_cm["platforms"]: cfg.downstream.get("default_platforms", ["小红书", "抖音"]),
            f_cm["hook"]: copy.get("hook", ""),
            f_cm["body"]: copy.get("body", ""),
            f_cm["visuals"]: copy.get("visual", ""),
            f_cm["generated_at"]: now_ms,
        }

        # 显式回填品牌字段（v5.6.0 新增）
        brand_field_cm = f_cm.get("brand")
        if brand_field_cm and topic_brand:
            fields[brand_field_cm] = topic_brand

        logic_field = f_cm.get("logic_breakdown")
        logic_text = copy.get("logic_breakdown")
        if logic_field and isinstance(logic_text, str) and logic_text.strip():
            fields[logic_field] = logic_text

        # v5.6.0 双平台字段回填
        for copy_key, cm_key in [
            ("douyin_script", "douyin_script"),
            ("xhs_title", "xhs_title"),
            ("xhs_note", "xhs_note"),
        ]:
            field_name = f_cm.get(cm_key)
            value = copy.get(copy_key)
            if field_name and isinstance(value, str) and value.strip():
                fields[field_name] = value

        try:
            resp = add_single(app_token, cm_tbl, fields)
        except Exception as e:
            logger.warning("写入 ContentMatrix 失败, topic='%s': %s", t.topic, e)
            continue
        record = None
        if isinstance(resp, dict):
            record = resp.get("record") or (resp.get("data") or {}).get("record")
        elif isinstance(resp, str) and resp:
            record = {"record_id": resp}
        if not record:
            continue

        existing_keys.add(dedup_key)

        created_items.append(
            GeneratedContentItem(
                topic=t,
                persona=persona,
                products=top_products,
                copywriting=copy,
                record=record,
            )
        )

    return created_items


# --------------------------- Public entrypoints ---------------------------


def run_downstream_pipeline(
    cfg: Config,
    topic_record_ids: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """Run downstream pipeline on given topics (or all topics if ids not provided).

    Returns:
        A list of created ContentMatrix records (raw `data.record` payloads).
    """

    # 1) 读取 TopicSelection
    topics = load_topics_from_bitable(cfg, record_ids=topic_record_ids)
    if not topics:
        return []

    # 2) 补全人群标签
    ensure_persona_for_topics(cfg, topics)

    # 3) 读取产品池
    products = load_products(cfg)
    if not products:
        return []

    # 4) 生成文案并写入 ContentMatrix
    items = generate_content_items(cfg, topics, products)
    return [it.record for it in items]


def run_full_pipeline(cfg: Config, input_path: str) -> Dict[str, Any]:
    """Convenience wrapper: upstream → downstream in one shot."""

    from .upstream import run_upstream_pipeline

    upstream_records = run_upstream_pipeline(cfg, input_path=input_path)
    topic_rids = [
        r.get("record_id") for r in upstream_records if isinstance(r, dict) and r.get("record_id")
    ]

    downstream_records = run_downstream_pipeline(cfg, topic_record_ids=topic_rids or None)

    return {
        "upstream_created": upstream_records,
        "downstream_created": downstream_records,
    }
