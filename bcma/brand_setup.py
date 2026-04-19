"""Brand initialization pipeline: Step 1 ~ 4（可独立运行）.

提供 6 个公共入口函数——4 个独立步骤 + 1 个组合入口 + 1 个辅助加载：
  run_step1_init_brand       — 判断品牌是否存在，不存在则 LLM 生成 7 维度并写入 Brand 表
  run_step2_brand_audience   — 基于 Brand 表 LLM 生成品牌人群画像
  run_step3_products         — 基于 Brand 表 + 品牌人群表 LLM 生成产品线
  run_step4_topic_rules      — 基于内置骨架 + Brand 表 + 品牌人群表生成 4R 策略
  run_init_brand             — 一键执行 Step 1~4（向后兼容）
  load_existing_audience     — 从品牌人群表读取已有数据（供独立步骤复用依赖）
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from .bitable import (
    add_single,
    get_client,
    search_all_records,
    update_single,
)
from .config import Config
from .llm_client import call_llm_text
from .product_assets import ensure_product_gallery_for_brand
from .tx import TxLog, tx_add_single, tx_delete_record, tx_update_single
from .utils import get_text_field

logger = logging.getLogger("bcma.brand_setup")


# ---------------------------------------------------------------------------
#  LLM 调用通用封装
# ---------------------------------------------------------------------------

def _call_llm(prompt: str, cfg: Config, max_tokens: int = 4096) -> Optional[str]:
    """调用 LLM 返回文本结果，失败返回 None。使用 openclaw 配置的模型。"""
    model_cfg = cfg.select_model()
    return call_llm_text(prompt, model_cfg, max_tokens=max_tokens)


def _parse_json_from_llm(text: str) -> Optional[Any]:
    """从 LLM 回复中提取 JSON（兼容 markdown code block 包裹）。"""
    import re
    if not text:
        return None
    # 优先解析 ```json ... ``` 块
    m = re.search(r"```(?:json)?\s*\n?([\s\S]*?)```", text)
    if m:
        text = m.group(1).strip()
    # 再尝试 { ... } 或 [ ... ]
    for start_ch, end_ch in [("{", "}"), ("[", "]")]:
        s = text.find(start_ch)
        e = text.rfind(end_ch)
        if s != -1 and e != -1 and e > s:
            try:
                return json.loads(text[s:e + 1])
            except json.JSONDecodeError:
                continue
    return None


# ===========================================================================
#  Step 1: Init Brand 表（存在即复用，缺失则 LLM 自动生成并写入）
# ===========================================================================

_STEP1_AUTO_PROMPT = """\
你是一名资深品牌策略顾问。请为「{brand}」品牌在中国市场的品牌定位生成 7 维度的基础信息。

请严格返回以下 JSON 格式，不要包裹 markdown code block：
{{
  "category_price": "品类与价格带（例：肉制品/火腿肠/冷鲜肉，10-100元）",
  "differentiation": "核心差异化（该品牌区别于竞品的独特优势，30-80字）",
  "competitors": "最大竞品（3-5个，逗号分隔）",
  "excluded_audience": "排斥人群（该品牌不适合的人群，3-5个，逗号分隔）",
  "compatible_persona": "适配人设/美学（该品牌调性匹配的人设标签，3-5个，逗号分隔）",
  "conflict_persona": "冲突人设/美学（与品牌调性冲突的人设标签，3-5个，逗号分隔）",
  "high_value_scenes": "高价值场景（该品牌产品最常出现的使用场景，3-5个，逗号分隔）"
}}

## 硬性约束
1. 所有 7 个字段必须非空，每个字段至少 10 个字。
2. 基于该品牌在中国市场的公开信息和行业认知生成，保持客观准确。
3. 只返回 JSON，不要加任何解释性文字。
"""


def _step1_init_brand(
    cfg: Config,
    brand: str,
    tx: Optional[TxLog] = None,
) -> Dict[str, Any]:
    """Step 1: 在 Brand 表里按品牌名精确查找；不存在则 LLM 生成 7 维度并写入。

    - 命中且 7 维度齐全 → 直接复用
    - 命中但部分维度为空 → LLM 补齐并回写
    - 未命中 → LLM 生成 7 维度并新增记录

    当 `tx` 非空时，所有写操作会登记到 TxLog，便于跨步骤失败回滚。
    """

    app_token = cfg.app_token
    tbl_cfg = cfg.tables.get("brands") or {}
    tbl_id = tbl_cfg.get("table_id", "")
    if not tbl_id:
        raise ValueError(
            "Brand 表的 table_id 未配置。请先在飞书多维表格创建 Brand 表，"
            "把 7 维度字段填好，然后把 table_id 写入 config.yaml 的 tables.brands"
        )

    f = cfg.fields.get("brands") or {}
    required_keys = [
        "name", "category_price", "differentiation", "competitors",
        "excluded_audience", "compatible_persona", "conflict_persona",
        "high_value_scenes",
    ]
    missing_keys = [k for k in required_keys if not f.get(k)]
    if missing_keys:
        raise ValueError(
            f"Brand 表字段映射缺失: {missing_keys}。请在 config.yaml 的 fields.brands 中补齐"
        )

    records = search_all_records(app_token, tbl_id, page_size=200)

    matched: Optional[Dict[str, Any]] = None
    matched_record_id: str = ""
    for item in records:
        fields = item.get("fields") or {}
        name = get_text_field(fields, f["name"], "")
        if name.strip() == brand.strip():
            matched = fields
            matched_record_id = item.get("record_id", "")
            break

    if matched is None:
        # 品牌不存在 → LLM 自动生成 7 维度并写入
        logger.info("Brand 表中未找到 '%s'，自动用 LLM 生成 7 维度", brand)
        brand_info = _auto_generate_brand_dims(cfg, brand)

        fields_to_write: Dict[str, Any] = {f["name"]: brand}
        for k in required_keys:
            if k != "name":
                fields_to_write[f[k]] = brand_info[k]

        resp = tx_add_single(app_token, tbl_id, fields_to_write, tx=tx)
        if isinstance(resp, dict):
            record = resp.get("record") or (resp.get("data") or {}).get("record")
            matched_record_id = record.get("record_id", "") if isinstance(record, dict) else ""
        elif isinstance(resp, str) and resp:
            matched_record_id = resp
        logger.info("已自动创建品牌 '%s' → record_id=%s", brand, matched_record_id)

        return {
            "step": 1,
            "brand": brand,
            "record_id": matched_record_id,
            "brand_info": brand_info,
            "auto_generated": True,
        }

    brand_info: Dict[str, str] = {
        "name": brand,
        "category_price": get_text_field(matched, f["category_price"], ""),
        "differentiation": get_text_field(matched, f["differentiation"], ""),
        "competitors": get_text_field(matched, f["competitors"], ""),
        "excluded_audience": get_text_field(matched, f["excluded_audience"], ""),
        "compatible_persona": get_text_field(matched, f["compatible_persona"], ""),
        "conflict_persona": get_text_field(matched, f["conflict_persona"], ""),
        "high_value_scenes": get_text_field(matched, f["high_value_scenes"], ""),
    }

    # 校验：7 维度不能有空字段 → 自动补齐
    empty_dims = [k for k in required_keys if k != "name" and not brand_info.get(k, "").strip()]
    if empty_dims:
        logger.info("Brand 表中 '%s' 有空字段 %s，自动用 LLM 补齐", brand, empty_dims)
        generated = _auto_generate_brand_dims(cfg, brand)
        for k in empty_dims:
            brand_info[k] = generated[k]
        # 回写到表
        update_fields: Dict[str, Any] = {}
        for k in empty_dims:
            update_fields[f[k]] = brand_info[k]
        tx_update_single(
            app_token, tbl_id, matched_record_id, update_fields,
            snapshot=matched, tx=tx,
        )

    return {
        "step": 1,
        "brand": brand,
        "record_id": matched_record_id,
        "brand_info": brand_info,
    }


def _auto_generate_brand_dims(cfg: Config, brand: str) -> Dict[str, str]:
    """用 LLM 自动生成品牌 7 维度信息。"""
    prompt = _STEP1_AUTO_PROMPT.format(brand=brand)
    llm_text = _call_llm(prompt, cfg, max_tokens=2048)
    if not llm_text:
        raise RuntimeError(f"LLM 未返回品牌 '{brand}' 的 7 维度信息")

    parsed = _parse_json_from_llm(llm_text)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"LLM 返回的品牌信息无法解析为 JSON: {llm_text[:200]}")

    dim_keys = [
        "category_price", "differentiation", "competitors",
        "excluded_audience", "compatible_persona", "conflict_persona",
        "high_value_scenes",
    ]
    result: Dict[str, str] = {"name": brand}
    empty = []
    for k in dim_keys:
        val = str(parsed.get(k, "")).strip()
        result[k] = val
        if not val:
            empty.append(k)

    if empty:
        raise RuntimeError(f"LLM 生成的品牌信息缺少字段: {empty}")

    return result


# ===========================================================================
#  Step 2: 品牌人群表（LLM 生成 + 严格校验无空字段）
# ===========================================================================

_STEP2_PROMPT = """\
你是一名资深品牌策略顾问。请基于以下 {brand} 品牌的基础信息，为其在中国市场（尤其小红书平台）的每个核心目标人群**分别**生成独立的画像。

## 品牌基础信息（来自 Brand 表，必须严格遵守）
- 品牌名称：{brand}
- 品类与价格带：{category_price}
- 核心差异化：{differentiation}
- 最大竞品：{competitors}
- 排斥人群（严禁出现在人群画像中）：{excluded_audience}
- 适配人设/美学：{compatible_persona}
- 冲突人设/美学：{conflict_persona}
- 高价值场景：{high_value_scenes}

## 任务
为该品牌选择 1-3 个核心目标人群（从 audience 枚举中选），**每个人群单独输出一个 JSON 对象**，放入数组。
每个人群对象包含以下字段，字段内容必须针对该人群**个性化描述**，不同人群的动机/偏好/描述不应雷同：

[
  {{
    "audience": "人群标签（单个）",
    "persona_tags": "该人群的画像关键词，用顿号分隔（≥ 40 字）",
    "motivation": "该人群购买 {brand} 的核心消费动机，必须引用品牌「核心差异化」（≥ 80 字）",
    "content_preference": "该人群在小红书上偏好的内容类型/风格/博主，必须与「适配人设/美学」吻合（≥ 80 字）",
    "persona_description": "该人群的画像描述，涵盖生活方式、价值观、消费习惯、场景偏好（≥ 100 字）"
  }}
]

## 硬性约束
1. 返回 JSON 数组，包含 1-3 个人群对象。每个对象 5 个字段**全部必填**，任何字段为空或低于最小字数都视为失败。
2. audience 只能从下列选项中选择：Z世代、新锐白领、资深中产、精致妈妈、小镇青年。每个对象只填一个。
3. 每个人群的 motivation/content_preference/persona_description 必须体现该人群的独有特征，不能复制粘贴。
4. 画像必须与「适配人设/美学」「高价值场景」高度一致；**严禁**出现「排斥人群」或「冲突人设/美学」中描述的任何特征。
5. motivation 字段中必须出现"核心差异化"的关键词或其自然改写。
6. persona_description 中必须至少提到 1 个来自 Brand 表「高价值场景」的场景。
7. 只返回 JSON 数组，不要包裹 markdown code block，不要加任何解释性文字。
"""


def _validate_brand_audience(parsed: Any) -> Tuple[bool, List[str]]:
    """校验 LLM 返回的品牌人群 JSON 是否为合法的人群画像数组。

    接受两种格式：
    - 新格式（数组）：[{audience: "精致妈妈", ...}, {audience: "资深中产", ...}]
    - 旧格式（单对象）：{audience: ["精致妈妈", "资深中产"], ...}  → 自动转换

    Returns:
        (is_valid, issues_list) —— 若合法返回 (True, [])；否则 (False, [具体问题描述])
    """
    issues: List[str] = []

    # 兼容旧格式：单 dict → 拆成多行
    if isinstance(parsed, dict):
        parsed = _split_single_audience_dict(parsed)

    if not isinstance(parsed, list) or not parsed:
        return False, ["返回值不是非空 JSON 数组"]

    valid_options = {"Z世代", "新锐白领", "资深中产", "精致妈妈", "小镇青年"}
    min_len_map = {
        "persona_tags": 40,
        "motivation": 80,
        "content_preference": 80,
        "persona_description": 100,
    }

    for i, item in enumerate(parsed):
        prefix = f"[{i}] "
        if not isinstance(item, dict):
            issues.append(f"{prefix}元素不是 JSON 对象")
            continue

        # audience: 单个字符串
        aud = item.get("audience", "")
        if isinstance(aud, list):
            aud = aud[0] if aud else ""
        aud = str(aud).strip()
        if aud not in valid_options:
            issues.append(f"{prefix}audience '{aud}' 非法，只能从 {sorted(valid_options)} 中选择")

        for key, min_len in min_len_map.items():
            val = item.get(key)
            if not isinstance(val, str):
                issues.append(f"{prefix}{key} 必须是字符串，当前类型 {type(val).__name__}")
                continue
            stripped = val.strip()
            if not stripped:
                issues.append(f"{prefix}{key} 为空字符串")
            elif len(stripped) < min_len:
                issues.append(f"{prefix}{key} 长度 {len(stripped)} 字，低于最小要求 {min_len} 字")

    return (len(issues) == 0), issues


def _split_single_audience_dict(d: Dict[str, Any]) -> List[Dict[str, Any]]:
    """将旧格式单 dict（audience 为数组）拆成每人群一行。

    如果 audience 是单个字符串或只有一个元素，直接返回 [d]。
    """
    audience = d.get("audience", [])
    if isinstance(audience, str):
        audience = [audience]
    if not isinstance(audience, list) or len(audience) <= 1:
        return [d]

    # 拆分：每个 audience 复用相同的文本字段（降级兼容，不如 LLM 直接生成好）
    result = []
    for aud in audience:
        row = dict(d)
        row["audience"] = str(aud).strip()
        result.append(row)
    return result


def _step2_init_brand_audience(
    cfg: Config,
    brand: str,
    brand_info: Dict[str, str],
    tx: Optional[TxLog] = None,
) -> Dict[str, Any]:
    """Step 2: 生成/复用品牌人群画像，写入品牌人群表（每个人群一行）。

    - 已存在多行且所有字段非空 → 直接复用
    - 缺失或部分字段为空 → LLM 生成（两次重试）→ 写入
    - 两次重试均失败 → 抛错中断整个 init_brand
    """

    app_token = cfg.app_token
    tbl_id = cfg.tables["brand_audience"]["table_id"]
    f = cfg.fields["brand_audience"]

    # 1) 查现有记录（可能有多行）
    existing_records = search_all_records(app_token, tbl_id, page_size=200)
    brand_rows: List[Dict[str, Any]] = []  # [{record_id, fields}, ...]
    for item in existing_records:
        fields = item.get("fields") or {}
        name = get_text_field(fields, f["name"], "")
        if name.strip() == brand.strip():
            brand_rows.append({"record_id": item.get("record_id", ""), "fields": fields})

    # 2) 如果已有记录且所有关键字段非空 → 直接复用
    if brand_rows:
        all_complete = True
        personas = []
        for row in brand_rows:
            fld = row["fields"]
            info = {
                "audience": fld.get(f["audience"]) or [],
                "persona_tags": get_text_field(fld, f["persona_tags"], ""),
                "motivation": get_text_field(fld, f["motivation"], ""),
                "content_preference": get_text_field(fld, f["content_preference"], ""),
                "persona_description": get_text_field(fld, f["persona_description"], ""),
            }
            if not (info["audience"] and info["motivation"].strip() and info["persona_description"].strip()):
                all_complete = False
                break
            personas.append(info)

        if all_complete and personas:
            logger.info("品牌人群表中 '%s' 已有 %d 行完整记录，直接复用", brand, len(personas))
            merged = _merge_personas_for_compat(personas)
            return {
                "step": 2,
                "brand": brand,
                "record_ids": [r["record_id"] for r in brand_rows],
                "reused": True,
                "personas": personas,
                **merged,
            }
        logger.info("品牌人群表中 '%s' 存在但字段不全，将 LLM 重新生成", brand)

    # 3) LLM 生成（最多两次尝试）
    base_prompt = _STEP2_PROMPT.format(brand=brand, **brand_info)
    parsed: Optional[List[Dict[str, Any]]] = None
    issues: List[str] = []

    for attempt in range(2):
        if attempt == 0:
            prompt = base_prompt
        else:
            prompt = (
                f"你上次返回的 JSON 存在以下问题：\n"
                + "\n".join(f"  - {iss}" for iss in issues)
                + "\n\n请严格按要求重新返回完整 JSON 数组。原始任务：\n\n"
                + base_prompt
            )

        llm_text = _call_llm(prompt, cfg, max_tokens=4096)
        if not llm_text:
            issues = ["LLM 未返回任何文本"]
            logger.warning("Step 2 第 %d 次尝试：LLM 调用无输出", attempt + 1)
            continue

        candidate = _parse_json_from_llm(llm_text)
        # 兼容旧格式 dict → list
        if isinstance(candidate, dict):
            candidate = _split_single_audience_dict(candidate)
        ok, issues = _validate_brand_audience(candidate)
        if ok:
            parsed = candidate
            break
        logger.warning("Step 2 第 %d 次尝试校验失败：%s", attempt + 1, issues)

    if parsed is None:
        raise RuntimeError(
            f"Step 2 两次尝试均未生成合法的品牌人群画像 JSON。问题：{issues}。"
            f"请检查 LLM 是否正常、Brand 表内容是否足够具体后重试 init_brand。"
        )

    # 4) 删除旧记录（如果有），然后逐行写入新记录
    if brand_rows:
        for row in brand_rows:
            try:
                tx_delete_record(
                    app_token, tbl_id, row["record_id"],
                    prev_fields=row.get("fields") or {},
                    tx=tx,
                )
                logger.info("已删除旧人群记录 %s", row["record_id"])
            except Exception as e:
                logger.warning("删除旧记录 %s 失败: %s", row["record_id"], e)

    record_ids: List[str] = []
    personas: List[Dict[str, Any]] = []
    for item in parsed:
        aud = item.get("audience", "")
        if isinstance(aud, list):
            aud = aud[0] if aud else ""
        aud_str = str(aud).strip()

        fields_to_write: Dict[str, Any] = {
            f["name"]: brand,
            f["audience"]: [aud_str],  # MultiSelect 需要 list
            f["persona_tags"]: str(item.get("persona_tags", "")).strip(),
            f["motivation"]: str(item.get("motivation", "")).strip(),
            f["content_preference"]: str(item.get("content_preference", "")).strip(),
            f["persona_description"]: str(item.get("persona_description", "")).strip(),
        }

        resp = tx_add_single(app_token, tbl_id, fields_to_write, tx=tx)
        rid = ""
        if isinstance(resp, dict):
            record = resp.get("record") or (resp.get("data") or {}).get("record")
            rid = record.get("record_id", "") if isinstance(record, dict) else ""
        elif isinstance(resp, str) and resp:
            rid = resp
        record_ids.append(rid)
        logger.info("写入人群行: %s → record_id=%s", aud_str, rid)

        personas.append({
            "audience": [aud_str],
            "persona_tags": fields_to_write[f["persona_tags"]],
            "motivation": fields_to_write[f["motivation"]],
            "content_preference": fields_to_write[f["content_preference"]],
            "persona_description": fields_to_write[f["persona_description"]],
        })

    merged = _merge_personas_for_compat(personas)
    return {
        "step": 2,
        "brand": brand,
        "record_ids": record_ids,
        "reused": False,
        "personas": personas,
        **merged,
    }


def _merge_personas_for_compat(personas: List[Dict[str, Any]]) -> Dict[str, Any]:
    """将多行 persona 合并为旧格式的单 audience_info dict，供 Step 3/4 等消费。"""
    all_audiences: List[str] = []
    all_tags: List[str] = []
    all_motivations: List[str] = []
    all_prefs: List[str] = []
    all_descs: List[str] = []

    for p in personas:
        aud = p.get("audience", [])
        if isinstance(aud, list):
            all_audiences.extend(aud)
        elif isinstance(aud, str):
            all_audiences.append(aud)
        if p.get("persona_tags"):
            all_tags.append(str(p["persona_tags"]))
        if p.get("motivation"):
            all_motivations.append(str(p["motivation"]))
        if p.get("content_preference"):
            all_prefs.append(str(p["content_preference"]))
        if p.get("persona_description"):
            all_descs.append(str(p["persona_description"]))

    return {
        "audience": all_audiences,
        "persona_tags": "\n".join(all_tags),
        "motivation": "\n".join(all_motivations),
        "content_preference": "\n".join(all_prefs),
        "persona_description": "\n".join(all_descs),
    }


# ===========================================================================
#  Step 3: Products 表（LLM 生成 + 图库补全）
# ===========================================================================

_STEP3_PROMPT = """\
你是一名资深品牌产品专家。请列出 {brand} 在中国市场的主要产品线（5-10 款核心产品）。

## 品牌基础信息（来自 Brand 表，所有产品必须与这三条高度吻合）
- 品类与价格带：{category_price}
- 核心差异化：{differentiation}
- 高价值场景：{high_value_scenes}

## 目标人群画像（来自品牌人群表）
- 典型人群：{audience}
- 画像标签：{persona_tags}

## 硬性要求
1. 所有产品的 price_band 必须落在「品类与价格带」声明的区间内
2. 所有产品的 selling_point / selling_point_detail 必须紧扣「核心差异化」
3. 所有产品的 functions / season 必须服务于「高价值场景」中至少一个场景
4. 产品 persona_tags 必须与目标人群画像对齐

## 返回格式（严格 JSON 数组，不要加包裹或注释）
[
  {{
    "series": "产品系列名称",
    "name": "具体产品名称（中文+英文）",
    "selling_point": "一句话核心卖点（20 字以内）",
    "selling_point_detail": "详细卖点阐述（50-100 字，引用核心差异化）",
    "persona_tags": ["目标人群标签1", "目标人群标签2"],
    "pain_points": "该产品解决的核心人群痛点（30-60 字）",
    "season": "适用季节",
    "price_band": "价格带",
    "material": "核心材质",
    "functions": ["功能点1", "功能点2"]
  }}
]

## 枚举约束
- persona_tags 只能从以下选项中多选：新锐白领、精致妈妈、学生党、资深打工人、户外玩家、品质中产、潮流青年
- season 只能是：春、夏、秋、冬、四季通用
- price_band 只能是：入门、中端、高端、旗舰、奢华
- functions 只能从以下选项中多选：极致保暖、轻量通勤、防风防水、城市户外、防雨防污、可机洗、高强度抗皱

不要返回 JSON 之外的任何内容。
"""


def _step3_populate_products(
    cfg: Config,
    brand: str,
    brand_info: Dict[str, str],
    audience_info: Dict[str, Any],
    tx: Optional[TxLog] = None,
) -> Dict[str, Any]:
    """Step 3: 通过 LLM 生成产品信息并写入 Products 表，然后补全图库。"""

    app_token = cfg.app_token
    tbl_id = cfg.tables["products"]["table_id"]
    f = cfg.fields["products"]

    # 检查该品牌是否已有产品记录
    existing_records = search_all_records(app_token, tbl_id, page_size=200)
    existing_names: set = set()
    for item in existing_records:
        fields = item.get("fields") or {}
        b = get_text_field(fields, f["brand"], "")
        if brand in b:
            n = get_text_field(fields, f["name"], "")
            if n:
                existing_names.add(n)

    audience = audience_info.get("audience", [])
    audience_str = ", ".join(audience) if isinstance(audience, list) else str(audience)

    prompt = _STEP3_PROMPT.format(
        brand=brand,
        category_price=brand_info.get("category_price", ""),
        differentiation=brand_info.get("differentiation", ""),
        high_value_scenes=brand_info.get("high_value_scenes", ""),
        audience=audience_str,
        persona_tags=audience_info.get("persona_tags", ""),
    )
    llm_text = _call_llm(prompt, cfg)
    products = _parse_json_from_llm(llm_text) if llm_text else None

    if not products or not isinstance(products, list):
        logger.warning("Step 3: LLM 未返回有效产品列表")
        return {
            "step": 3,
            "brand": brand,
            "products_created": 0,
            "products_skipped": 0,
            "gallery_stats": {},
        }

    created_count = 0
    skipped_count = 0
    created_products: List[Dict[str, str]] = []  # [{name, series, selling_point}, ...]

    for p in products:
        if not isinstance(p, dict):
            continue
        name = str(p.get("name", "")).strip()
        if not name:
            continue
        if name in existing_names:
            logger.info("产品 '%s' 已存在，跳过", name)
            skipped_count += 1
            continue

        persona_tag_list = p.get("persona_tags", [])
        if isinstance(persona_tag_list, str):
            persona_tag_list = [x.strip() for x in persona_tag_list.split(",") if x.strip()]

        function_list = p.get("functions", [])
        if isinstance(function_list, str):
            function_list = [x.strip() for x in function_list.split(",") if x.strip()]

        fields_to_write = {
            f["brand"]: brand,
            f["series"]: str(p.get("series", "")),
            f["name"]: name,
            f["selling_point"]: str(p.get("selling_point", "")),
            f["selling_point_detail"]: str(p.get("selling_point_detail", "")),
            f["persona_tags"]: persona_tag_list,
            f["pain_points"]: str(p.get("pain_points", "")),
            f["season"]: str(p.get("season", "")),
            f["price_band"]: str(p.get("price_band", "")),
            f["material"]: str(p.get("material", "")),
            f["functions"]: function_list,
        }

        try:
            tx_add_single(app_token, tbl_id, fields_to_write, tx=tx)
            created_count += 1
            existing_names.add(name)
            created_products.append({
                "name": name,
                "series": str(p.get("series", "")),
                "selling_point": str(p.get("selling_point", "")),
            })
        except Exception as e:
            logger.warning("创建产品记录失败 name='%s': %s", name, e)

    # 补全产品图库
    try:
        gallery_stats = ensure_product_gallery_for_brand(
            brand=brand,
            app_token=app_token,
            tables_cfg=cfg.tables,
            fields_cfg=cfg.fields,
        )
    except Exception as e:
        logger.warning("产品图库补全失败: %s", e)
        gallery_stats = {}

    return {
        "step": 3,
        "brand": brand,
        "products_created": created_count,
        "products_skipped": skipped_count,
        "created_products": created_products,
        "gallery_stats": gallery_stats,
    }


# ===========================================================================
#  Step 4: BrandTopicRules 表（内置骨架 + LLM 槽位填充）
# ===========================================================================

# 内置骨架模板：结构、标题、emoji、表格、决策规则一律逐字保留。
# << >> 槽位将由 LLM 基于 Brand 表 + 品牌人群表 内容填充。
_TOPIC_RULES_SKELETON = """\
# Role
你是 <<品牌名>>（<<品牌昵称或核心差异化关键词>>）中国区小红书内容策略官，服务一个<<客单价区间+品类定位,一句话>>的品牌。你的任务是从我提供的候选话题池中，筛选出真正值得品牌投入预算的话题，并给出可执行的内容方向。

# 品牌底层认知（不可违背的前提）
1. **品牌定位**：<<基于品类与价格带+核心差异化,一句话精准定位>>。
2. **最大竞品**：<<逐个列出 1-3 个竞品及其心智关键词>>。
3. **<<品牌名>>对<<最大竞品之一>>最锋利的一刀**：不是"<<错误的差异化叙述>>"，而是"<<基于核心差异化的真实买点>>"。
4. **目标人群**：<<基于人群描述+画像标签压缩成一句话>>。
5. **不要的人群**：<<基于 Brand 表排斥人群,精炼一句话>>。

# 4R 筛选法则（每个话题必须逐项打分，1-5 分）

## R1. Relevance 相关度
该话题是否契合「<<品牌核心差异化提炼的 3-5 个关键词>>」的品牌基因？
- 5 分：<<直接命中核心差异化的话题关键词示例 3-5 个,用顿号分隔>>
- 1 分：<<与排斥人群/定位相斥的话题关键词示例 3-5 个,用顿号分隔>>

## R2. Resonance 场景力
该话题能否自然植入一个**真实的高价值生活场景**？
- 优先场景：**<<直接列出 Brand 表高价值场景,用 / 分隔>>**
- 警惕场景：<<反推排斥人群+冲突人设衍生的低价值场景 3-5 个,用 / 分隔>>

## R3. Reach 流量与趋势
- 是处于上升期（近 14 天热度↑）还是衰退期？
- 是否有节点爆发力（<<基于高价值场景衍生的 3-4 个年度时节/节点,用顿号分隔>>）？
- 关键词搜索量级与商业笔记饱和度如何？

## R4. Risk 舆情风险（关键过滤器）
- 是否涉及<<基于排斥人群+冲突人设列出该品牌的舆情红线关键词 3-5 个>>？
- 平台是否近期对该话题限流？
- 竞品是否在该话题下有翻车前科？
- **任何 R4 ≤ 2 分的话题，无论其他维度多高，一律 PASS。**

# 人设审美一致性校验（避免左右互搏）
<<品牌名>>的<<核心差异化简称,2-4 字>>，**不适配**以下人设：
<<逐项列出冲突人设美学,每项独立一行,以 ❌ 开头,后接一句简短原因说明为什么伤害品牌>>

**真正适配的人设关键词**：
<<直接列出适配人设美学中每一项,用 / 分隔,整行前面加 ✅>>

筛选时如果话题强绑定不适配人设，需在输出中明确标注「人设冲突」。

# 反漏斗机制：从评论区反推
除了筛选我给的话题池，请额外建议 3 个「**用户原话型话题**」——即从竞品爆文评论区可能出现的高频痛点反推出的话题，例如：
<<基于核心差异化+消费动机+人群描述的痛点,给出 3-5 个该品牌专属的用户原话示例,每行以 - 开头,用引号包裹具体话题>>

这类话题转化率远高于话题榜上的热词。

# 输出格式（严格遵守）

## 第一部分：话题筛选结果表

| 话题 | R1 相关 | R2 场景 | R3 流量 | R4 风险 | 总分 | 决策 | 一句话理由 |
|---|---|---|---|---|---|---|---|
| #xxx | 4 | 5 | 3 | 5 | 17 | ✅ 主推 | …… |
| #xxx | 5 | 2 | 4 | 5 | 16 | 🟡 备选 | …… |
| #xxx | 3 | 4 | 5 | 1 | 13 | ❌ PASS | 舆情高危 |

**决策规则**：
- 总分 ≥ 16 且 R4 ≥ 4 → ✅ 主推
- 总分 13-15 且 R4 ≥ 3 → 🟡 备选
- R4 ≤ 2 或总分 < 13 → ❌ PASS

## 第二部分：主推话题的内容方向（每个话题给 3 条）
针对每个 ✅ 主推话题，输出：
1. **内容钩子**（标题方向，5 条以内）
2. **视觉锚点**（一定要让"<<核心差异化简称,2-4 字>>"成为画面焦点的具体拍法）
3. **达人类型建议**（头部 / 腰部 / KOC，及人群标签）
4. **预算配比建议**（参考默认 2:8 头部 vs KOC，可按话题调整）

## 第三部分：评论区反推的 3 个增量话题
来源 + 推断逻辑 + 内容方向。

## 第四部分：本周风险预警
列出 1-3 个**看起来诱人但建议放弃**的话题，说明踩雷理由。
"""


_STEP4_GEN_PROMPT = """\
你是一名资深品牌内容策略专家。请基于以下品牌基础信息和目标人群画像，生成一份《{brand} 小红书话题筛选策略》prompt，将作为该品牌在 BrandTopicRules 表中的唯一一行记录，未来每周直接拿来给 LLM 跑批量话题筛选。

## 品牌基础信息（来自 Brand 表）
- 品牌名称：{brand}
- 品类与价格带：{category_price}
- 核心差异化：{differentiation}
- 最大竞品：{competitors}
- 排斥人群：{excluded_audience}
- 适配人设/美学：{compatible_persona}
- 冲突人设/美学：{conflict_persona}
- 高价值场景：{high_value_scenes}

## 目标人群画像（来自品牌人群表）
- 典型人群受众：{audience}
- 画像标签：{persona_tags}
- 消费动机：{motivation}
- 内容偏好：{content_preference}
- 人群描述：{persona_description}

## 任务
严格按照下面的骨架生成完整 prompt。骨架中带 `<<>>` 的部分是槽位，必须用真实内容填充；骨架的 markdown 结构、小标题、emoji、表格格式、决策规则阈值必须**逐字保留**。

---
{skeleton}
---

## 填充指引（必须遵守）
- Role 段的"客单价区间+品类定位" —— 直接用 Brand 表的「品类与价格带」改写成一句话
- 品牌底层认知第 1 条 —— 用「品类与价格带」+「核心差异化」合写一句话品牌定位
- 品牌底层认知第 2 条 —— 逐字使用 Brand 表「最大竞品」（最好带括号注释每个竞品的心智关键词）
- 品牌底层认知第 3 条"最锋利的一刀" —— 基于「核心差异化」反驳最大竞品之一的常见误解叙述
- 品牌底层认知第 4 条 —— 基于「人群描述」+「画像标签」压缩成一句话（保留地域、年龄、职业、身材/生活方式关键词）
- 品牌底层认知第 5 条 —— 直接改写 Brand 表「排斥人群」
- R1 的 5 分关键词 —— 从「核心差异化」提炼 3-5 个高频话题词
- R1 的 1 分关键词 —— 反推「排斥人群」+「冲突人设/美学」的负面话题词
- R2 优先场景 —— 直接使用 Brand 表「高价值场景」，保留原始场景名
- R2 警惕场景 —— 反推「排斥人群」+「冲突人设/美学」衍生 3-5 个具体场景
- R3 节点爆发力 —— 基于「高价值场景」选 3-4 个该品类相关的年度节点（降温/换季/跨年/节日/滑雪季/春节返乡等，按品类定制）
- R4 舆情关键词 —— 基于「排斥人群」+「冲突人设/美学」列出该品牌特有的舆情红线关键词 3-5 个
- 人设审美一致性校验中的"不适配" ❌ 列表 —— 直接使用「冲突人设/美学」逐项列出，每项后面补一句简短原因
- 人设审美一致性校验中的"适配" ✅ 列表 —— 直接使用 Brand 表「适配人设/美学」
- 反漏斗机制的示例话题 —— 基于「核心差异化」+「消费动机」+「人群描述」的痛点反推 3-5 个用户原话型话题（用引号包裹）
- 第二部分视觉锚点的"核心差异化简称" —— 从「核心差异化」提炼 2-4 字短语（示例：修身剪裁、防晒黑科技、低糖主义……）

## 输出要求
1. **直接返回改写后的完整 prompt 纯文本**，不要用 ``` 或任何 markdown code block 包裹，不要加解释性前言或后记
2. 不允许残留任何 `<<` 或 `>>` 字符
3. 所有品牌叙述必须基于上面提供的品牌信息和人群画像，严禁编造
4. 保持骨架的 markdown 结构、小标题层级、表格格式、emoji 完全不变
5. 决策规则阈值（≥16 主推 / 13-15 备选 / R4 ≤2 PASS）必须逐字保留
6. 最终输出预期长度 1500-4500 字
"""


def _validate_rules_prompt(text: str) -> Tuple[bool, List[str]]:
    """校验 Step 4 生成的 rules_prompt 是否合法。"""
    issues: List[str] = []
    if not text or not text.strip():
        return False, ["输出为空"]

    if "<<" in text or ">>" in text:
        issues.append("仍有未填充的 << >> 槽位")

    required_headers = [
        "# Role",
        "# 品牌底层认知",
        "# 4R 筛选法则",
        "## R1.",
        "## R2.",
        "## R3.",
        "## R4.",
        "# 人设审美一致性校验",
        "# 反漏斗机制",
        "# 输出格式",
    ]
    for h in required_headers:
        if h not in text:
            issues.append(f"缺少必要章节标题 '{h}'")

    # 决策规则阈值必须保留
    if "≥ 16" not in text and ">= 16" not in text:
        issues.append("决策规则中的 ≥ 16 主推阈值缺失")
    if "R4 ≤ 2" not in text and "R4 <= 2" not in text:
        issues.append("R4 ≤ 2 的一票否决阈值缺失")

    if len(text.strip()) < 1500:
        issues.append(f"输出长度 {len(text.strip())} 字过短，低于 1500 字")

    return (len(issues) == 0), issues


def _step4_generate_topic_rules(
    cfg: Config,
    brand: str,
    brand_info: Dict[str, str],
    audience_info: Dict[str, Any],
    tx: Optional[TxLog] = None,
) -> Dict[str, Any]:
    """Step 4: 基于内置骨架 + Brand 表 + 品牌人群表，LLM 动态生成该品牌专属话题筛选策略。

    生成失败（两次重试仍不合法）会抛错中断整个 init_brand，不再 fallback 到默认模板。
    """

    app_token = cfg.app_token
    tbl_id = cfg.tables["brand_topic_rules"]["table_id"]
    f = cfg.fields["brand_topic_rules"]

    audience_list = audience_info.get("audience", [])
    audience_str = ", ".join(audience_list) if isinstance(audience_list, list) else str(audience_list)

    base_prompt = _STEP4_GEN_PROMPT.format(
        brand=brand,
        category_price=brand_info.get("category_price", ""),
        differentiation=brand_info.get("differentiation", ""),
        competitors=brand_info.get("competitors", ""),
        excluded_audience=brand_info.get("excluded_audience", ""),
        compatible_persona=brand_info.get("compatible_persona", ""),
        conflict_persona=brand_info.get("conflict_persona", ""),
        high_value_scenes=brand_info.get("high_value_scenes", ""),
        audience=audience_str,
        persona_tags=audience_info.get("persona_tags", ""),
        motivation=audience_info.get("motivation", ""),
        content_preference=audience_info.get("content_preference", ""),
        persona_description=audience_info.get("persona_description", ""),
        skeleton=_TOPIC_RULES_SKELETON,
    )

    rewritten: Optional[str] = None
    issues: List[str] = []

    for attempt in range(2):
        if attempt == 0:
            prompt = base_prompt
        else:
            prompt = (
                f"你上次生成的 prompt 存在以下问题：\n"
                + "\n".join(f"  - {iss}" for iss in issues)
                + "\n\n请严格按要求重新生成。原始任务：\n\n"
                + base_prompt
            )

        llm_text = _call_llm(prompt, cfg, max_tokens=8192)
        if not llm_text:
            issues = ["LLM 未返回任何文本"]
            logger.warning("Step 4 第 %d 次尝试：LLM 调用无输出", attempt + 1)
            continue

        # 去掉可能的 markdown code block 包裹
        stripped = llm_text.strip()
        if stripped.startswith("```"):
            import re
            m = re.match(r"```(?:markdown|text)?\s*\n?([\s\S]*?)```", stripped)
            if m:
                stripped = m.group(1).strip()

        ok, issues = _validate_rules_prompt(stripped)
        if ok:
            rewritten = stripped
            break
        logger.warning("Step 4 第 %d 次尝试校验失败：%s", attempt + 1, issues)

    if rewritten is None:
        raise RuntimeError(
            f"Step 4 两次尝试均未生成合法的话题筛选策略 prompt。问题：{issues}。"
            f"请检查 LLM 是否正常、Brand 表内容是否足够具体后重试 init_brand。"
        )

    # upsert 到 BrandTopicRules 表
    existing_records = search_all_records(app_token, tbl_id, page_size=50)
    existing_record_id = ""
    existing_fields: Dict[str, Any] = {}
    for item in existing_records:
        fields = item.get("fields") or {}
        name = get_text_field(fields, f["name"], "")
        if name.strip() == brand.strip():
            existing_record_id = item.get("record_id", "")
            existing_fields = fields
            break

    fields_to_write = {
        f["name"]: brand,
        f["rules_prompt"]: rewritten,
    }

    if existing_record_id:
        tx_update_single(
            app_token, tbl_id, existing_record_id, fields_to_write,
            snapshot=existing_fields, tx=tx,
        )
        result_record_id = existing_record_id
    else:
        resp = tx_add_single(app_token, tbl_id, fields_to_write, tx=tx)
        record = None
        if isinstance(resp, dict):
            record = resp.get("record") or (resp.get("data") or {}).get("record")
        if isinstance(record, dict):
            result_record_id = record.get("record_id", "")
        elif isinstance(resp, str):
            result_record_id = resp
        else:
            result_record_id = ""

    return {
        "step": 4,
        "brand": brand,
        "record_id": result_record_id,
        "rules_prompt_length": len(rewritten),
        "rules_prompt": rewritten,
        "upserted": "update" if existing_record_id else "insert",
    }


# ===========================================================================
#  统一入口
# ===========================================================================

def run_init_brand_transactional(cfg: Config, brand: str) -> Dict[str, Any]:
    """事务化执行 Step 1-4：任一步抛错则回滚本次所有写操作。

    实现方式：所有写操作经 TxLog 登记 (add/update/delete)，异常时按 LIFO 逆序撤销。
    只适用于 run_all 场景；单步 CLI 仍走非事务路径。

    返回值：
      - 成功：{"brand", "step1_brand", "step2_audience", "step3_products",
               "step4_topic_rules", "tx_ops": N, "tx_status": "committed"}
      - 失败：抛出原异常，但在日志中写入 rollback summary；调用方可捕获后决定后续流程。
    """
    brand = brand.strip()
    if not brand:
        raise ValueError("brand 不能为空")

    tx = TxLog(cfg.app_token)
    try:
        logger.info("===== [TX] Step 1: 初始化 Brand 表 '%s' =====", brand)
        step1 = _step1_init_brand(cfg, brand, tx=tx)
        brand_info = step1["brand_info"]

        logger.info("===== [TX] Step 2: 初始化品牌人群表 =====")
        step2 = _step2_init_brand_audience(cfg, brand, brand_info, tx=tx)

        logger.info("===== [TX] Step 3: 填充 Products 表 =====")
        step3 = _step3_populate_products(cfg, brand, brand_info, step2, tx=tx)

        logger.info("===== [TX] Step 4: 生成 BrandTopicRules =====")
        step4 = _step4_generate_topic_rules(cfg, brand, brand_info, step2, tx=tx)

        logger.info(
            "===== [TX] Step 1-4 全部完成，共 %d 条写操作，事务提交 =====",
            tx.op_count(),
        )
        return {
            "brand": brand,
            "step1_brand": step1,
            "step2_audience": step2,
            "step3_products": step3,
            "step4_topic_rules": step4,
            "tx_ops": tx.op_count(),
            "tx_status": "committed",
        }
    except Exception as e:
        logger.error(
            "===== [TX] Step 1-4 失败: %s；开始回滚 %d 条操作 =====",
            e, tx.op_count(),
        )
        rollback_summary = tx.rollback()
        logger.error("===== [TX] 回滚完成: %s =====", rollback_summary)
        # 把 rollback summary 附在异常上，调用方可以取出来
        e._tx_rollback_summary = rollback_summary  # type: ignore[attr-defined]
        raise


def run_init_brand(cfg: Config, brand: str) -> Dict[str, Any]:
    """一键执行 Step 1 + 2 + 3 + 4，初始化一个品牌的完整数据建设。

    前提：Brand 表中已有该品牌的人工维护记录（7 维度全部填好）。
    返回四步的结构化结果摘要。
    """

    brand = brand.strip()
    if not brand:
        raise ValueError("brand 不能为空")

    logger.info("===== Step 1: 初始化 Brand 表 '%s' =====", brand)
    step1 = _step1_init_brand(cfg, brand)
    brand_info = step1["brand_info"]
    logger.info(
        "Step 1 完成: auto_generated=%s, record_id=%s",
        step1.get("auto_generated", False),
        step1.get("record_id", ""),
    )

    logger.info("===== Step 2: 初始化品牌人群表 =====")
    step2 = _step2_init_brand_audience(cfg, brand, brand_info)
    logger.info(
        "Step 2 完成: reused=%s, audience=%s",
        step2.get("reused"),
        step2.get("audience"),
    )

    logger.info("===== Step 3: 填充 Products 表 =====")
    step3 = _step3_populate_products(cfg, brand, brand_info, step2)
    logger.info(
        "Step 3 完成: created=%d, skipped=%d",
        step3.get("products_created", 0),
        step3.get("products_skipped", 0),
    )

    logger.info("===== Step 4: 生成 BrandTopicRules =====")
    step4 = _step4_generate_topic_rules(cfg, brand, brand_info, step2)
    logger.info(
        "Step 4 完成: rules_prompt_length=%d, upserted=%s",
        step4.get("rules_prompt_length", 0),
        step4.get("upserted"),
    )

    return {
        "brand": brand,
        "step1_brand": step1,
        "step2_audience": step2,
        "step3_products": step3,
        "step4_topic_rules": step4,
    }


# ===========================================================================
#  辅助：从品牌人群表读取已有记录（供独立步骤复用依赖）
# ===========================================================================

def load_existing_audience(cfg: Config, brand: str) -> Dict[str, Any]:
    """从 brand_audience 表读取已有的品牌人群画像（支持多行格式）。

    返回合并后的 audience_info dict（兼容 Step 3/4 等消费方），
    同时在 "personas" key 中保留每行独立数据。
    找不到或关键字段不全时抛 ValueError，提示先运行 Step 2。
    """
    app_token = cfg.app_token
    tbl_id = cfg.tables["brand_audience"]["table_id"]
    f = cfg.fields["brand_audience"]

    records = search_all_records(app_token, tbl_id, page_size=200)
    personas: List[Dict[str, Any]] = []
    for item in records:
        fields = item.get("fields") or {}
        name = get_text_field(fields, f["name"], "")
        if name.strip() != brand.strip():
            continue
        info = {
            "audience": fields.get(f["audience"]) or [],
            "persona_tags": get_text_field(fields, f["persona_tags"], ""),
            "motivation": get_text_field(fields, f["motivation"], ""),
            "content_preference": get_text_field(fields, f["content_preference"], ""),
            "persona_description": get_text_field(fields, f["persona_description"], ""),
        }
        if not (info["audience"] and info["motivation"].strip()):
            raise ValueError(
                f"品牌人群表中 '{brand}' 的记录字段不全。请先运行 Step 2 (init_audience) 完成人群画像生成。"
            )
        personas.append(info)

    if not personas:
        raise ValueError(
            f"品牌人群表中未找到 '{brand}' 的记录。请先运行 Step 2 (init_audience)。"
        )

    merged = _merge_personas_for_compat(personas)
    merged["personas"] = personas
    return merged


# ===========================================================================
#  独立步骤入口（每个步骤自动从飞书表加载依赖数据）
# ===========================================================================

def run_step1_init_brand(cfg: Config, brand: str) -> Dict[str, Any]:
    """Step 1 独立入口：判断品牌是否存在，不存在则 LLM 生成 7 维度并写入 Brand 表。"""
    brand = brand.strip()
    if not brand:
        raise ValueError("brand 不能为空")
    logger.info("===== Step 1: 初始化 Brand 表 '%s' =====", brand)
    result = _step1_init_brand(cfg, brand)
    logger.info(
        "Step 1 完成: auto_generated=%s, record_id=%s",
        result.get("auto_generated", False),
        result.get("record_id", ""),
    )
    return result


def run_step2_brand_audience(cfg: Config, brand: str) -> Dict[str, Any]:
    """Step 2 独立入口：生成品牌人群画像。自动加载 Step 1 依赖。"""
    brand = brand.strip()
    if not brand:
        raise ValueError("brand 不能为空")
    logger.info("===== Step 2: 初始化品牌人群表 '%s' =====", brand)
    step1 = _step1_init_brand(cfg, brand)
    brand_info = step1["brand_info"]
    result = _step2_init_brand_audience(cfg, brand, brand_info)
    logger.info("Step 2 完成: reused=%s, audience=%s", result.get("reused"), result.get("audience"))
    return result


def run_step3_products(cfg: Config, brand: str) -> Dict[str, Any]:
    """Step 3 独立入口：生成产品线。自动加载 Step 1 + Step 2 依赖。"""
    brand = brand.strip()
    if not brand:
        raise ValueError("brand 不能为空")
    logger.info("===== Step 3: 填充 Products 表 '%s' =====", brand)
    step1 = _step1_init_brand(cfg, brand)
    brand_info = step1["brand_info"]
    audience_info = load_existing_audience(cfg, brand)
    result = _step3_populate_products(cfg, brand, brand_info, audience_info)
    logger.info("Step 3 完成: created=%d, skipped=%d", result.get("products_created", 0), result.get("products_skipped", 0))
    return result


def run_step4_topic_rules(cfg: Config, brand: str) -> Dict[str, Any]:
    """Step 4 独立入口：生成品牌专属话题策略。自动加载 Step 1 + Step 2 依赖。"""
    brand = brand.strip()
    if not brand:
        raise ValueError("brand 不能为空")
    logger.info("===== Step 4: 生成 BrandTopicRules '%s' =====", brand)
    step1 = _step1_init_brand(cfg, brand)
    brand_info = step1["brand_info"]
    audience_info = load_existing_audience(cfg, brand)
    result = _step4_generate_topic_rules(cfg, brand, brand_info, audience_info)
    logger.info("Step 4 完成: rules_prompt_length=%d, upserted=%s", result.get("rules_prompt_length", 0), result.get("upserted"))
    return result
