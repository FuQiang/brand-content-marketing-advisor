"""Table schema sync — v5.7.0.

作用：基于 `config.yaml` 中 `fields.*` 登记的字段映射，对技能管理的飞书多维表格做**结构对齐**：

- **创建缺失字段**：`config.fields[table]` 中登记、但 Bitable 表里不存在的字段，按类型推断
  映射自动建列（type_code / ui_type）。
- **安全清理多余空列**：Bitable 表里存在、但 `config.fields[table]` 未登记的列，仅在**整列全空**
  且表中记录数 ≥ `MIN_RECORDS_FOR_CLEANUP` 时才删除。非空或样本量不足一律保留。

覆盖范围：
- **跳过** `brands` 表（人工维护的品牌知识库底座，Skill 永不改其结构）。
- 默认处理 `brand_audience` / `products` / `brand_topic_rules` / `topic_selection` /
  `content_matrix` 五张 Skill 托管表，可通过 `table_keys` 指定子集。

设计原则：
- **可重入 / 幂等**：已存在字段不重复创建，`list_table_fields` 结果做 diff。
- **破坏性操作高门槛**：删除只针对 (不在登记 + 全空 + 样本足够) 的交集，任一条件不满足即保留。
- **per-table 独立**：任一表失败只影响当前表，返回摘要中记录错误原因，继续处理其他表。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .bitable import (
    delete_field_if_exists,
    ensure_field_exists,
    list_table_fields,
    search_all_records,
)
from .config import Config

logger = logging.getLogger("bcma.schema_sync")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# 样本量门槛：表中记录数少于这个阈值时，跳过删除空列，避免冷启动时误删
MIN_RECORDS_FOR_CLEANUP = 3

# brands 表是人工维护的品牌知识库底座，Skill 永远不改其结构
HARD_SKIP_TABLE_KEYS = frozenset({"brands"})

# 默认参与 schema 同步的表 key 列表（保持稳定顺序，方便输出摘要阅读）
DEFAULT_TABLE_KEYS: Tuple[str, ...] = (
    "brand_audience",
    "products",
    "brand_topic_rules",
    "topic_selection",
    "content_matrix",
)

# 系统保护列 —— 任何表都保留这些列，不参与删除判定
SYSTEM_PROTECTED_FIELDS = frozenset({
    "记录ID",
    "创建时间",
    "修改时间",
    "创建人",
    "修改人",
})


# ---------------------------------------------------------------------------
# Type inference
# ---------------------------------------------------------------------------

# 字段名 → (type_code, ui_type)
#
# 约定参考：
#   1  Text          —— 单行/多行文本
#   2  Number        —— 数字
#   4  MultiSelect   —— 多选
#   5  DateTime      —— 日期时间（毫秒时间戳）
#   17 Attachment    —— 附件
#
# 推断策略：按字段名后缀 / 具体字段名 / 字段 key 名三层兜底。
_ATTACHMENT_KEY_HINTS = (
    "asset_gallery_field",
    "cover_image",
    "cover_image_ai_field",
    "douyin_cover",
    "xhs_cover",
    "video_asset_ai",
)

_NUMBER_KEY_HINTS = (
    "r1",
    "r2",
    "r3",
    "r4",
    "total_score",
    "base_weight",
)

_DATETIME_KEY_HINTS = (
    "created_at",
    "fetched_at",
    "generated_at",
)

# 会被展示成多选的 field key
_MULTISELECT_KEY_HINTS = (
    "audience",           # brand_audience.典型人群受众 / topic_selection.适用人群
    "persona_tags",       # products.目标人群标签 / brand_audience.画像标签
    "functions",          # products.功能点
    "platforms",          # content_matrix.适用平台
)


def _infer_field_type(field_key: str, field_name: str) -> Tuple[int, str]:
    """根据字段 key 名推断 (type_code, ui_type)，未命中规则一律兜底 Text (1/Text)。"""

    key = (field_key or "").lower()

    if key in _ATTACHMENT_KEY_HINTS:
        return 17, "Attachment"

    if key in _NUMBER_KEY_HINTS:
        return 2, "Number"

    if key in _DATETIME_KEY_HINTS:
        return 5, "DateTime"

    if key in _MULTISELECT_KEY_HINTS:
        return 4, "MultiSelect"

    # 兜底：文本
    return 1, "Text"


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _collect_configured_field_names(fields_cfg: Dict[str, Any]) -> List[Tuple[str, str]]:
    """从 config.fields[table] 中收集 (key, 飞书字段名) 二元组列表。

    注意：
    - 值必须是 str 才视为"一个字段映射"；遇到 list / dict（如 products.non_removable_fields
      或未来可能出现的嵌套结构）一律跳过。
    - 空字符串值跳过。
    """

    out: List[Tuple[str, str]] = []
    if not isinstance(fields_cfg, dict):
        return out
    for key, value in fields_cfg.items():
        if not isinstance(value, str):
            continue
        name = value.strip()
        if not name:
            continue
        out.append((str(key), name))
    return out


def _scan_non_empty_columns(
    app_token: str,
    table_id: str,
) -> Tuple[int, set[str]]:
    """单次全表扫描，返回 (记录数, 非空列名集合)。

    只拉一次分页，在内存中判断每个字段是否全空，避免逐字段扫表。
    """

    records = search_all_records(
        app_token,
        table_id,
        view_id=None,
        automatic_fields=False,
        page_size=200,
    )

    non_empty: set[str] = set()
    for item in records:
        fields = item.get("fields") or {}
        for k, v in fields.items():
            if k in non_empty:
                continue
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            if isinstance(v, (list, dict)) and len(v) == 0:
                continue
            non_empty.add(k)

    return len(records), non_empty


# ---------------------------------------------------------------------------
# Per-table sync
# ---------------------------------------------------------------------------

def sync_table_schema(
    cfg: Config,
    table_key: str,
) -> Dict[str, Any]:
    """同步单张表的 schema。

    流程：
    1. 跳过 HARD_SKIP 表（brands 人工维护底座）。
    2. 查 `cfg.tables[table_key].table_id`；为空则 skip 并记录原因。
    3. 收集 `cfg.fields[table_key]` 登记的字段名，查 `list_table_fields` 做 diff。
    4. 对 diff 出的缺失字段，按 `_infer_field_type` 类型映射创建；失败记录错误。
    5. 单次全表扫描得到 (记录数, 非空列集合)；
       - 记录数 < MIN_RECORDS_FOR_CLEANUP → 跳过删除。
       - 否则对"Bitable 存在但 config 未登记且全空且非系统保护列"的列做 `delete_field_if_exists`。
    """

    result: Dict[str, Any] = {
        "table": table_key,
        "table_id": "",
        "skipped": False,
        "skipped_reason": "",
        "created_fields": [],
        "deleted_empty_fields": [],
        "kept_non_empty_fields": [],
        "errors": [],
    }

    if table_key in HARD_SKIP_TABLE_KEYS:
        result["skipped"] = True
        result["skipped_reason"] = f"{table_key} 表为人工维护底座，Skill 永不改其结构"
        return result

    tables_cfg = cfg.tables or {}
    tbl_cfg = tables_cfg.get(table_key) or {}
    table_id = (tbl_cfg.get("table_id") or "").strip()
    if not table_id:
        result["skipped"] = True
        result["skipped_reason"] = f"config.tables.{table_key}.table_id 未配置"
        return result
    result["table_id"] = table_id

    fields_cfg = (cfg.fields or {}).get(table_key) or {}
    configured = _collect_configured_field_names(fields_cfg)
    configured_name_set = {name for _, name in configured}

    # 读取 Bitable 实际 schema
    try:
        existing_meta = list_table_fields(cfg.app_token, table_id)
    except Exception as e:
        msg = f"list_table_fields 失败: {e}"
        logger.warning("[%s] %s", table_key, msg)
        result["errors"].append(msg)
        return result

    existing_names: set[str] = set(existing_meta.keys())

    # --- 1) 创建缺失字段 ---
    for key, name in configured:
        if name in existing_names:
            continue
        type_code, ui_type = _infer_field_type(key, name)
        try:
            ensure_field_exists(
                cfg.app_token,
                table_id,
                name,
                type_code=type_code,
                ui_type=ui_type,
                property_obj=None,
            )
            result["created_fields"].append({
                "field_key": key,
                "field_name": name,
                "type_code": type_code,
                "ui_type": ui_type,
            })
            existing_names.add(name)
        except Exception as e:
            msg = f"创建字段 '{name}' (type={type_code}/{ui_type}) 失败: {e}"
            logger.warning("[%s] %s", table_key, msg)
            result["errors"].append(msg)

    # --- 2) 扫表 & 判定删除候选 ---
    try:
        record_count, non_empty_cols = _scan_non_empty_columns(cfg.app_token, table_id)
    except Exception as e:
        msg = f"全表扫描失败（跳过删除阶段）: {e}"
        logger.warning("[%s] %s", table_key, msg)
        result["errors"].append(msg)
        result["record_count"] = 0
        return result

    result["record_count"] = record_count

    if record_count < MIN_RECORDS_FOR_CLEANUP:
        # 样本量不足，全部保留，标注原因
        result["cleanup_skipped_reason"] = (
            f"记录数 {record_count} < MIN_RECORDS_FOR_CLEANUP({MIN_RECORDS_FOR_CLEANUP})，跳过空列删除"
        )
        return result

    # 删除候选 = Bitable 有 ∧ config 没登记 ∧ 非系统保护 ∧ 整列全空
    for field_name in list(existing_names):
        if field_name in configured_name_set:
            continue
        if field_name in SYSTEM_PROTECTED_FIELDS:
            continue
        if field_name in non_empty_cols:
            # 非空列 —— 可能是用户手动加的自定义列，绝对保留
            result["kept_non_empty_fields"].append(field_name)
            continue
        try:
            delete_field_if_exists(cfg.app_token, table_id, field_name)
            result["deleted_empty_fields"].append(field_name)
        except Exception as e:
            msg = f"删除空列 '{field_name}' 失败: {e}"
            logger.warning("[%s] %s", table_key, msg)
            result["errors"].append(msg)

    return result


# ---------------------------------------------------------------------------
# Multi-table sync
# ---------------------------------------------------------------------------

def sync_all_schemas(
    cfg: Config,
    table_keys: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """批量同步多张表 schema，返回聚合摘要。

    Args:
        cfg: 已加载的 Config 对象（`cfg.app_token` / `cfg.tables` / `cfg.fields`）。
        table_keys: 指定要处理的 config.tables key 列表。None 时使用 DEFAULT_TABLE_KEYS。
                    传入的 key 若落在 HARD_SKIP_TABLE_KEYS（如 `brands`）中会被跳过。

    Returns:
        {
            "tables_processed": [...],
            "tables_skipped": [{"table": ..., "reason": ...}],
            "created_fields": {table_key: [ {field_key, field_name, type_code, ui_type}, ... ]},
            "deleted_empty_fields": {table_key: [field_name, ...]},
            "kept_non_empty_fields": {table_key: [field_name, ...]},
            "errors": {table_key: [error_message, ...]},
            "record_counts": {table_key: int},
        }
    """

    if table_keys is None:
        table_keys = list(DEFAULT_TABLE_KEYS)
    else:
        table_keys = list(table_keys)

    summary: Dict[str, Any] = {
        "tables_processed": [],
        "tables_skipped": [],
        "created_fields": {},
        "deleted_empty_fields": {},
        "kept_non_empty_fields": {},
        "errors": {},
        "record_counts": {},
    }

    for key in table_keys:
        key = (key or "").strip()
        if not key:
            continue

        try:
            tbl_result = sync_table_schema(cfg, key)
        except Exception as e:
            logger.warning("[%s] schema 同步异常: %s", key, e)
            summary["errors"][key] = [f"schema 同步异常: {e}"]
            continue

        if tbl_result.get("skipped"):
            summary["tables_skipped"].append({
                "table": key,
                "reason": tbl_result.get("skipped_reason", ""),
            })
            continue

        summary["tables_processed"].append(key)

        created = tbl_result.get("created_fields") or []
        if created:
            summary["created_fields"][key] = created

        deleted = tbl_result.get("deleted_empty_fields") or []
        if deleted:
            summary["deleted_empty_fields"][key] = deleted

        kept = tbl_result.get("kept_non_empty_fields") or []
        if kept:
            summary["kept_non_empty_fields"][key] = kept

        errs = tbl_result.get("errors") or []
        if errs:
            summary["errors"][key] = errs

        if "record_count" in tbl_result:
            summary["record_counts"][key] = tbl_result["record_count"]

    return summary
