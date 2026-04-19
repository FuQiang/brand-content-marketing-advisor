"""Product visual asset completion utilities.

职责：
- 基于品牌名，从 Products 表中筛选对应品牌的主推产品（当前实现为该品牌下所有产品记录）。
- 当产品记录的「产品图库(真实大片)」附件列为空时，通过搜索引擎抓取「真实产品图」
  3–5 张（优先 DuckDuckGo→页面 og:image，兜底 Bing 图片），上传为附件回填。

实现细节：
- 图片获取走 `bcma.image_search.search_real_product_images`，不再走 AI 生成，
  也不再依赖 AIME image_search；
- 搜索关键词由品牌信息 + 产品字段（名称、系列、材质等）拼接而来；
- 搜索失败的产品会跳过（不阻断其他产品的处理）。
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

from .bitable import (
    ensure_field_exists,
    search_records_with_filter,
    update_single,
    upload_attachment_file,
    list_table_fields,
    delete_field_if_exists,
)
from .image_search import search_real_product_images
from .utils import get_text_field

logger = logging.getLogger("bcma.product_assets")


def resolve_app_token_from_base_url(base_url: str) -> str:
    """从飞书多维表格 Base URL 中解析 app_token。

    示例：
        https://<tenant>.larkoffice.com/base/<APP_TOKEN>
        → app_token = "<APP_TOKEN>"
    """

    base_url = (base_url or "").strip()
    if not base_url:
        return ""

    try:
        from urllib.parse import urlparse

        parsed = urlparse(base_url)
        parts = [p for p in (parsed.path or "").split("/") if p]
        if "base" in parts:
            idx = parts.index("base")
            if idx + 1 < len(parts):
                return parts[idx + 1]
    except Exception:
        # 解析失败时直接返回原始字符串，交由上层处理
        pass

    return base_url


def _build_product_search_query(
    brand: str,
    fields: Dict[str, Any],
    fields_cfg: Dict[str, Any],
) -> str:
    """基于品牌 + 产品字段构造一条搜索关键词。"""

    f = fields_cfg
    def _g(key: str) -> str:
        col = f.get(key)
        return get_text_field(fields, col, "") if col else ""

    name = _g("name")
    series = _g("series")
    material = _g("material")

    parts: List[str] = [brand]
    if name:
        parts.append(name)
    if series:
        parts.append(series)
    if material:
        parts.append(material)
    parts.append("产品图")
    return " ".join(p for p in parts if p)


def _search_product_images(
    brand: str,
    fields: Dict[str, Any],
    fields_cfg: Dict[str, Any],
    num: int,
) -> List[str]:
    """搜索 N 张真实产品图，返回本地文件路径列表（可能少于 num）。"""

    query = _build_product_search_query(brand, fields, fields_cfg)
    if not query.strip():
        return []
    download_dir = tempfile.mkdtemp(prefix="product_img_search_")
    return search_real_product_images(query=query, num=max(1, num), download_dir=download_dir)


def list_products_by_brand(
    brand: str,
    app_token: str,
    tables_cfg: Dict[str, Any],
    fields_cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """列出指定品牌在 Products 表中的产品记录（原始记录结构）。"""

    brand = (brand or "").strip()
    if not brand:
        return []

    products_tbl = tables_cfg.get("products", {})
    tbl_id = products_tbl.get("table_id")
    if not tbl_id:
        return []

    f_products = fields_cfg.get("products", {})
    brand_field = f_products.get("brand")
    if not brand_field:
        return []

    records = search_records_with_filter(app_token, tbl_id, [])

    def _brand_text(value: Any) -> str:
        if isinstance(value, list):
            parts: List[str] = []
            for item in value:
                if isinstance(item, dict):
                    parts.append(str(item.get("text") or item.get("name") or ""))
                else:
                    parts.append(str(item))
            return " ".join(parts)
        if isinstance(value, dict):
            return str(value.get("text") or value.get("name") or "")
        return str(value or "")

    return [
        rec for rec in records
        if brand in _brand_text(rec.get("fields", {}).get(brand_field))
    ]


def upload_gallery_images(
    app_token: str,
    tables_cfg: Dict[str, Any],
    fields_cfg: Dict[str, Any],
    record_id: str,
    file_paths: List[str],
) -> List[str]:
    """将本地图片上传为附件并写入 Products 表的图库字段。

    返回成功写入的 file_token 列表。"""

    if not record_id or not file_paths:
        return []

    products_tbl = tables_cfg.get("products", {})
    tbl_id = products_tbl.get("table_id")
    if not tbl_id:
        return []

    f_products = fields_cfg.get("products", {})
    gallery_field = f_products.get("asset_gallery_field", "产品图库(真实大片)")

    # 确保图库字段存在且为附件类型
    ensure_field_exists(
        app_token,
        tbl_id,
        gallery_field,
        type_code=17,
        ui_type="Attachment",
        property_obj=None,
    )

    tokens: List[Dict[str, str]] = []
    raw_tokens: List[str] = []
    for path in file_paths:
        abs_path = os.path.abspath(path)
        if not os.path.isfile(abs_path):
            continue
        try:
            token = upload_attachment_file(app_token, abs_path, file_type="bitable_image")
        except Exception as e:
            logger.warning("上传图片失败 path='%s': %s", abs_path, e)
            continue
        tokens.append({"file_token": token})
        raw_tokens.append(token)

    if not tokens:
        return []

    update_single(app_token, tbl_id, record_id, {gallery_field: tokens})
    return raw_tokens


def ensure_product_gallery_for_brand(
    brand: str,
    app_token: str,
    tables_cfg: Dict[str, Any],
    fields_cfg: Dict[str, Any],
    min_images: int = 3,
    max_images: int = 5,
) -> Dict[str, Any]:
    """为指定品牌的主推产品补全「产品图库(真实大片)」附件。

    当前实现策略：
    - 将 Products 表中 `brand` 字段包含指定品牌名的记录视为该品牌主推产品候选；
    - 若候选记录在图库字段中已存在附件，则跳过；
    - 若为空，则调用本地 dreamina text2image 生成产品大片并写入附件列；
    - 若生成结果不足 `min_images` 张，仍会尽可能写入可用结果，但不会视为失败。

    返回：
        {
          "brand": 品牌名,
          "products_scanned": 总记录数,
          "products_with_existing_gallery": 已有图库记录数,
          "products_updated": 实际补全图库的记录数,
          "updated_record_ids": [record_id1, record_id2, ...],
        }
    """

    brand = (brand or "").strip()
    if not brand:
        return {
            "brand": brand,
            "products_scanned": 0,
            "products_with_existing_gallery": 0,
            "products_updated": 0,
            "updated_record_ids": [],
        }

    records = list_products_by_brand(brand, app_token, tables_cfg, fields_cfg)
    if not records:
        return {
            "brand": brand,
            "products_scanned": 0,
            "products_with_existing_gallery": 0,
            "products_updated": 0,
            "updated_record_ids": [],
        }

    products_tbl = tables_cfg.get("products", {})
    tbl_id = products_tbl.get("table_id")
    f_products = fields_cfg.get("products", {})
    gallery_field = f_products.get("asset_gallery_field", "产品图库(真实大片)")
    name_field = f_products.get("name")

    # 兜底确保图库字段存在
    ensure_field_exists(
        app_token,
        tbl_id,
        gallery_field,
        type_code=17,
        ui_type="Attachment",
        property_obj=None,
    )

    products_scanned = 0
    products_with_existing_gallery = 0
    products_updated = 0
    updated_record_ids: List[str] = []

    gen_count = max(1, int(min_images))
    cap = max(gen_count, int(max_images))

    for item in records:
        record_id = item.get("record_id") or ""
        if not record_id:
            continue

        fields = item.get("fields") or {}
        products_scanned += 1

        existing_gallery = fields.get(gallery_field) or []
        if isinstance(existing_gallery, list) and existing_gallery:
            products_with_existing_gallery += 1
            continue

        # 搜索真实产品图
        selected_paths = _search_product_images(
            brand=brand,
            fields=fields,
            fields_cfg=f_products,
            num=gen_count,
        )
        if not selected_paths:
            logger.warning(
                "搜索产品图全部失败，跳过 record_id='%s' name='%s'",
                record_id,
                get_text_field(fields, name_field, "") if name_field else "",
            )
            continue
        selected_paths = selected_paths[:cap]

        try:
            upload_gallery_images(app_token, tables_cfg, fields_cfg, record_id, selected_paths)
        except Exception as e:
            logger.warning("写入图库失败 record_id='%s': %s", record_id, e)
            continue

        products_updated += 1
        updated_record_ids.append(record_id)

    return {
        "brand": brand,
        "products_scanned": products_scanned,
        "products_with_existing_gallery": products_with_existing_gallery,
        "products_updated": products_updated,
        "updated_record_ids": updated_record_ids,
    }


def cleanup_empty_columns_for_products(
    app_token: str,
    tables_cfg: Dict[str, Any],
    fields_cfg: Dict[str, Any],
    non_removable_fields: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """清理 Products 表中所有记录均为空的非核心字段。

    使用单次全表扫描判断所有字段的填充情况，避免逐字段拉取全表数据。

    - 核心字段通过 non_removable_fields 传入，为字段名列表；
      若未显式提供，则基于 config 中已有字段名构造兜底集合；
    - 当表内记录数不足 MIN_RECORDS_FOR_CLEANUP 时，跳过清理以避免误删；
    - 返回删除的字段名列表，便于调用方在日志或结果中展示。
    """

    MIN_RECORDS_FOR_CLEANUP = 3

    products_tbl = tables_cfg.get("products", {}) or {}
    tbl_id = products_tbl.get("table_id")
    if not tbl_id:
        return {"deleted_fields": []}

    f_products = fields_cfg.get("products", {}) or {}

    # 构造不可删除字段集合：优先使用 config 中显式给出的列表
    non_removable: List[str] = []
    if isinstance(non_removable_fields, list):
        non_removable = [str(x) for x in non_removable_fields if x]

    if not non_removable:
        candidates = [
            f_products.get("brand"),
            f_products.get("name"),
            f_products.get("series"),
            f_products.get("selling_point"),
            f_products.get("persona_tags"),
            f_products.get("functions"),
            f_products.get("season"),
            f_products.get("base_weight"),
            f_products.get("asset_gallery_field", "产品图库(真实大片)"),
        ]
        non_removable = [str(x) for x in candidates if x]

    # 额外保护常见系统字段
    non_removable.extend([
        "记录ID",
        "创建时间",
        "修改时间",
        "创建人",
        "修改人",
    ])

    non_removable_set = set(non_removable)

    field_meta_map = list_table_fields(app_token, tbl_id)

    # 单次全表扫描，在内存中判断每个字段是否全空
    from .bitable import search_all_records
    records = search_all_records(app_token, tbl_id, view_id=None, automatic_fields=False, page_size=200)

    if len(records) < MIN_RECORDS_FOR_CLEANUP:
        # 记录数过少时跳过清理，避免因数据异常导致误删
        return {"deleted_fields": []}

    non_empty_fields: set[str] = set()
    for item in records:
        fields = item.get("fields") or {}
        for k, v in fields.items():
            if k in non_empty_fields:
                continue
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            if isinstance(v, list) and len(v) == 0:
                continue
            non_empty_fields.add(k)

    deleted: List[str] = []
    for field_name in field_meta_map.keys():
        if not field_name:
            continue
        if field_name in non_removable_set:
            continue
        if field_name in non_empty_fields:
            continue

        try:
            delete_field_if_exists(app_token, tbl_id, field_name)
            deleted.append(field_name)
        except Exception as e:
            logger.warning("删除空列 '%s' 失败: %s", field_name, e)
            continue

    return {"deleted_fields": deleted}
