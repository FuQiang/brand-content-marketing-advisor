#!/usr/bin/env python3
"""为已有的 ContentMatrix 记录补生封面（dreamina）和视频（dreamina），写回多维表。

用法:
    python3 backfill_assets.py --brand 君乐宝 [--max-videos 3] [--dry-run]

逻辑：
1. 读取 ContentMatrix 中该品牌的所有记录
2. 对每条记录，根据文案内容精准构造 prompt
3. 用 dreamina image2image（产品底图）或 text2image 生成双平台封面
4. 用 dreamina text2video 生成 9:16 视频（最多 max_videos 条）
5. 上传附件并写回多维表对应字段
"""

import argparse
import json
import logging
import os
import sys

# 确保 bcma 包可导入
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bcma.bitable import (
    get_client,
    search_all_records,
    update_single,
    upload_attachment_file,
    download_attachment_file,
    ensure_field_exists,
)
from bcma.config import load_config
from bcma.brand_pipeline import (
    _build_douyin_cover_prompt,
    _build_xhs_cover_prompt,
    _dreamina_image2image,
    _dreamina_text2image,
    _dreamina_text2video,
    _dreamina_available,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("backfill_assets")


def _get_text(fields: dict, key: str) -> str:
    """从 fields 中安全提取文本值（兼容飞书多种格式）。"""
    val = fields.get(key)
    if val is None:
        return ""
    if isinstance(val, str):
        return val.strip()
    if isinstance(val, list):
        # 多维表文本字段可能是 [{"text": "..."}] 格式
        parts = []
        for item in val:
            if isinstance(item, dict):
                parts.append(item.get("text", ""))
            elif isinstance(item, str):
                parts.append(item)
        return " ".join(parts).strip()
    if isinstance(val, dict):
        return val.get("text", str(val)).strip()
    return str(val).strip()


def _has_attachment(fields: dict, key: str) -> bool:
    """判断某个附件字段是否已有内容。"""
    val = fields.get(key)
    if not val:
        return False
    if isinstance(val, list) and len(val) > 0:
        return True
    return False


def main():
    parser = argparse.ArgumentParser(description="补生封面和视频到 ContentMatrix")
    parser.add_argument("--brand", required=True, help="品牌名称")
    parser.add_argument("--max-videos", type=int, default=3, help="最多生成几条视频")
    parser.add_argument("--dry-run", action="store_true", help="仅打印计划，不执行生成")
    args = parser.parse_args()

    brand = args.brand.strip()
    max_videos = args.max_videos

    if not _dreamina_available():
        logger.error("dreamina CLI 未安装或不在 PATH 中")
        sys.exit(1)

    cfg = load_config()
    app_token = cfg.app_token
    cm_tbl = cfg.tables["content_matrix"]["table_id"]
    f_cm = cfg.fields["content_matrix"]

    # 字段名
    brand_field = f_cm.get("brand", "适用品牌")
    topic_field = f_cm.get("topic", "匹配话题")
    hook_field = f_cm.get("hook", "爆款标题/钩子")
    body_field = f_cm.get("body", "正文与脚本")
    visuals_field = f_cm.get("visuals", "视觉画面建议")
    products_field = f_cm.get("products", "主推产品")
    audience_field = f_cm.get("audience", "目标人群")
    douyin_script_field = f_cm.get("douyin_script", "抖音短视频脚本")
    xhs_title_field = f_cm.get("xhs_title", "小红书标题")
    xhs_note_field = f_cm.get("xhs_note", "小红书种草笔记")
    douyin_cover_field = f_cm.get("douyin_cover", "抖音封面(9:16)")
    xhs_cover_field = f_cm.get("xhs_cover", "小红书封面(3:4)")
    cover_ai_legacy = f_cm.get("cover_image_ai_field", "视频封面(AI生成)")
    video_field = f_cm.get("video_asset_ai", "视频素材(AI生成)")

    # 确保附件字段存在
    for fname in [douyin_cover_field, xhs_cover_field, cover_ai_legacy, video_field]:
        if fname:
            try:
                ensure_field_exists(app_token, cm_tbl, fname, field_type=17)
            except Exception as e:
                logger.warning("确保字段 '%s' 存在失败: %s", fname, e)

    # 读取所有 ContentMatrix 记录
    logger.info("读取 ContentMatrix 表 (table_id=%s)...", cm_tbl)
    all_records = search_all_records(app_token, cm_tbl, page_size=200)
    logger.info("共 %d 条记录", len(all_records))

    # 过滤该品牌的记录
    brand_records = []
    for rec in all_records:
        fields = rec.get("fields", {})
        rec_brand = _get_text(fields, brand_field)
        if brand in rec_brand:
            brand_records.append(rec)

    logger.info("品牌「%s」的记录: %d 条", brand, len(brand_records))
    if not brand_records:
        logger.info("无记录需要处理")
        return

    # 尝试下载产品底图（从 Products 表获取）
    products_tbl = cfg.tables["products"]["table_id"]
    f_products = cfg.fields.get("products", {})
    gallery_field = f_products.get("asset_gallery_field", "产品图库(真实大片)")
    product_brand_field = f_products.get("brand", "所属品牌")

    base_image_path = None
    try:
        product_records = search_all_records(app_token, products_tbl, page_size=200)
        for prec in product_records:
            pfields = prec.get("fields", {})
            pbrand = _get_text(pfields, product_brand_field)
            if brand not in pbrand:
                continue
            attachments = pfields.get(gallery_field)
            if not isinstance(attachments, list) or not attachments:
                continue
            for att in attachments:
                ft = att.get("file_token") or att.get("token") or att.get("fileToken")
                if ft:
                    import tempfile
                    dl_dir = tempfile.mkdtemp(prefix="bcma_base_")
                    dl_path = os.path.join(dl_dir, f"{ft}.png")
                    result_path = download_attachment_file(app_token, str(ft))
                    if result_path and os.path.isfile(result_path):
                        base_image_path = result_path
                        logger.info("产品底图已下载: %s", base_image_path)
                    break
            if base_image_path:
                break
    except Exception as e:
        logger.warning("获取产品底图失败: %s", e)

    # 开始处理
    video_count = 0
    cover_douyin_count = 0
    cover_xhs_count = 0

    for idx, rec in enumerate(brand_records):
        record_id = rec.get("record_id")
        fields = rec.get("fields", {})
        topic = _get_text(fields, topic_field)
        hook = _get_text(fields, hook_field)
        body = _get_text(fields, body_field)
        visuals = _get_text(fields, visuals_field)
        products = _get_text(fields, products_field)
        audience = _get_text(fields, audience_field)
        xhs_title = _get_text(fields, xhs_title_field)
        douyin_script = _get_text(fields, douyin_script_field)

        logger.info("--- [%d/%d] record_id=%s 话题=「%s」---",
                     idx + 1, len(brand_records), record_id, topic[:30])

        if args.dry_run:
            has_dy_cover = _has_attachment(fields, douyin_cover_field)
            has_xhs_cover = _has_attachment(fields, xhs_cover_field)
            has_video = _has_attachment(fields, video_field)
            logger.info("  抖音封面: %s | 小红书封面: %s | 视频: %s",
                        "已有" if has_dy_cover else "需生成",
                        "已有" if has_xhs_cover else "需生成",
                        "已有" if has_video else ("需生成" if video_count < max_videos else "跳过"))
            if not has_video:
                video_count += 1
            continue

        payload = {}

        # === 抖音 9:16 封面 ===
        if not _has_attachment(fields, douyin_cover_field) and hook:
            logger.info("  生成抖音封面...")
            dy_prompt = _build_douyin_cover_prompt(
                brand=brand,
                topic_title=topic,
                hook=hook,
                visual_direction=visuals,
            )
            dy_path = _dreamina_image2image(base_image_path, dy_prompt, ratio="9:16") if base_image_path else _dreamina_text2image(dy_prompt, ratio="9:16")
            if dy_path:
                try:
                    ft = upload_attachment_file(app_token, dy_path)
                    if ft:
                        payload[douyin_cover_field] = [{"file_token": ft}]
                        if cover_ai_legacy:
                            payload[cover_ai_legacy] = [{"file_token": ft}]
                        cover_douyin_count += 1
                        logger.info("  抖音封面上传成功: %s", ft)
                except Exception as e:
                    logger.warning("  抖音封面上传失败: %s", e)
            else:
                logger.warning("  抖音封面生成失败")

        # === 小红书 3:4 封面 ===
        if not _has_attachment(fields, xhs_cover_field) and xhs_title:
            logger.info("  生成小红书封面...")
            xhs_prompt = _build_xhs_cover_prompt(
                brand=brand,
                topic_title=topic,
                xhs_title=xhs_title,
                visual_direction=visuals,
            )
            xhs_path = _dreamina_image2image(base_image_path, xhs_prompt, ratio="3:4") if base_image_path else _dreamina_text2image(xhs_prompt, ratio="3:4")
            if xhs_path:
                try:
                    ft = upload_attachment_file(app_token, xhs_path)
                    if ft:
                        payload[xhs_cover_field] = [{"file_token": ft}]
                        cover_xhs_count += 1
                        logger.info("  小红书封面上传成功: %s", ft)
                except Exception as e:
                    logger.warning("  小红书封面上传失败: %s", e)
            else:
                logger.warning("  小红书封面生成失败")

        # === 视频（最多 max_videos 条）===
        if not _has_attachment(fields, video_field) and video_count < max_videos:
            script = douyin_script or body or ""
            if script:
                logger.info("  生成视频 (%d/%d)...", video_count + 1, max_videos)
                video_prompt = (
                    f"为品牌「{brand}」制作一条 9:16 竖版短视频。\n"
                    f"话题：{topic}\n"
                    f"目标人群：{audience}\n"
                    f"主推产品：{products}\n"
                    f"脚本：{script[:500]}\n"
                    f"视觉风格：{visuals}\n"
                    f"重点约束：实施「品牌Logo精准保护」，请准确还原【{brand}】的官方品牌 Logo，"
                    "Logo 必须清晰、标准、比例正确、无变形、无乱码错字。"
                )
                video_path = _dreamina_text2video(video_prompt, duration=5, poll_seconds=180)
                if video_path:
                    try:
                        ft = upload_attachment_file(app_token, video_path, file_type="bitable_file")
                        if ft:
                            payload[video_field] = [{"file_token": ft}]
                            video_count += 1
                            logger.info("  视频上传成功: %s", ft)
                    except Exception as e:
                        logger.warning("  视频上传失败: %s", e)
                else:
                    logger.warning("  视频生成失败")

        # 写回多维表
        if payload:
            try:
                update_single(app_token, cm_tbl, record_id, payload)
                logger.info("  已写回 %d 个字段", len(payload))
            except Exception as e:
                logger.warning("  写回失败: %s", e)
        else:
            logger.info("  无需更新")

    logger.info("=== 完成 ===")
    logger.info("抖音封面: %d | 小红书封面: %d | 视频: %d", cover_douyin_count, cover_xhs_count, video_count)


if __name__ == "__main__":
    main()
