"""Brand content pipeline — Step 6 of v5.7.0.

第六步：基于品牌名，读取 `TopicSelection` 表中**由第五步写入的** Top K 话题
（同品牌，按时间/总分降序），匹配主推产品、生成双平台文案并写入 `ContentMatrix`，
同时为 Top K 内容补全双平台封面 + 9:16 AI 视频。

v5.7.0 改动：
- 视觉资产生成全面切换至 `dreamina` CLI（即梦官方工具）：
  * 封面生成：`dreamina image2image`（产品底图 + prompt），兜底 `dreamina text2image`；
  * 视频生成：`dreamina text2video`（seedance2.0_vip 模型，9:16 竖版）；
  * 封面为所有 Top K 内容均生成；视频每次执行最多生成 3 条，节省积分。
- prompt 基于 content_matrix 表实际内容（话题、产品、文案、人群）精准构造。

v5.6.0 基础：
- 文案从模板化切换为 LLM 生成双平台版本（见 `bcma/copywriting.py`）：
  * 抖音短视频分镜脚本 → 写入 `douyin_script` 列；
  * 小红书种草笔记（标题 + 800-1200 字正文 + tags）→ 写入 `xhs_title` / `xhs_note` 列；
  * 通用字段 `hook/body/visuals` 同步镜像回填（向后兼容 v5.5.0 及以前看板）。
- 封面从单张 AI 封面扩展为 **双平台 AI 封面**（共用同一张产品图库底图）：
  * 抖音 9:16 首帧封面 → `douyin_cover`；
  * 小红书 3:4 封面 → `xhs_cover`；
  * 旧 `cover_image_ai_field` 保留并回填抖音 9:16 内容。
- 视频仍为一条 9:16 AI 视频（两个平台共用）。

注意：
- 所有多维表读写均通过 `managing-lark-bitable-data` 封装模块完成
- 封面底图复用 Products 表「产品图库(真实大片)」，字段由 config.yaml 的
  `fields.products.asset_gallery_field` 控制
- 视觉资产生成依赖 `dreamina` CLI；若 CLI 不可用或未登录，
  会自动跳过对应资产生成，保留文字内容
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .bitable import (
    ensure_field_exists,
    search_all_records,
    update_single,
    upload_attachment_file,
    download_attachment_file,
)
from .config import Config
from .downstream import (
    GeneratedContentItem,
    ProductRecord,
    TopicRecord,
    ensure_persona_for_topics,
    load_products,
    generate_content_items,
)
from .dreamina_cli import (
    image2image as _dreamina_image2image,
    text2image as _dreamina_text2image,
    text2video as _dreamina_text2video,
)
from .product_assets import ensure_product_gallery_for_brand, cleanup_empty_columns_for_products
from .utils import get_number_field, get_text_field, now_ts_ms

logger = logging.getLogger("bcma.brand_pipeline")


@dataclass
class BrandTopic:
    topic: TopicRecord
    total_score: float
    created_at_ms: int


def _load_recent_topics_for_brand(
    cfg: Config,
    brand: str,
    lookback_hours: int = 48,
) -> List[BrandTopic]:
    """从 TopicSelection 中按时间与品牌筛选话题，并按总分降序排序。"""

    brand = brand.strip()
    app_token = cfg.app_token
    tbl_id = cfg.tables["topic_selection"]["table_id"]
    f_cfg = cfg.fields["topic_selection"]

    records = search_all_records(app_token, tbl_id, view_id=None, automatic_fields=False, page_size=200)
    if not records:
        return []

    now_ms = now_ts_ms()
    window_ms = max(1, int(lookback_hours)) * 3600 * 1000
    lower_bound = now_ms - window_ms

    items: List[BrandTopic] = []

    for item in records:
        rid = item.get("record_id")
        if not rid:
            continue
        fields = item.get("fields") or {}

        # 注意: 默认名对齐 topic_selection.created_at = "入库时间"
        created_ms = int(get_number_field(fields, f_cfg.get("created_at", "入库时间"), default=0.0))
        if created_ms and created_ms < lower_bound:
            continue

        topic_text = get_text_field(fields, f_cfg["topic"], "")
        if not topic_text:
            continue

        brand_field_name = f_cfg.get("brand")
        if brand_field_name:
            brand_value = get_text_field(fields, brand_field_name, "")
            # 精确等值匹配（忽略首尾空格/大小写）；子串匹配会误召"双汇肠" vs "双汇"
            if brand_value and brand_value.strip().lower() != brand.strip().lower():
                continue

        persona = get_text_field(fields, f_cfg["audience"], "") or None
        raw_text = get_text_field(fields, f_cfg["raw_text"], "")
        total_score = float(get_number_field(fields, f_cfg["total_score"], default=0.0))

        topic = TopicRecord(
            record_id=rid,
            topic=topic_text,
            persona=persona,
            raw_text=raw_text,
            fields=fields,
        )
        items.append(
            BrandTopic(
                topic=topic,
                total_score=total_score,
                created_at_ms=created_ms or now_ms,
            )
        )

    items.sort(key=lambda x: (x.total_score, x.created_at_ms), reverse=True)
    return items


def _build_cover_prompt(brand: str, item: GeneratedContentItem) -> str:
    product_names = [p.name for p in item.products]
    topic_title = item.topic.topic
    persona = item.persona or "核心消费人群"
    visual = item.copywriting.get("visual") or ""
    products_str = " / ".join(product_names) if product_names else "核心主推产品"

    return (
        f"为品牌「{brand}」生成一张真实质感的短视频封面/主图，适用于小红书/抖音等竖版平台。"
        f"主打产品：{products_str}，话题：「{topic_title}」，核心人群：{persona}。"
        f"画面风格参考：{visual}。要求画面清晰、构图简洁，突出人物与产品细节，9:16 竖版。"
    )


def _build_douyin_cover_prompt(
    brand: str,
    topic_title: str,
    hook: str,
    visual_direction: str,
) -> str:
    """构造抖音 9:16 首帧封面 Prompt（v5.6.0）。

    抖音端封面的核心是信息流首帧的大字钩子：`hook` 为 10-20 字的冲突/好奇钩子，
    版式要求大字居中、色彩强对比、避开被操作栏遮挡的安全区。
    """

    brand = (brand or "").strip()
    topic_title = (topic_title or "").strip()
    hook = (hook or "").strip()
    visual_direction = (visual_direction or "").strip()

    parts: List[str] = []
    parts.append(
        "以提供的真实产品实拍图为唯一底图，为抖音短视频信息流首帧设计一张 9:16 竖版封面海报。"
    )
    if brand:
        parts.append(f"品牌为「{brand}」。")
    if topic_title:
        parts.append(f"关联话题为「{topic_title}」。")

    parts.append(
        "必须严格保留原图中的服装款式、版型结构、面料细节和品牌 Logo，"
        "不得凭空生成新衣服、不得改变衣服轮廓或 Logo 形态，只允许调整背景、光影、色调、构图和文字排版。"
    )
    parts.append(
        f"用中文大字展示首帧钩子：「{hook}」，大标题占画面上 1/3 黄金区，"
        "字重厚实、字号醒目、避开抖音信息流进度条/头像/操作栏等安全区域，静音也能 1 秒读懂。"
    )
    if visual_direction:
        parts.append(f"整体视觉基调参考：{visual_direction}。")
    parts.append(
        "设计风格：构图简洁有冲击力、色彩强对比、信息层级清晰，避免过度花哨或动漫化，"
        "保留真实城市生活质感，适配抖音信息流的快速扫读节奏。"
    )
    parts.append(
        "输出为 9:16 竖版构图，2K 分辨率 PNG 海报。"
    )

    return " ".join(parts)


def _build_xhs_cover_prompt(
    brand: str,
    topic_title: str,
    xhs_title: str,
    visual_direction: str,
) -> str:
    """构造小红书 3:4 封面 Prompt（v5.6.0）。

    小红书端封面的核心是九宫格缩略图扫读 + 博主审美：`xhs_title` 为 20 字以内带
    emoji 的标题，版式要求真实生活质感、自然光、暖色调、人物主体占比适中、避免
    过度滤镜。
    """

    brand = (brand or "").strip()
    topic_title = (topic_title or "").strip()
    xhs_title = (xhs_title or "").strip()
    visual_direction = (visual_direction or "").strip()

    parts: List[str] = []
    parts.append(
        "以提供的真实产品实拍图为唯一底图，为小红书种草笔记设计一张 3:4 竖版封面。"
    )
    if brand:
        parts.append(f"品牌为「{brand}」。")
    if topic_title:
        parts.append(f"关联话题为「{topic_title}」。")

    parts.append(
        "必须严格保留原图中的服装款式、版型结构、面料细节和品牌 Logo，"
        "不得凭空生成新衣服、不得改变衣服轮廓或 Logo 形态，只允许调整背景、光影、色调、构图和文字排版。"
    )
    parts.append(
        f"用中文展示小红书标题：「{xhs_title}」，保留标题中的 emoji；"
        "字体清晰有质感、风格贴近小红书博主审美，避免艺术字或过度花哨的字效。"
    )
    if visual_direction:
        parts.append(f"构图建议：{visual_direction}。")
    parts.append(
        "设计风格：自然光、暖色调、真实生活质感，人物主体占比适中（约 50-65%），"
        "背景留呼吸感、不要过度滤镜或动漫化处理，适配小红书九宫格缩略图的扫读。"
    )
    parts.append(
        "输出为 3:4 竖版构图，2K 分辨率 PNG 海报。"
    )

    return " ".join(parts)


def _extract_camera_directions_from_script(script_text: str) -> List[str]:
    """从脚本文本中提取运镜指令列表。

    基于常见关键词（如「特写」「拉近」「切换」「走路」等）进行启发式解析，
    仅做运镜层面的结构化，不改写脚本内容本身。
    """
    directions: List[str] = []
    if not script_text:
        return directions

    def add_unique(text: str) -> None:
        if text not in directions:
            directions.append(text)

    for line in script_text.splitlines():
        line = line.strip()
        if not line:
            continue

        if "特写" in line or "拉近" in line or "推近" in line:
            add_unique("推近/特写：镜头贴近人物或产品细节，突出质感与情绪。")

        if "走路" in line or "走在" in line or "走向" in line:
            add_unique("跟拍：镜头跟随人物走路或移动，保持平稳运动。")

        if "切换" in line or "转场" in line or "对比" in line:
            add_unique("转场/交叉剪辑：在不同场景间进行切换，强化前后场景对比。")

        if any(k in line for k in ["细节", "衣料", "毛领", "拉链"]):
            add_unique("细节微距：用近景/微距呈现衣料、毛领、拉链等关键细节。")

        if "字幕" in line:
            add_unique("字幕叠加：在关键画面上叠加简洁文案，卡点节奏。")

    return directions


def _build_video_prompts(item: GeneratedContentItem, brand: str) -> tuple[str, str]:
    """基于正文与脚本构造极梦视频生成 Prompt 与脚本文本。

    Prompt 结构严格为：

    【脚本】<原始文本>

    【运镜指令（由脚本解析）】
    - 跟拍/推近...
    """
    script_text = (item.copywriting.get("body") or "").strip()
    camera_directions = _extract_camera_directions_from_script(script_text)

    if not camera_directions:
        camera_directions = ["平稳推进：缓慢前移或轻微摇镜，保持生活化的真实感。"]

    camera_lines = "\n".join(f"- {d}" for d in camera_directions)

    prompt = f"【脚本】\n{script_text}\n\n【运镜指令（由脚本解析）】\n{camera_lines}"
    prompt += (
        f"\n\n重点约束：实施「品牌Logo精准保护」，请准确还原【{brand}】的官方品牌 Logo（如金属剪刀Logo/经典标识等），"
        "Logo 必须清晰、标准、比例正确、无变形、无乱码错字，切勿发生 AI 幻觉。"
    )
    return prompt, script_text


def _choose_cover_tokens_from_product_gallery(
    cfg: Config,
    item: GeneratedContentItem,
) -> Optional[List[Dict[str, str]]]:
    """从主推产品的「产品图库(真实大片)」附件中选择一张图作为封面来源。

    优先使用第一个商品，如其图库为空则尝试下一个。
    """

    f_products = cfg.fields.get("products", {})
    gallery_field = f_products.get("asset_gallery_field")
    if not gallery_field:
        return None

    for product in item.products:
        fields = getattr(product, "fields", {}) or {}
        attachments = fields.get(gallery_field) or []
        if not isinstance(attachments, list) or not attachments:
            continue

        for att in attachments:
            token = (
                att.get("file_token")
                or att.get("token")
                or att.get("fileToken")
            )
            if token:
                return [{"file_token": str(token)}]

    return None


def _generate_dual_covers_for_item(
    cfg: Config,
    brand: str,
    item: GeneratedContentItem,
    f_cm: Dict[str, Any],
) -> Dict[str, Any]:
    """为一条 ContentMatrix 记录分别生成抖音 9:16 + 小红书 3:4 双封面（v5.6.0）。

    Returns:
        {
            "douyin": file_token or None,
            "xhs":    file_token or None,
            "skip_reasons": ["reason1", ...]  # 跳过原因列表
        }
    """

    result: Dict[str, Any] = {"douyin": None, "xhs": None, "skip_reasons": []}

    # 1) 尝试从产品图库取一张真实图片作为底图（image2image 质量更稳）；
    #    若图库为空 / 附件解析失败，base_image_path 保持 None，下游自动退回 text2image。
    base_image_path: Optional[str] = None
    cover_tokens = _choose_cover_tokens_from_product_gallery(cfg, item)
    base_file_token: Optional[str] = None
    if cover_tokens:
        first = cover_tokens[0] if isinstance(cover_tokens, list) and cover_tokens else None
        if isinstance(first, dict):
            base_file_token = (
                first.get("file_token")
                or first.get("token")
                or first.get("fileToken")
            )

    if base_file_token:
        try:
            base_image_path = download_attachment_file(cfg.app_token, str(base_file_token))
        except Exception as e:
            logger.warning("下载产品底图失败，退回 text2image: %s", e)
            base_image_path = None
            result["skip_reasons"].append(f"下载产品底图失败（已退回 text2image）: {e}")
    else:
        result["skip_reasons"].append("产品图库为空，使用 text2image 直出")

    # 3) 从 copywriting dict 中读取双平台所需的文案字段
    copy = item.copywriting or {}
    hook_text = str(copy.get("douyin_hook") or copy.get("hook", "") or "").strip()
    douyin_visual = str(copy.get("douyin_visual") or copy.get("visual", "") or "").strip()
    xhs_title = str(copy.get("xhs_title", "") or "").strip()
    xhs_visual = str(copy.get("xhs_visual") or copy.get("visual", "") or "").strip()

    # 4) 抖音 9:16 封面（dreamina image2image，自动兜底 text2image）
    if hook_text:
        douyin_prompt = _build_douyin_cover_prompt(
            brand=brand,
            topic_title=item.topic.topic,
            hook=hook_text,
            visual_direction=douyin_visual,
        )
        douyin_image_path = _dreamina_image2image(base_image_path, douyin_prompt, ratio="9:16")
        if douyin_image_path:
            try:
                result["douyin"] = upload_attachment_file(
                    cfg.app_token, douyin_image_path, file_type="bitable_image"
                )
            except Exception as e:
                logger.warning("抖音 9:16 封面上传失败: %s", e)
                result["skip_reasons"].append(f"抖音封面上传失败: {e}")
        else:
            result["skip_reasons"].append("dreamina 生成抖音封面返回空（CLI 不可用或生成超时）")
    else:
        result["skip_reasons"].append("缺少 hook 文案，跳过抖音封面")

    # 5) 小红书 3:4 封面（dreamina image2image，自动兜底 text2image）
    if xhs_title:
        xhs_prompt = _build_xhs_cover_prompt(
            brand=brand,
            topic_title=item.topic.topic,
            xhs_title=xhs_title,
            visual_direction=xhs_visual,
        )
        xhs_image_path = _dreamina_image2image(base_image_path, xhs_prompt, ratio="3:4")
        if xhs_image_path:
            try:
                result["xhs"] = upload_attachment_file(
                    cfg.app_token, xhs_image_path, file_type="bitable_image"
                )
            except Exception as e:
                logger.warning("小红书 3:4 封面上传失败: %s", e)
                result["skip_reasons"].append(f"小红书封面上传失败: {e}")
        else:
            result["skip_reasons"].append("dreamina 生成小红书封面返回空（CLI 不可用或生成超时）")
    else:
        result["skip_reasons"].append("缺少 xhs_title 文案，跳过小红书封面")

    return result


def _attach_cover_and_video(
    cfg: Config,
    brand: str,
    items: List[GeneratedContentItem],
    max_videos: int = 3,
) -> Dict[str, Any]:
    """为 Top K 内容生成双平台封面与 9:16 视频并写回多维表（v5.7.0）。

    流程：
    1. 对每条记录，基于同一张产品图库底图生成两张封面（dreamina image2image）：
       - 抖音 9:16 首帧封面 → 写入 `douyin_cover`（同时回填旧 `cover_image_ai_field`）；
       - 小红书 3:4 封面 → 写入 `xhs_cover`。
       **封面为所有 Top K 内容均生成。**
    2. 调用 dreamina text2video 生成 9:16 AI 视频，两个平台共用。
       **视频每次执行最多生成 max_videos（默认 3）条，节省积分。**
    3. 任一封面或视频失败都只记日志、跳过该字段，不影响其他平台与后续记录。

    v5.9.0: 返回值新增 skip_reasons 列表，透传每条记录跳过封面/视频的原因。
    """

    if not items:
        return {
            "cover_douyin_ai_success": 0,
            "cover_xhs_ai_success": 0,
            "cover_ai_success": 0,
            "video_success": 0,
            "skip_reasons": [],
        }

    # 前置检查 dreamina CLI
    dreamina_ok = _dreamina_available()
    all_skip_reasons: List[str] = []
    if not dreamina_ok:
        all_skip_reasons.append("dreamina CLI 不可用（未安装或未登录），跳过全部封面和视频生成")

    app_token = cfg.app_token
    cm_tbl = cfg.tables["content_matrix"]["table_id"]
    f_cm = cfg.fields["content_matrix"]

    douyin_cover_field = f_cm.get("douyin_cover")
    xhs_cover_field = f_cm.get("xhs_cover")
    cover_field_ai_legacy = f_cm.get("cover_image_ai_field")  # v5.2.0 旧字段，回填抖音 9:16
    video_field = f_cm.get("video_asset_ai")

    # 确保附件字段存在
    for fname in [douyin_cover_field, xhs_cover_field, cover_field_ai_legacy, video_field]:
        if fname:
            try:
                ensure_field_exists(
                    app_token,
                    cm_tbl,
                    fname,
                    type_code=17,
                    ui_type="Attachment",
                    property_obj=None,
                )
            except Exception as e:
                logger.warning("创建附件字段 '%s' 失败: %s", fname, e)

    cover_douyin_ai_success = 0
    cover_xhs_ai_success = 0
    video_success = 0

    if not dreamina_ok:
        return {
            "cover_douyin_ai_success": 0,
            "cover_xhs_ai_success": 0,
            "cover_ai_success": 0,
            "video_success": 0,
            "skip_reasons": all_skip_reasons,
        }

    for idx, item in enumerate(items):
        record = item.record
        record_id = record.get("record_id") if isinstance(record, dict) else None
        if not record_id:
            continue

        topic_label = item.topic.topic[:20]

        # 1) 双平台 AI 封面（所有 Top K 都生成）
        try:
            ai_tokens = _generate_dual_covers_for_item(cfg, brand, item, f_cm)
        except Exception as e:
            logger.warning("生成双平台 AI 封面失败 record_id=%s: %s", record_id, e)
            ai_tokens = {"douyin": None, "xhs": None, "skip_reasons": [f"异常: {e}"]}

        # 收集跳过原因
        item_reasons = ai_tokens.get("skip_reasons") or []
        for reason in item_reasons:
            all_skip_reasons.append(f"[{topic_label}] 封面: {reason}")

        douyin_token = ai_tokens.get("douyin")
        xhs_token = ai_tokens.get("xhs")

        payload: Dict[str, Any] = {}
        if douyin_token:
            if douyin_cover_field:
                payload[douyin_cover_field] = [{"file_token": douyin_token}]
            if cover_field_ai_legacy:
                payload[cover_field_ai_legacy] = [{"file_token": douyin_token}]
        if xhs_token and xhs_cover_field:
            payload[xhs_cover_field] = [{"file_token": xhs_token}]

        if payload:
            try:
                update_single(app_token, cm_tbl, record_id, payload)
                if douyin_token:
                    cover_douyin_ai_success += 1
                if xhs_token:
                    cover_xhs_ai_success += 1
            except Exception as e:
                logger.warning("写入双平台 AI 封面失败 record_id=%s: %s", record_id, e)
                all_skip_reasons.append(f"[{topic_label}] 封面写入飞书失败: {e}")

        # 2) 视频（dreamina text2video，最多生成 max_videos 条）
        if video_field and video_success < max_videos:
            try:
                video_prompt, script_text = _build_video_prompts(item, brand=brand)
                video_path = _dreamina_text2video(video_prompt, duration=5, poll_seconds=180)
                if video_path:
                    file_token = upload_attachment_file(app_token, video_path, file_type="bitable_file")
                    update_single(app_token, cm_tbl, record_id, {video_field: [{"file_token": file_token}]})
                    video_success += 1
                else:
                    all_skip_reasons.append(f"[{topic_label}] 视频: dreamina text2video 返回空")
            except Exception as e:
                logger.warning("视频生成/上传失败 record_id=%s: %s", record_id, e)
                all_skip_reasons.append(f"[{topic_label}] 视频: {e}")
        elif video_field and video_success >= max_videos:
            logger.info("已达视频生成上限 (%d)，跳过 record_id=%s", max_videos, record_id)

    return {
        "cover_douyin_ai_success": cover_douyin_ai_success,
        "cover_xhs_ai_success": cover_xhs_ai_success,
        # v5.5.0 旧 key：两平台任一成功都算，便于兼容旧看板
        "cover_ai_success": cover_douyin_ai_success + cover_xhs_ai_success,
        "video_success": video_success,
        "skip_reasons": all_skip_reasons,
    }


def run_brand_content_pipeline(
    cfg: Config,
    brand: str,
) -> Dict[str, Any]:
    """第六步入口：基于品牌名的"文案矩阵 + Top K 视觉资产"生产。

    前置条件：第五步 `select_topic` 已经把当日 Top K 话题写入 `TopicSelection`
    并打上了该品牌的"适用品牌"字段。

    本步骤流程：
    1. 清理 Products 表空列 / 为该品牌产品补全图库
    2. 从 `TopicSelection` 中按品牌 + 时间窗口加载话题
    3. 匹配主推产品、生成文案（含爆款逻辑拆解），写入 `ContentMatrix`
    4. 按总分取 Top K 做封面 / 视频附件补全

    返回结构化摘要，包含：
    - 参与评估的话题数量；
    - 新生成的 ContentMatrix 记录数；
    - Top K 视觉封面/视频的实际成功上传数量；
    - 为该品牌补全产品图库的记录数量与 record_id 列表；
    - 本次运行中在 Products 表被自动清理的"全空列"字段名列表。
    """

    brand = brand.strip()
    if not brand:
        raise ValueError("brand 不能为空")

    # 默认 240h (10 天) 对齐 daily_topics.lookback_days，避免与 Step 5 窗口错位
    lookback_hours = int(cfg.downstream.get("brand_topic_lookback_hours", 240)) or 240
    top_k = int(cfg.downstream.get("brand_top_k_assets", 5)) or 5

    # 0) 先清理 Products 表中“所有记录均为空”的非核心字段
    products_fields_cfg = cfg.fields.get("products", {}) or {}
    non_removable_fields = products_fields_cfg.get("non_removable_fields")
    try:
        cleanup_stats = cleanup_empty_columns_for_products(
            app_token=cfg.app_token,
            tables_cfg=cfg.tables,
            fields_cfg=cfg.fields,
            non_removable_fields=non_removable_fields,
        )
    except Exception as e:
        logger.warning("Products 空列清理失败: %s", e)
        cleanup_stats = {"deleted_fields": []}

    deleted_fields = (
        cleanup_stats.get("deleted_fields", []) if isinstance(cleanup_stats, dict) else []
    )
    deleted_count = len(deleted_fields)

    # 1) 在生成内容前，为该品牌的主推产品补全「产品图库(真实大片)」附件
    try:
        gallery_stats = ensure_product_gallery_for_brand(
            brand=brand,
            app_token=cfg.app_token,
            tables_cfg=cfg.tables,
            fields_cfg=cfg.fields,
        )
    except Exception as e:
        logger.warning("产品图库补全失败 brand='%s': %s", brand, e)
        gallery_stats = {
            "brand": brand,
            "products_scanned": 0,
            "products_with_existing_gallery": 0,
            "products_updated": 0,
            "updated_record_ids": [],
        }

    gallery_updated_count = (
        int(gallery_stats.get("products_updated", 0)) if isinstance(gallery_stats, dict) else 0
    )
    gallery_updated_ids = (
        gallery_stats.get("updated_record_ids", []) if isinstance(gallery_stats, dict) else []
    )

    # 2) 从 TopicSelection 中加载近期指定品牌话题
    brand_topics = _load_recent_topics_for_brand(cfg, brand=brand, lookback_hours=lookback_hours)
    if not brand_topics:
        return {
            "brand": brand,
            "topic_count": 0,
            "content_created_count": 0,
            "asset_top_k": top_k,
            "asset_cover_uploaded": 0,
            "asset_video_uploaded": 0,
            "created_record_ids": [],
            "top_record_ids": [],
            "product_gallery_updated_count": gallery_updated_count,
            "product_gallery_updated_ids": gallery_updated_ids,
            "product_empty_columns_deleted_count": deleted_count,
            "product_empty_columns_deleted": deleted_fields,
        }

    topics: List[TopicRecord] = [bt.topic for bt in brand_topics]

    # 3) 补全人群标签
    ensure_persona_for_topics(cfg, topics)

    # 4) 读取产品池
    products: List[ProductRecord] = load_products(cfg)
    if not products:
        return {
            "brand": brand,
            "topic_count": len(topics),
            "content_created_count": 0,
            "asset_top_k": top_k,
            "asset_cover_uploaded": 0,
            "asset_video_uploaded": 0,
            "created_record_ids": [],
            "top_record_ids": [],
            "product_gallery_updated_count": gallery_updated_count,
            "product_gallery_updated_ids": gallery_updated_ids,
            "product_empty_columns_deleted_count": deleted_count,
            "product_empty_columns_deleted": deleted_fields,
        }

    # 5) 生成文案并写入 ContentMatrix（v5.6.0：显式传 brand 以驱动双平台 LLM 文案）
    items: List[GeneratedContentItem] = generate_content_items(
        cfg, topics, products, brand=brand
    )
    if not items:
        return {
            "brand": brand,
            "topic_count": len(topics),
            "content_created_count": 0,
            "asset_top_k": top_k,
            "asset_cover_douyin_uploaded": 0,
            "asset_cover_xhs_uploaded": 0,
            "asset_video_uploaded": 0,
            "created_record_ids": [],
            "top_record_ids": [],
            "product_gallery_updated_count": gallery_updated_count,
            "product_gallery_updated_ids": gallery_updated_ids,
            "product_empty_columns_deleted_count": deleted_count,
            "product_empty_columns_deleted": deleted_fields,
        }

    score_map = {bt.topic.record_id: bt.total_score for bt in brand_topics}
    items_sorted = sorted(
        items,
        key=lambda it: score_map.get(it.topic.record_id, 0.0),
        reverse=True,
    )
    top_items = items_sorted[:top_k]

    stats = _attach_cover_and_video(cfg, brand, top_items)

    created_record_ids = [it.record.get("record_id") for it in items if isinstance(it.record, dict)]
    top_record_ids = [it.record.get("record_id") for it in top_items if isinstance(it.record, dict)]

    asset_skip_reasons = stats.get("skip_reasons") or []

    return {
        "brand": brand,
        "topic_count": len(topics),
        "content_created_count": len(items),
        "asset_top_k": top_k,
        # v5.6.0 双平台分项统计
        "asset_cover_douyin_uploaded": int(stats.get("cover_douyin_ai_success", 0)),
        "asset_cover_xhs_uploaded": int(stats.get("cover_xhs_ai_success", 0)),
        # v5.5.0 兼容 key：两平台之和
        "asset_cover_ai_uploaded": int(stats.get("cover_ai_success", 0)),
        "asset_video_uploaded": int(stats.get("video_success", 0)),
        # v5.9.0: 跳过原因透传，方便排查封面/视频未生成的根因
        "asset_skip_reasons": asset_skip_reasons,
        "created_record_ids": [rid for rid in created_record_ids if rid],
        "top_record_ids": [rid for rid in top_record_ids if rid],
        "product_gallery_updated_count": gallery_updated_count,
        "product_gallery_updated_ids": gallery_updated_ids,
        "product_empty_columns_deleted_count": deleted_count,
        "product_empty_columns_deleted": deleted_fields,
    }
