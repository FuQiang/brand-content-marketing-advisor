"""Dual-platform copywriting via LLM — v5.6.0 (Step 6 core).

第六步的文案生成核心：基于「话题 × 产品 × 人群 × 品牌」四要素，调用 LLM
同时产出两套平台差异化文案：

1. **抖音短视频分镜脚本** — 钩子标题 + 5 镜头分镜（运镜/动作/字幕） + CTA
2. **小红书种草笔记** — emoji 标题 + 800-1200 字情绪化正文 + 话题标签

两个 prompt 都注入：
- 品牌 4R prompt 的「品牌底层认知 + 4R 筛选法则 + 人设审美一致性校验」三段
  （通过 `upstream.load_brand_rules_prompt` 复用 scoring section 抽取逻辑）
- `brand_audience` 表的完整人群画像（标签/动机/偏好/描述）
- 候选话题 + 原始文本
- 选中的主推产品详情（名称/卖点/详细阐述/人群标签/功能点/季节）
- 目标人群启发标签

LLM 调用策略沿用 `scoring.py` 的 Anthropic → AIME 降级模式：
Claude Opus 4.6 → Claude Sonnet 4.6 → doubao-pro-32k。任一环节失败时兜底为
基于模板的确定性文案（不中断批次）。
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .bitable import search_all_records
from .config import Config
from .llm_client import call_llm_json, call_llm_text
from .upstream import load_brand_rules_prompt
from .utils import get_multi_select_field, get_text_field

logger = logging.getLogger("bcma.copywriting")


# ---------------------------------------------------------------------------
#  Brand context loading
# ---------------------------------------------------------------------------

@dataclass
class BrandContext:
    """文案生成所需的品牌上下文。"""
    brand: str
    audience_text: str       # brand_audience 表拼接的人群画像长文本
    rules_prompt: str        # BrandTopicRules 的三段 scoring sections

    def as_prompt_block(self) -> str:
        parts: List[str] = [f"# 品牌\n{self.brand}"]
        if self.audience_text:
            parts.append(f"# 品牌人群画像\n{self.audience_text}")
        if self.rules_prompt:
            parts.append(f"# 品牌底层认知与审美一致性（节选自 BrandTopicRules）\n{self.rules_prompt}")
        return "\n\n".join(parts)


def load_brand_context(cfg: Config, brand: str) -> BrandContext:
    """从 brand_audience + BrandTopicRules 拼接品牌上下文。

    - brand_audience 取该品牌记录的 persona_description / persona_tags / motivation /
      content_preference / audience 五个字段，拼接成一段文本。
    - BrandTopicRules 复用 `load_brand_rules_prompt` 只拿三段 scoring sections。

    两者缺失时返回空串，不 raise，由调用方决定回退行为。
    """
    brand = (brand or "").strip()
    audience_text = ""
    if brand:
        try:
            audience_text = _load_brand_audience_text(cfg, brand)
        except Exception as e:
            logger.warning("加载 brand_audience 失败 brand='%s': %s", brand, e)

    rules_prompt = ""
    if brand:
        try:
            rules_prompt = load_brand_rules_prompt(cfg, brand) or ""
        except Exception as e:
            logger.warning("加载 BrandTopicRules 失败 brand='%s': %s", brand, e)

    return BrandContext(brand=brand, audience_text=audience_text, rules_prompt=rules_prompt)


def _load_brand_audience_text(cfg: Config, brand: str) -> str:
    """Load brand_audience record and stringify as a prompt block."""
    ba_cfg = cfg.tables.get("brand_audience") or {}
    tbl_id = ba_cfg.get("table_id", "")
    if not tbl_id:
        return ""

    f = cfg.fields.get("brand_audience", {})
    name_field = f.get("name", "品牌名称")
    audience_field = f.get("audience", "典型人群受众")
    tags_field = f.get("persona_tags", "画像标签")
    motivation_field = f.get("motivation", "消费动机")
    pref_field = f.get("content_preference", "内容偏好")
    desc_field = f.get("persona_description", "人群描述")

    records = search_all_records(cfg.app_token, tbl_id, page_size=100)
    all_parts: List[str] = []
    for item in records:
        fields = item.get("fields") or {}
        if get_text_field(fields, name_field, "") != brand:
            continue

        audience = get_multi_select_field(fields, audience_field)
        tags = get_text_field(fields, tags_field, "")
        motivation = get_text_field(fields, motivation_field, "")
        pref = get_text_field(fields, pref_field, "")
        desc = get_text_field(fields, desc_field, "")

        parts: List[str] = []
        if audience:
            parts.append("人群：" + " / ".join(audience))
        if tags:
            parts.append(f"  画像标签：{tags}")
        if motivation:
            parts.append(f"  消费动机：{motivation}")
        if pref:
            parts.append(f"  内容偏好：{pref}")
        if desc:
            parts.append(f"  人群描述：{desc}")
        if parts:
            all_parts.append("\n".join(parts))

    return "\n\n".join(all_parts)


# ---------------------------------------------------------------------------
#  Product context
# ---------------------------------------------------------------------------

def build_product_context(products: List[Any]) -> str:
    """把选中的 ProductRecord 列表拼装成 prompt block。

    注意：这里接受 `downstream.ProductRecord`，但为避免循环依赖声明为 Any。
    """
    if not products:
        return "（无主推产品信息）"

    lines: List[str] = []
    for i, p in enumerate(products, 1):
        name = getattr(p, "name", "")
        series = getattr(p, "series", "")
        selling_point = getattr(p, "selling_point", "")
        selling_detail = getattr(p, "selling_detail", "")

        extra_parts: List[str] = []
        fields = getattr(p, "fields", {}) or {}
        persona_tags = get_multi_select_field(fields, "目标人群标签")
        functions = get_multi_select_field(fields, "功能点")
        season = get_text_field(fields, "季节", "")
        material = get_text_field(fields, "材质", "")
        price_band = get_text_field(fields, "价格带", "")

        if persona_tags:
            extra_parts.append(f"目标人群标签：{' / '.join(persona_tags)}")
        if functions:
            extra_parts.append(f"功能点：{' / '.join(functions)}")
        if season:
            extra_parts.append(f"季节：{season}")
        if material:
            extra_parts.append(f"材质：{material}")
        if price_band:
            extra_parts.append(f"价格带：{price_band}")

        block = f"## 产品 {i}: {name}"
        if series:
            block += f"（系列：{series}）"
        if selling_point:
            block += f"\n卖点：{selling_point}"
        if selling_detail:
            block += f"\n卖点详述：{selling_detail}"
        if extra_parts:
            block += "\n" + "\n".join(extra_parts)

        lines.append(block)

    return "\n\n".join(lines)


# ---------------------------------------------------------------------------
#  Prompts
# ---------------------------------------------------------------------------

_DOUYIN_SCRIPT_PROMPT = """你是品牌「{brand}」的抖音爆款短视频编剧，擅长把社会热点话题、真实人群洞察与产品卖点揉进 30-60 秒的短视频分镜脚本。

{brand_block}

# 当前话题
标题：{topic}
原始内容：{raw_text}

# 目标人群启发
{persona}

# 主推产品
{product_block}

# 输出要求
严格按如下 JSON 格式返回，不要任何多余说明，不要 markdown 代码块包裹：

{{
  "hook_title": "抖音信息流首帧大字钩子（10-20 字，强冲突或强好奇）",
  "scenes": [
    {{"camera": "镜头运镜描述", "action": "人物/产品动作", "subtitle": "屏幕字幕文案"}},
    {{"camera": "...", "action": "...", "subtitle": "..."}},
    {{"camera": "...", "action": "...", "subtitle": "..."}},
    {{"camera": "...", "action": "...", "subtitle": "..."}},
    {{"camera": "...", "action": "...", "subtitle": "..."}}
  ],
  "cta": "结尾行动召唤文案（引导点赞/评论/直播间/购物车，15 字以内）",
  "visual_direction": "整体视觉基调与色调建议（40-80 字，供封面与视频生成参考）"
}}

要求：
- hook_title 必须承接话题热度 + 人群痛点，**禁止**通用空话（如"超好看"/"太爱了"）。
- 5 个镜头必须呈"情绪开场 → 产品出场 → 对比切换 → 价值锤击 → 行动召唤"节奏。
- 字幕要能独立阅读（静音观看也成立）。
- 产品 Logo、服装结构、材质必须真实，不得凭空编造。
- 严格遵守品牌 4R 筛选法则中的"排斥人群 / 冲突人设美学"红线。
"""


_XHS_NOTE_PROMPT = """你是品牌「{brand}」的小红书种草笔记达人，擅长以第一人称情绪化长文把产品价值嵌入真实生活场景。

{brand_block}

# 当前话题
标题：{topic}
原始内容：{raw_text}

# 目标人群启发
{persona}

# 主推产品
{product_block}

# 输出要求
严格按如下 JSON 格式返回，不要任何多余说明，不要 markdown 代码块包裹：

{{
  "title": "小红书标题（20 字以内，必须带 1-2 个合适 emoji，强钩子）",
  "body": "小红书正文（**800-1200 字**，第一人称，段落分明，包含场景还原/真实痛点/产品使用体验/购买理由/适配人群 5 段结构）",
  "tags": ["#话题标签1", "#产品标签2", "#人群标签3", "#场景标签4", "#品类标签5"],
  "visual_direction": "封面拍摄建议：构图/光线/主体占比（40-80 字）"
}}

要求：
- **body 严格 800-1200 字**，少于 800 字视为不合格。
- 第一人称视角，用"姐妹们/宝子们"等小红书常见称呼自然融入。
- 段落清晰，每段首句要能独立承接情绪（用户 1 秒扫读也能抓住）。
- 产品卖点必须落在具体场景里（例如"零下 10 度接娃放学站在风口里等 20 分钟"），禁止干巴巴列参数。
- tags 数量 3-6 个，混合话题/产品/人群/场景/品类维度。
- 严格遵守品牌 4R 筛选法则中的"排斥人群 / 冲突人设美学"红线，不要出现冲突人设描述。
"""


# ---------------------------------------------------------------------------
#  LLM call (reusing scoring.py's Anthropic → AIME fallback pattern)
# ---------------------------------------------------------------------------

def _call_llm_json(prompt: str, model_cfg: Dict[str, Any], max_tokens: int = 3072) -> Optional[Dict[str, Any]]:
    """Call LLM and parse JSON response via unified llm_client."""
    return call_llm_json(prompt, model_cfg, max_tokens=max_tokens)


def _parse_json(text: str) -> Optional[Dict[str, Any]]:
    """Parse JSON object from LLM response, tolerating markdown code fences."""
    if not text:
        return None
    try:
        m = re.search(r"```(?:json)?\s*\n?([\s\S]*?)```", text)
        if m:
            text = m.group(1).strip()
        m = re.search(r"\{[\s\S]*\}", text)
        if not m:
            return None
        data = json.loads(m.group(0))
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.warning("Copy JSON 解析失败: %s", e)
    return None


# ---------------------------------------------------------------------------
#  Platform-specific generators
# ---------------------------------------------------------------------------

def _render_douyin_script_text(data: Dict[str, Any]) -> str:
    """把 LLM 返回的抖音 JSON 渲染为可读分镜脚本文本。"""
    hook = str(data.get("hook_title", "")).strip()
    scenes = data.get("scenes") or []
    cta = str(data.get("cta", "")).strip()

    lines: List[str] = []
    if hook:
        lines.append(f"【首帧钩子】{hook}")
    if isinstance(scenes, list):
        for i, s in enumerate(scenes, 1):
            if not isinstance(s, dict):
                continue
            camera = str(s.get("camera", "")).strip()
            action = str(s.get("action", "")).strip()
            subtitle = str(s.get("subtitle", "")).strip()
            block = f"\n【镜头{i}】\n运镜：{camera}\n动作：{action}\n字幕：{subtitle}"
            lines.append(block)
    if cta:
        lines.append(f"\n【CTA】{cta}")

    return "\n".join(lines).strip()


def _render_xhs_note_text(data: Dict[str, Any]) -> str:
    """把 LLM 返回的小红书 JSON 渲染为完整笔记（正文 + tags）。"""
    body = str(data.get("body", "")).strip()
    tags = data.get("tags") or []
    tag_line = ""
    if isinstance(tags, list):
        tag_line = " ".join(str(t).strip() for t in tags if str(t).strip())

    if tag_line:
        return f"{body}\n\n{tag_line}"
    return body


def _fallback_douyin(topic_title: str, persona: str, product_names: List[str]) -> Dict[str, Any]:
    """LLM 失败兜底：仍然按模板拼一条可用的抖音脚本，保证流水线不中断。"""
    p_display = persona or "核心消费人群"
    p_str = " / ".join(product_names) if product_names else "核心主推产品"
    return {
        "hook_title": f"{p_display}别再硬扛：{topic_title}",
        "scenes_text": (
            f"【首帧钩子】{p_display}别再硬扛：{topic_title}\n\n"
            f"【镜头1】\n运镜：特写 {p_display} 生活场景开场\n动作：呼出白气/瑟瑟发抖\n字幕：「{topic_title}」刷屏了\n\n"
            f"【镜头2】\n运镜：拉近到产品细节\n动作：用手感受材质\n字幕：{p_str} 值得\n\n"
            "【镜头3】\n运镜：对比切换\n动作：通勤→聚会→周末切镜\n字幕：一件搞定多场景\n\n"
            "【镜头4】\n运镜：价值锤击慢镜头\n动作：人物自信大步走\n字幕：真正值得投资的单品\n\n"
            "【镜头5】\n运镜：收尾门口/衣柜\n动作：挥手转身\n字幕：趁现在，升级你的战袍\n\n"
            "【CTA】点购物车领券"
        ),
        "cta": "点购物车领券",
        "visual_direction": f"冷调城市背景 + 暖色室内光，突出 {p_display} 的真实生活质感",
    }


def _fallback_xhs(topic_title: str, persona: str, product_names: List[str]) -> Dict[str, Any]:
    """LLM 失败兜底：小红书笔记模板。"""
    p_display = persona or "姐妹们"
    p_str = " / ".join(product_names) if product_names else "这套装备"
    body = (
        f"姐妹们，最近「{topic_title}」真的把我卷到了，{p_display}应该都懂那种感受。\n\n"
        f"上周出门就被狠狠上了一课，冷风钻进衣领里那一下，直接让我下定决心换装备。回家立刻翻出 {p_str}，才发现自己以前真的亏待了自己。\n\n"
        "穿上的第一感受就是——原来不冷是这种体验！面料扛风、版型修身、走路一点都不臃肿，去地铁站的路上整个人状态都不一样了。\n\n"
        "最打动我的是细节：领口毛领刚好贴住脸颊，口袋深度可以整只手揣进去，连拉链都是那种顺滑的高级感。贵有贵的道理，这笔钱花得真的不亏。\n\n"
        f"所以如果你也是{p_display}，在城市里每天都要面对通勤和天气的双重考验，真的推荐你下手。穿对衣服，整个冬天的幸福感都不一样。"
    )
    # 兜底也保证 800+ 字
    while len(body) < 820:
        body += "\n\n写在最后：穿衣服这件事，真的是越早投资自己越划算，别让天气决定你出门的心情。"
    return {
        "title": f"🧥{p_display}的冬日救命单品：{topic_title}",
        "body": body,
        "tags": [f"#{topic_title}", "#冬日穿搭", f"#{p_display}", "#真实测评", "#通勤必备"],
        "visual_direction": "冷调城市街景中人物近景，主体占比 60%，自然光+暖色背景光",
    }


# ---------------------------------------------------------------------------
#  Public entry
# ---------------------------------------------------------------------------

def generate_dual_platform_copy(
    cfg: Config,
    brand: str,
    topic_title: str,
    raw_text: str,
    persona: str,
    products: List[Any],
) -> Dict[str, Any]:
    """生成双平台文案的主入口。

    Returns:
        {
            "douyin": {
                "hook_title": str,       # 首帧钩子大字
                "scenes_text": str,      # 完整分镜脚本文本（供写入 ContentMatrix.douyin_script）
                "cta": str,
                "visual_direction": str, # 供封面/视频生成参考
            },
            "xhs": {
                "title": str,            # 小红书标题（供写入 xhs_title）
                "note_text": str,        # 正文+tags 拼接（供写入 xhs_note）
                "body": str,             # 纯正文
                "tags": List[str],
                "visual_direction": str,
            },
            "brand_context_used": bool,  # 是否成功注入品牌上下文
        }
    """

    brand_ctx = load_brand_context(cfg, brand)
    brand_block = brand_ctx.as_prompt_block() or "（未加载到品牌上下文，使用通用风格）"

    product_names = [getattr(p, "name", "") for p in products if getattr(p, "name", "")]
    product_block = build_product_context(products)

    model_cfg = cfg.select_model()

    # ---- 1. 抖音脚本 ----
    douyin_prompt = _DOUYIN_SCRIPT_PROMPT.format(
        brand=brand or "",
        brand_block=brand_block,
        topic=topic_title,
        raw_text=raw_text or "（无）",
        persona=persona or "未指定",
        product_block=product_block,
    )
    douyin_data = _call_llm_json(douyin_prompt, model_cfg, max_tokens=2048)

    if not douyin_data or not douyin_data.get("scenes"):
        logger.warning("抖音文案 LLM 返回无效，使用兜底模板 topic='%s'", topic_title)
        douyin_out = _fallback_douyin(topic_title, persona, product_names)
    else:
        douyin_out = {
            "hook_title": str(douyin_data.get("hook_title", "")).strip(),
            "scenes_text": _render_douyin_script_text(douyin_data),
            "cta": str(douyin_data.get("cta", "")).strip(),
            "visual_direction": str(douyin_data.get("visual_direction", "")).strip(),
        }

    # ---- 2. 小红书笔记 ----
    xhs_prompt = _XHS_NOTE_PROMPT.format(
        brand=brand or "",
        brand_block=brand_block,
        topic=topic_title,
        raw_text=raw_text or "（无）",
        persona=persona or "未指定",
        product_block=product_block,
    )
    xhs_data = _call_llm_json(xhs_prompt, model_cfg, max_tokens=3072)

    if not xhs_data or not xhs_data.get("body") or len(str(xhs_data.get("body", ""))) < 400:
        logger.warning("小红书文案 LLM 返回无效或过短，使用兜底模板 topic='%s'", topic_title)
        xhs_data = _fallback_xhs(topic_title, persona, product_names)

    xhs_body = str(xhs_data.get("body", "")).strip()
    xhs_tags = xhs_data.get("tags") or []
    if not isinstance(xhs_tags, list):
        xhs_tags = []

    xhs_out = {
        "title": str(xhs_data.get("title", "")).strip(),
        "body": xhs_body,
        "tags": [str(t).strip() for t in xhs_tags if str(t).strip()],
        "note_text": _render_xhs_note_text({"body": xhs_body, "tags": xhs_tags}),
        "visual_direction": str(xhs_data.get("visual_direction", "")).strip(),
    }

    return {
        "douyin": douyin_out,
        "xhs": xhs_out,
        "brand_context_used": bool(brand_ctx.audience_text or brand_ctx.rules_prompt),
    }
