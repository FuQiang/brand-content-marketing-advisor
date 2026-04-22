# Step 6 — 双平台内容生成 + 封面 + 视频

## 目的

为 Step 5 写入的 Top K 话题,逐条生成:
1. 抖音短视频分镜脚本 + 9:16 封面 + 9:16 AI 视频
2. 小红书种草笔记(标题+800-1200 字正文+tags)+ 3:4 封面

全部内容落到 `content_matrix` 表,一条话题一行.

## 执行逻辑

逐条话题循环(每条话题):
1. **匹配主推产品**:从 `products` 表按品牌精确匹配,取 `max_products_per_topic`(默认 2)条.选品规则:人群标签匹配度 > 新品/经典爆品优先 > `base_weight` 降序.
2. **生成抖音脚本**:用下面 `抖音 Prompt`,LLM 返回 JSON.
3. **生成小红书笔记**:用下面 `小红书 Prompt`,LLM 返回 JSON.
4. **取产品底图**:从主推产品的"产品图库(真实大片)"附件取第一张 → `feishu_drive_file.download` 下载到 /tmp.
5. **生成抖音封面**:`dreamina image2image` + 底图 + 下面 `抖音封面 Prompt` → 上传回飞书 → file_token.
6. **生成小红书封面**:`dreamina image2image` + 底图 + 下面 `小红书封面 Prompt` → 上传 → file_token.
7. **生成视频**(前 `brand_top_k_assets=5` 条才做):`dreamina text2video` + 下面 `视频 Prompt` → 上传.
8. **写 content_matrix 行**:一条 `feishu_bitable_app_table_record.create`.

## 🚨 产品真实性红线(再次强调)

- Step 6 选品只能用当前品牌的 products 表记录 —— **严禁**跨品牌捞 SKU
- `所属品牌` 字段必须精确等值匹配目标品牌.子串匹配会误召"双汇肠" vs "双汇"
- 如果该品牌产品表为空或 < 1 条 → 不写 content_matrix,发 warn 卡"该品牌产品池为空,请先跑 Step 3"

## 抖音脚本 Prompt（逐字使用）

```
你是品牌「<品牌>」的抖音爆款短视频编剧，擅长把社会热点话题、真实人群洞察与产品卖点揉进 30-60 秒的短视频分镜脚本。

# 品牌
<品牌>

# 品牌人群画像
人群：<audience 拼接>
  画像标签：<persona_tags>
  消费动机：<motivation>
  内容偏好：<content_preference>
  人群描述：<persona_description>

# 品牌底层认知与审美一致性（节选自 BrandTopicRules）
<rules_prompt 的三段:品牌底层认知 + 4R 筛选法则 + 人设审美一致性校验>

# 当前话题
标题：<topic>
原始内容：<raw_text>

# 目标人群启发
<topic_selection 行里的"适用人群">

# 主推产品
## 产品 1: <name>（系列：<series>）
卖点：<selling_point>
卖点详述：<selling_point_detail>
目标人群标签：<persona_tags / 拼接>
功能点：<functions / 拼接>
季节：<season>
材质：<material>
价格带：<price_band>

## 产品 2: ...

# 输出要求
严格按如下 JSON 格式返回，不要任何多余说明，不要 markdown 代码块包裹：

{
  "hook_title": "抖音信息流首帧大字钩子（10-20 字，强冲突或强好奇）",
  "scenes": [
    {"camera": "镜头运镜描述", "action": "人物/产品动作", "subtitle": "屏幕字幕文案"},
    {"camera": "...", "action": "...", "subtitle": "..."},
    {"camera": "...", "action": "...", "subtitle": "..."},
    {"camera": "...", "action": "...", "subtitle": "..."},
    {"camera": "...", "action": "...", "subtitle": "..."}
  ],
  "cta": "结尾行动召唤文案（引导点赞/评论/直播间/购物车，15 字以内）",
  "visual_direction": "整体视觉基调与色调建议（40-80 字，供封面与视频生成参考）"
}

要求：
- hook_title 必须承接话题热度 + 人群痛点，**禁止**通用空话（如"超好看"/"太爱了"）。
- 5 个镜头必须呈"情绪开场 → 产品出场 → 对比切换 → 价值锤击 → 行动召唤"节奏。
- 字幕要能独立阅读（静音观看也成立）。
- 产品 Logo、服装结构、材质必须真实，不得凭空编造。
- 严格遵守品牌 4R 筛选法则中的"排斥人群 / 冲突人设美学"红线。
```

分镜脚本文本格式化为:
```
【首帧钩子】<hook_title>

【镜头1】
运镜：<camera>
动作：<action>
字幕：<subtitle>

【镜头2】
...

【CTA】<cta>
```

写入 `content_matrix.douyin_script` 字段.

## 小红书笔记 Prompt（逐字使用）

```
你是品牌「<品牌>」的小红书种草笔记达人，擅长以第一人称情绪化长文把产品价值嵌入真实生活场景。

<与抖音 Prompt 完全相同的 # 品牌 / # 品牌人群画像 / # 品牌底层认知 / # 当前话题 / # 目标人群启发 / # 主推产品 六段>

# 输出要求
严格按如下 JSON 格式返回，不要任何多余说明，不要 markdown 代码块包裹：

{
  "title": "小红书标题（20 字以内，必须带 1-2 个合适 emoji，强钩子）",
  "body": "小红书正文（**800-1200 字**，第一人称，段落分明，包含场景还原/真实痛点/产品使用体验/购买理由/适配人群 5 段结构）",
  "tags": ["#话题标签1", "#产品标签2", "#人群标签3", "#场景标签4", "#品类标签5"],
  "visual_direction": "封面拍摄建议：构图/光线/主体占比（40-80 字）"
}

要求：
- **body 严格 800-1200 字**，少于 800 字视为不合格。
- 第一人称视角，用"姐妹们/宝子们"等小红书常见称呼自然融入。
- 段落清晰，每段首句要能独立承接情绪（用户 1 秒扫读也能抓住）。
- 产品卖点必须落在具体场景里（例如"零下 10 度接娃放学站在风口里等 20 分钟"），禁止干巴巴列参数。
- tags 数量 3-6 个，混合话题/产品/人群/场景/品类维度。
- 严格遵守品牌 4R 筛选法则中的"排斥人群 / 冲突人设美学"红线。
```

- `title` 写入 `content_matrix.xhs_title`
- `body + "\n\n" + tags.join(" ")` 写入 `content_matrix.xhs_note`

## 抖音 9:16 封面 Prompt（喂给 dreamina）

```
以提供的真实产品实拍图为唯一底图，为抖音短视频信息流首帧设计一张 9:16 竖版封面海报。品牌为「<品牌>」。关联话题为「<topic>」。必须严格保留原图中的服装款式、版型结构、面料细节和品牌 Logo，不得凭空生成新衣服、不得改变衣服轮廓或 Logo 形态，只允许调整背景、光影、色调、构图和文字排版。用中文大字展示首帧钩子：「<hook_title>」，大标题占画面上 1/3 黄金区，字重厚实、字号醒目、避开抖音信息流进度条/头像/操作栏等安全区域，静音也能 1 秒读懂。整体视觉基调参考：<visual_direction>。设计风格：构图简洁有冲击力、色彩强对比、信息层级清晰，避免过度花哨或动漫化，保留真实城市生活质感，适配抖音信息流的快速扫读节奏。输出为 9:16 竖版构图，2K 分辨率 PNG 海报。
```

Bash 调用:
```bash
dreamina image2image \
  --images="/tmp/bcma_base_<brand>_<product>.png" \
  --prompt="<上面的 Prompt>" \
  --ratio=9:16 \
  --resolution_type=2k \
  --poll=120
```

或:
```bash
python3 ~/.openclaw/skills/brand-content-marketing-advisor/bcma/dreamina_cli.py \
  image2image \
  --base-image=/tmp/bcma_base.png \
  --prompt="..." \
  --ratio=9:16
```

(底图缺失时自动退回 `text2image`)

产出本地 PNG → `feishu_drive_file.upload` → file_token → 写入 `content_matrix.douyin_cover` 和 `content_matrix.cover_image_ai_field`(兼容旧看板).

## 小红书 3:4 封面 Prompt

```
以提供的真实产品实拍图为唯一底图，为小红书种草笔记设计一张 3:4 竖版封面。品牌为「<品牌>」。关联话题为「<topic>」。必须严格保留原图中的服装款式、版型结构、面料细节和品牌 Logo，不得凭空生成新衣服、不得改变衣服轮廓或 Logo 形态，只允许调整背景、光影、色调、构图和文字排版。用中文展示小红书标题：「<xhs_title>」，保留标题中的 emoji；字体清晰有质感、风格贴近小红书博主审美，避免艺术字或过度花哨的字效。构图建议：<visual_direction>。设计风格：自然光、暖色调、真实生活质感，人物主体占比适中（约 50-65%），背景留呼吸感、不要过度滤镜或动漫化处理，适配小红书九宫格缩略图的扫读。输出为 3:4 竖版构图，2K 分辨率 PNG 海报。
```

```bash
dreamina image2image --images=... --ratio=3:4 --resolution_type=2k --poll=120
```

上传写入 `content_matrix.xhs_cover`.

## 视频 Prompt（text2video,只生成前 5 条）

```
【脚本】
<body/正文文本>

【运镜指令（由脚本解析）】
- 推近/特写：镜头贴近人物或产品细节，突出质感与情绪。
- 跟拍：镜头跟随人物走路或移动，保持平稳运动。
- 转场/交叉剪辑：在不同场景间进行切换，强化前后场景对比。
- 细节微距：用近景/微距呈现衣料、毛领、拉链等关键细节。
- 字幕叠加：在关键画面上叠加简洁文案，卡点节奏。

重点约束：实施「品牌Logo精准保护」，请准确还原【<品牌>】的官方品牌 Logo（如金属剪刀Logo/经典标识等），Logo 必须清晰、标准、比例正确、无变形、无乱码错字，切勿发生 AI 幻觉。
```

```bash
dreamina text2video \
  --prompt="..." \
  --duration=5 \
  --ratio=9:16 \
  --model_version=seedance2.0_vip \
  --poll=180
```

上传写入 `content_matrix.video_asset_ai`.

## content_matrix 写入规范

```yaml
content_matrix:
  topic: "匹配话题"              # 文本
  brand: "适用品牌"              # 文本
  audience: "目标人群"           # 文本
  products: "主推产品"           # 文本(多产品用 / 分隔)
  platforms: "适用平台"          # 多选: ["抖音", "小红书"]  ← 中文
  hook: "爆款标题/钩子"          # 文本(= hook_title)
  body: "正文与脚本"             # 文本(= 小红书 body)
  visuals: "视觉画面建议"        # 文本(= visual_direction)
  logic_breakdown: "爆款逻辑拆解(为什么会火)" # 文本(可选,LLM 总结)
  douyin_script: "抖音短视频脚本" # 文本(格式化后的分镜)
  douyin_cover: "抖音封面(9:16)" # 附件: [{file_token}]
  xhs_title: "小红书标题"        # 文本
  xhs_note: "小红书种草笔记"     # 文本(body + tags)
  xhs_cover: "小红书封面(3:4)"   # 附件
  cover_image_ai_field: "视频封面(AI生成)" # 附件(= douyin_cover 镜像)
  video_asset_ai: "视频素材(AI生成)" # 附件
  generated_at: "生成时间"       # 数字(毫秒)
  source_topic_id: "来源话题ID"  # 文本
  idempotent_key: "幂等键"       # 文本(eg. sha256(brand+topic+date))
```

**平台名称必须是中文**(抖音/小红书),禁止 douyin/xhs.

## 校验

- 每条 topic 至少匹配到 1 个主推产品
- xhs_note body ≥ 800 字
- 抖音 scenes 必须有 5 个
- 封面/视频失败时不 fallback 占位符,字段留空,skip_reasons 记录原因
- 所有失败逐步发 fail 卡,但流水线继续处理下一条 topic
