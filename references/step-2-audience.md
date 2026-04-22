# Step 2 — 品牌人群画像

## 目的

基于 `brand_info` 生成 1-3 个目标人群,每个人群写入 `brand_audience` 表一行.

## 执行逻辑

1. 读 `brand_audience` 表全表(page_size=100),过滤"品牌名称" == 当前品牌的行.
2. 如果已有 ≥1 行且 4 个文本字段非空 → 直接复用,跳过 LLM.
3. 否则:
   - 删除该品牌的旧记录(`feishu_bitable_app_table_record.delete`)
   - 跑下面 Prompt,最多重试 1 次
   - 对返回数组的每个元素用 `feishu_bitable_app_table_record.create` 写一行

## 生成 Prompt（逐字使用,字段名从 Step 1 的 brand_info 填入）

```
你是一名资深品牌策略顾问。请基于以下 <品牌> 品牌的基础信息，为其在中国市场（尤其小红书平台）的每个核心目标人群**分别**生成独立的画像。

## 品牌基础信息（来自 Brand 表，必须严格遵守）
- 品牌名称：<品牌>
- 品类与价格带：<category_price>
- 核心差异化：<differentiation>
- 最大竞品：<competitors>
- 排斥人群（严禁出现在人群画像中）：<excluded_audience>
- 适配人设/美学：<compatible_persona>
- 冲突人设/美学：<conflict_persona>
- 高价值场景：<high_value_scenes>

## 任务
为该品牌选择 1-3 个核心目标人群（从 audience 枚举中选），**每个人群单独输出一个 JSON 对象**，放入数组。
每个人群对象包含以下字段，字段内容必须针对该人群**个性化描述**，不同人群的动机/偏好/描述不应雷同：

[
  {
    "audience": "人群标签（单个）",
    "persona_tags": "该人群的画像关键词，用顿号分隔（≥ 40 字）",
    "motivation": "该人群购买 <品牌> 的核心消费动机，必须引用品牌「核心差异化」（≥ 80 字）",
    "content_preference": "该人群在小红书上偏好的内容类型/风格/博主，必须与「适配人设/美学」吻合（≥ 80 字）",
    "persona_description": "该人群的画像描述，涵盖生活方式、价值观、消费习惯、场景偏好（≥ 100 字）"
  }
]

## 硬性约束
1. 返回 JSON 数组，包含 1-3 个人群对象。每个对象 5 个字段**全部必填**，任何字段为空或低于最小字数都视为失败。
2. audience 只能从下列选项中选择：Z世代、新锐白领、资深中产、精致妈妈、小镇青年。每个对象只填一个。
3. 每个人群的 motivation/content_preference/persona_description 必须体现该人群的独有特征，不能复制粘贴。
4. 画像必须与「适配人设/美学」「高价值场景」高度一致；**严禁**出现「排斥人群」或「冲突人设/美学」中描述的任何特征。
5. motivation 字段中必须出现"核心差异化"的关键词或其自然改写。
6. persona_description 中必须至少提到 1 个来自 Brand 表「高价值场景」的场景。
7. 只返回 JSON 数组，不要包裹 markdown code block，不要加任何解释性文字。
```

## 字段映射与写入形态

```yaml
brand_audience:
  name: "品牌名称"              # 文本: <品牌>
  audience: "典型人群受众"      # 多选: [<audience>]  ← 注意是数组
  persona_tags: "画像标签"       # 文本
  motivation: "消费动机"         # 文本
  content_preference: "内容偏好" # 文本
  persona_description: "人群描述" # 文本
```

`audience` 是 MultiSelect 字段,值必须是列表形态:`["精致妈妈"]` 而非 `"精致妈妈"`.

## 校验

- 数组长度 1-3,元素为 dict
- audience 是 5 枚举之一
- persona_tags ≥ 40 字, motivation ≥ 80 字, content_preference ≥ 80 字, persona_description ≥ 100 字
- 任意一项不满足 → 重试 1 次 → 仍失败则中断并发 fail 卡

## 产出

`personas` 数组 + 拼接后的 `audience_info`(供 Step 3/4 使用,格式见下):

```
audience_info = {
  "audience": ["精致妈妈", "资深中产"],  # 所有 persona 的 audience 合集
  "persona_tags": "<tag1>\n<tag2>",      # 换行拼接
  "motivation": "<mot1>\n<mot2>",
  "content_preference": "<pref1>\n<pref2>",
  "persona_description": "<desc1>\n<desc2>"
}
```
