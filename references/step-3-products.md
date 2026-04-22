# Step 3 — 产品线 + 真实图库

## 🚨 产品真实性红线

产品必须**真实存在**,只列品牌在官网 / 天猫旗舰店 / 京东自营 / 小红书官方店实际在售或最近一年内售卖过的 SKU.你**不确定是否真实**时直接跳过,**绝不编造**.

每条记录都必须带 `source_url` —— 一条可以验证该 SKU 真实性的权威链接(优先顺序:品牌官网 > 天猫旗舰店 > 京东自营 > 小红书品牌号/官方店).没有链接的记录**直接丢弃**,不写入表.

产品矩阵必须同时覆盖两类 `lifecycle_stage`:
- **新品**:近 12 个月内首次上市的 SKU —— 至少 2 款
- **经典爆品**:销售多年或年销量长期领先的招牌 SKU —— 至少 2 款
- 其余可以是 **常规款**

不要用"常规款"凑数假装新品/爆品.

## 执行逻辑

1. 读 `products` 表全表,过滤"所属品牌"包含当前品牌 → 得到 `existing_names` 集合.
2. 跑下面 Prompt 生成候选 SKU 列表.
3. 逐条过滤:
   - `name` 已在 `existing_names` → 跳过
   - `source_url` 为空或不是 `http(s)://` 开头 → 跳过并 log WARNING(防虚构红线)
   - `lifecycle_stage` 不在 `{新品, 经典爆品, 常规款}` → 强制落成"常规款"
4. 用 `feishu_bitable_app_table_record.create` 逐条写入(遇到失败记 skipped_count,不 fallback).
5. 为每个新建的产品调用 Bash 补全"产品图库(真实大片)"(见下面的"产品图库"章节).

## 生成 Prompt（逐字使用）

```
你是一名资深品牌产品专家。请列出 <品牌> 在中国市场的主要产品线（5-10 款核心产品）。

## 品牌基础信息（来自 Brand 表，所有产品必须与这三条高度吻合）
- 品类与价格带：<category_price>
- 核心差异化：<differentiation>
- 高价值场景：<high_value_scenes>

## 目标人群画像（来自品牌人群表）
- 典型人群：<audience 逗号分隔>
- 画像标签：<persona_tags>

## 硬性要求（不满足就不要返回该条）
1. **产品必须真实存在，严禁虚构**。只列 <品牌> 当前在官网 / 天猫旗舰店 / 京东自营 / 小红书官方店实际在售或最近一年内售卖过的 SKU；你**不确定是否真实**时直接跳过，不要编造。
2. 每款产品必须附带 `source_url` —— 一条可以验证该 SKU 真实性的权威链接（优先顺序：品牌官网产品详情页 > 天猫旗舰店 > 京东自营 > 小红书品牌号/官方店）。没有链接就不要返回这条。
3. 产品矩阵必须同时覆盖两类 `lifecycle_stage`：
   - **新品**：近 12 个月内首次上市的 SKU —— 至少 2 款
   - **经典爆品**：销售多年或年销量长期领先的招牌 SKU（长尾复购 / 年销 10w+ / 官方多次复刻）—— 至少 2 款
   - 其余可以是 **常规款**
4. `launch_date` 必填，格式 `YYYY` 或 `YYYY-MM`（尽可能具体；不知道则用该产品官方公布的首发年份）。
5. 所有产品的 price_band 必须落在「品类与价格带」声明的区间内
6. 所有产品的 selling_point / selling_point_detail 必须紧扣「核心差异化」
7. 所有产品的 functions / season 必须服务于「高价值场景」中至少一个场景
8. 产品 persona_tags 必须与目标人群画像对齐

## 返回格式（严格 JSON 数组，不要加包裹或注释）
[
  {
    "series": "产品系列名称",
    "name": "具体产品名称（中文+英文，和官方命名保持一致）",
    "lifecycle_stage": "新品" | "经典爆品" | "常规款",
    "launch_date": "YYYY 或 YYYY-MM",
    "source_url": "https://...（可以公开访问验证该 SKU 真实性的链接）",
    "selling_point": "一句话核心卖点（20 字以内）",
    "selling_point_detail": "详细卖点阐述（50-100 字，引用核心差异化）",
    "persona_tags": ["目标人群标签1", "目标人群标签2"],
    "pain_points": "该产品解决的核心人群痛点（30-60 字）",
    "season": "适用季节",
    "price_band": "价格带",
    "material": "核心材质",
    "functions": ["功能点1", "功能点2"]
  }
]

## 枚举约束
- lifecycle_stage 只能是：新品 / 经典爆品 / 常规款
- persona_tags 只能从以下选项中多选：新锐白领、精致妈妈、学生党、资深打工人、户外玩家、品质中产、潮流青年
- season 只能是：春、夏、秋、冬、四季通用
- price_band 只能是：入门、中端、高端、旗舰、奢华
- functions 只能从以下选项中多选：极致保暖、轻量通勤、防风防水、城市户外、防雨防污、可机洗、高强度抗皱

## 自检
返回前复查：
- 有没有我自己记不清到底存在不存在的 SKU？有 → 删掉。
- `lifecycle_stage=新品` 至少 2 款？`经典爆品` 至少 2 款？不足 → 补齐或不要返回（不要用"常规款"凑数假装新品 / 爆品）。
- 每条都有 `source_url` 吗？没 → 删掉。

不要返回 JSON 之外的任何内容。
```

## 字段映射与写入形态

```yaml
products:
  brand: "所属品牌"              # 文本
  series: "产品系列"             # 文本
  name: "产品名称"               # 文本
  selling_point: "产品卖点"      # 文本
  selling_point_detail: "卖点详细阐述"  # 文本
  persona_tags: "目标人群标签"   # 多选: ["新锐白领", "精致妈妈"]
  pain_points: "人群痛点"        # 文本
  season: "季节"                 # 单选(或文本): "冬"
  price_band: "价格带"           # 单选(或文本): "高端"
  material: "材质"               # 文本
  functions: "功能点"            # 多选: ["极致保暖", "防风防水"]
  lifecycle_stage: "产品生命周期" # 单选: "新品" / "经典爆品" / "常规款"
  launch_date: "上市年月"        # 文本: "2024-09"
  source_url: "官方来源链接"     # 文本(URL)
  asset_gallery_field: "产品图库(真实大片)"  # 附件
```

多选字段必须是数组;单选是字符串.详见 `references/bitable-write-shapes.md`.

## 产品图库补全

每建立一条新产品,追一次 Bash 调用获取真实产品图,再上传到"产品图库(真实大片)"字段:

```bash
# 真实产品图搜索（DuckDuckGo / Bing 图片,不依赖 API Key）
python3 ~/.openclaw/skills/brand-content-marketing-advisor/bcma/image_search.py \
  --query "<品牌> <产品名称> 官方产品图" \
  --num 3 \
  --out /tmp/bcma_imgs/<品牌>/<产品名称>/
```

返回本地绝对路径列表(每行一条).逐条:
1. `feishu_drive_file.upload` 把图片上传到多维表格所在空间 → 得到 `file_token`
2. `feishu_bitable_app_table_record.update` 把 `[{file_token: "..."}, ...]` 写入"产品图库(真实大片)"附件字段

## 校验

- `products_created` ≥ 4(至少覆盖到 2 新品 + 2 经典爆品的下限)
- 每条都有非空 `source_url`
- 新品至少 2 条、经典爆品至少 2 条
- 不满足 → 发 fail 卡并中断,不要继续 Step 4

## 产出

`created_products` 列表,供 Step 6 匹配主推产品使用.
