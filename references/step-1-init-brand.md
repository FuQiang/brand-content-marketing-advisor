# Step 1 — Brand 7 维度生成

## 目的

保证 `brands` 表里存在目标品牌的 7 维度底层信息,作为 Step 2-6 的唯一事实来源。

## 执行逻辑

1. **查找**：用 `feishu_bitable_app_table_record.search` 读 `brands` 表全表(page_size=200),逐行比较"品牌名称"字段,精确等值匹配(`.strip()`).
2. **命中且 7 维度齐全** → 跳过,把 `brand_info` 传给 Step 2.
3. **命中但有空字段** → 只对空字段重跑下面的 Prompt,用 `feishu_bitable_app_table_record.update` 回写空的列.
4. **未命中** → 跑下面的 Prompt,用 `feishu_bitable_app_table_record.create` 新建整行.

## 生成 Prompt（逐字使用）

```
你是一名资深品牌策略顾问。请为「<品牌名>」品牌在中国市场的品牌定位生成 7 维度的基础信息。

请严格返回以下 JSON 格式，不要包裹 markdown code block：
{
  "category_price": "品类与价格带（例：肉制品/火腿肠/冷鲜肉，10-100元）",
  "differentiation": "核心差异化（该品牌区别于竞品的独特优势，30-80字）",
  "competitors": "最大竞品（3-5个，逗号分隔）",
  "excluded_audience": "排斥人群（该品牌不适合的人群，3-5个，逗号分隔）",
  "compatible_persona": "适配人设/美学（该品牌调性匹配的人设标签，3-5个，逗号分隔）",
  "conflict_persona": "冲突人设/美学（与品牌调性冲突的人设标签，3-5个，逗号分隔）",
  "high_value_scenes": "高价值场景（该品牌产品最常出现的使用场景，3-5个，逗号分隔）"
}

## 硬性约束
1. 所有 7 个字段必须非空，每个字段至少 10 个字。
2. 基于该品牌在中国市场的公开信息和行业认知生成，保持客观准确。
3. 只返回 JSON，不要加任何解释性文字。
```

## 字段映射

```yaml
brands:
  name: "品牌名称"
  category_price: "品类与价格带"
  differentiation: "核心差异化"
  competitors: "最大竞品"
  excluded_audience: "排斥人群"
  compatible_persona: "适配人设/美学"
  conflict_persona: "冲突人设/美学"
  high_value_scenes: "高价值场景"
```

## 校验

所有 7 个字段非空、每个 ≥ 10 字。LLM 返回不合格时最多重试一次,仍失败发 fail 卡中断流水线。

## 产出

```
brand_info = {
  "name": "<品牌>",
  "category_price": "...",
  "differentiation": "...",
  "competitors": "...",
  "excluded_audience": "...",
  "compatible_persona": "...",
  "conflict_persona": "...",
  "high_value_scenes": "..."
}
```

供 Step 2-6 消费。
