# Step 5 — 候选话题 4R 批量打分

## 目的

从 hot-topic-insight(跨 base)读当日候选话题,用本品牌的 4R 筛选策略一次性批量打分,Top K 条写入 `topic_selection` 表供 Step 6 消费.

## 执行逻辑

1. **跨 base 读候选**:见 `references/cross-base-topics.md`,得到 `candidates = [{topic, raw_text, platform, category, rank, heat, created_at}]`.
2. **正则预过滤**:逐条用 `blacklist_patterns` / `whitelist_patterns` / `allow_patterns`(见下)过滤,不过的直接 PASS 不计入打分.
3. **加载品牌规则**:读 `brand_topic_rules` 表,取该品牌 `rules_prompt` 的三段:"品牌底层认知"+"4R 筛选法则"+"人设审美一致性校验".
4. **一次 LLM 批量打分**:把全部剩余候选 + 品牌规则 + rubric 用下面 Batch Prompt 喂给 Claude,返回 JSON 数组,每个对象对应一条候选.
5. **决策标签**:用阈值给每条打 ✅ 主推 / 🟡 备选 / ❌ PASS(见下).
6. **Top K**:按总分降序取前 `top_k` 条(config: `daily_topics.top_k=5`)写入 `topic_selection`.

## 正则预过滤

从 `config.yaml.regex_filters` 读取:
- `blacklist_patterns`: 命中任一 → 直接 PASS
- `whitelist_patterns`: 命中 → reason 标记"whitelist hit"
- `allow_patterns`: 非空时必须命中至少一条,否则 PASS

默认黑名单:`政治 / 赌博 / 违法`.按 re.IGNORECASE 搜索 `topic + "\n" + raw_text`.

## 4R Rubric（1-5 分整数）

- **R1 Relevance 相关度**:与品牌核心差异化 / 品类的关联度
- **R2 Resonance 场景力**:是否能自然植入品牌"高价值场景"
- **R3 Reach 流量与趋势**:榜位/热度/上升期;节点爆发力
- **R4 Risk 舆情风险**:5=绝对安全,1=高危

**总分 = R1 + R2 + R3 + R4**(满分 20).

## 决策规则（硬编码,不要让 LLM 自己决策）

```
if R4 <= 2:           → "❌ PASS"   (R4 veto,无论总分)
elif total >= 16 and R4 >= 4:  → "✅ 主推"
elif total >= 13 and R4 >= 3:  → "🟡 备选"
else:                 → "❌ PASS"
```

阈值从 config 读:
- `scoring.threshold_main: 16`
- `scoring.threshold_candidate: 13`
- `scoring.r4_veto_threshold: 2`

LLM 返回的 `decision` 字段仅作参考,最终标签以 Python-style 的上面规则重算为准.

## Batch Scoring Prompt（逐字使用）

品牌规则(三段 scoring sections)作为 system 上下文,候选列表作为 user input.

```
<BRAND_RULES_SECTIONS 从 rules_prompt 摘出"品牌底层认知 + 4R 筛选法则 + 人设审美一致性校验"三段>

---

请对以下 N 条候选话题按 4R 体系进行批量打分。每条话题独立评估，必须返回与输入条目数一致的 JSON 数组。

- R1 Relevance 相关度（1-5 整数）
- R2 Resonance 场景力（1-5 整数）
- R3 Reach 流量趋势（1-5 整数）
- R4 Risk 舆情风险（1-5 整数；5=安全，1=高危）

严格按如下 JSON 数组返回，不要包裹 markdown 代码块，不要加任何解释性文字：

[
  {
    "index": <对应输入条目的序号,从 0 开始>,
    "topic": "<候选话题原文,用于校验对齐>",
    "relevance": <1-5>,
    "resonance": <1-5>,
    "reach": <1-5>,
    "risk": <1-5>,
    "total": <四项之和>,
    "decision": "✅ 主推 / 🟡 备选 / ❌ PASS",
    "one_line_reason": "一句话理由（30 字以内）",
    "content_direction": "主推/备选时给内容方向建议（100 字以内）；PASS 则留空字符串"
  }
]

[候选话题列表]
[0] #<topic_0>
原始文本：<raw_text_0 含分类/排名/热度信号>

[1] #<topic_1>
原始文本：<raw_text_1>

... (逐条列出全部候选)
```

## 写入 topic_selection 表

```yaml
topic_selection:
  topic: "话题名称"            # 文本
  brand: "适用品牌"            # 文本(当前品牌)
  audience: "适用人群"         # 文本(人群拼接字符串,供 Step 6 读)
  r1: "R1 相关度"              # 数字
  r2: "R2 场景力"              # 数字
  r3: "R3 流量趋势"            # 数字
  r4: "R4 舆情风险"            # 数字
  total_score: "总分"          # 数字
  decision: "决策结果"         # 文本(✅ 主推 / 🟡 备选)
  one_line_reason: "一句话理由" # 文本
  content_direction: "内容方向建议" # 文本
  created_at: "入库时间"        # 数字(毫秒时间戳)
  source: "来源"                # 文本(eg. hot-topic-insight)
  fetched_at: "抓取时间"        # 数字
  rule_hits: "规则命中说明"     # 文本
  raw_text: "原始文本"          # 文本
```

只写 Top K 条(按 total_score 降序),且 `decision` 必须是"✅ 主推"或"🟡 备选"(❌ PASS 不落库).

## 校验

- Top K 写入数量 ≤ `daily_topics.top_k`(默认 5)
- 如果全部候选都落入 R4 veto → 发 warn 卡"当日候选全部舆情高危,跳过 Step 6"并结束
- 至少写入 1 条才继续 Step 6
