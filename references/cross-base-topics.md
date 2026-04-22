# Step 5 跨 base 读候选话题

## 数据源

BCMA 的候选话题来自独立技能 **hot-topic-insight** 每天 04:00 抓取的抖音双榜 snapshot(热榜 + 飙升榜合计约 100 条).

数据在另一个 Bitable(与 BCMA 主 base 不同):

```yaml
daily_topics:
  app_token: "<HOT_TOPIC_INSIGHT_APP_TOKEN>"   # hot-topic-insight 的 base
  table_id: "<HOT_TOPIC_INSIGHT_TABLE_ID>"
  view_id: ""
  timezone_offset_hours: 8    # 北京时间
  top_k: 5                     # 写入 topic_selection 的 Top K
  max_rollback_days: 14        # T-1 无数据时最多往前回退天数
```

## 字段映射(来自 hot-topic-insight snapshot 表)

```yaml
fields:
  topic: "话题"
  source: "平台"           # 恒为"抖音"
  created_at: "日期"       # 文本日期 YYYY-MM-DD
  fetched_at: "日期"
  category: "分类"         # "已爆"(话题热榜) / "连升"(飙升话题榜)
  rank: "排名"             # 榜内排名,1 最热
  heat: "热度"             # 数值热度
  # 以下字段在 snapshot 表里不存在,留空:
  content_direction: ""
  raw_text: ""
  suggested_action: ""
  target_audience: ""
  heat_delta: ""
  rising_days: ""
```

## 锚定日逻辑

- 默认锚定日 = 北京时间 T-1(今天 = 2026-04-22 → 锚定日 = 2026-04-21)
- 锚定日无数据 → 向前回退 1 天,最多 `max_rollback_days` 天
- 找到最近一天有数据的日期 → 取该日期当天所有记录

## 执行步骤

### 1. 用 feishu_bitable_app_table_record.search 跨 base 拉数据

`feishu_bitable_*` 工具的 `app_token` 参数就是 base 的 token,所以跨 base 只是换参数,不需要额外步骤.

```
tool: feishu_bitable_app_table_record.search
input:
  app_token: "<HOT_TOPIC_INSIGHT_APP_TOKEN>"   # 注意:不是 BCMA_APP_TOKEN
  table_id: "<HOT_TOPIC_INSIGHT_TABLE_ID>"
  page_size: 500
  filter:
    conjunction: "and"
    conditions:
      - field_name: "日期"
        operator: "is"
        value: ["<YYYY-MM-DD 锚定日>"]
```

如果该日期返回 0 条记录 → 锚定日向前回退一天,继续 search,直到:
- 拿到 ≥1 条记录 → 进入下一步
- 回退达到 `max_rollback_days` → 发 warn 卡"<days> 天内无候选数据源",中断 Step 5

⚠️ 授权:首次调用会触发 openclaw-lark auto-auth 弹 OAuth 卡片申请 `base:record:search`.用户点同意即可.**不要**手动跑任何 authorize 命令.

### 2. 拼装 raw_text(供 4R 打分)

每条记录把分类/排名/热度作为关键信号拼到 `raw_text`(hot-topic-insight 无独立 raw_text 字段,必须在这里拼):

```
{category_label} | 排名 {rank} | 热度 {heat}
```

例如:
```
已爆 | 排名 3 | 热度 15.2w
```

这个信号对 R3 Reach 打分非常关键(已爆 vs 连升 vs 排名头部 vs 热度量级).

### 3. 授权:auto-auth

openclaw-lark 插件的 `auto-auth` 层会在 tool 调用失败并返回 `UserScopeInsufficientError` 时自动发 OAuth 卡片给当前用户,**只申请本次调用所需的单个 scope**(例如 `feishu_bitable_app_table_record.search` 只要 `base:record:read`).

用户点同意即可,technician-free.

### 4. 授权范围对照(参考)

每个 `feishu_bitable_*` 工具对应的 scope(来自 `openclaw-lark/src/core/tool-scopes.js`):

| 工具 | Scope |
|---|---|
| `feishu_bitable_app.create` | `base:app:create` |
| `feishu_bitable_app_table.batch_create` | `base:app:update` |
| `feishu_bitable_app_table_field.create` | `base:app:update` |
| `feishu_bitable_app_table_record.search` | `base:record:read` |
| `feishu_bitable_app_table_record.create` | `base:record:create` |
| `feishu_bitable_app_table_record.update` | `base:record:update` |
| `feishu_bitable_app_table_record.delete` | `base:record:delete` |
| `feishu_drive_file.upload` | `drive:file` |

**本技能不调 `feishu_im_*` 工具** —— openclaw-lark 目前只提供 `feishu_im_user_message.send`,走用户身份,需要敏感 scope `im:message.send_as_user`.进度文本由 Agent 直接输出,openclaw 层 reply 时走 bot 身份.

可能会发起 OAuth 的 scope 共 5-7 个,首次调用时分次授权(用户每次只点"同意"一次),非常轻量.比旧版的 13-scope 一次性批量授权体验更好.

## 数据隔离

- hot-topic-insight base 是**读**,不会写回
- BCMA 主 base(`<BCMA_APP_TOKEN>`)是**读写**
- 两个 base 的 OAuth scope 是**分开**的,首次跨 base 读会触发一次新的 OAuth(auto-auth 卡片标题会显示对应的 base 名称,用户点同意即可)
