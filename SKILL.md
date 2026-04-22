---
name: brand-content-marketing-advisor
description: 全链路品牌内容营销中枢 v6.0.0。SKILL.md-driven 主剧本,6 步流水线(品牌 7 维 → 人群 → 产品 → 4R 策略 → 每日选题 → 双平台内容)从触发词直跑到终态,全程通过 feishu_bitable_* / feishu_drive_* MCP 工具落地到飞书多维表格,授权由 openclaw-lark 官方 auto-auth 代管(首次调用自动弹 OAuth 卡片,只申请本次调用所需 scope)。每步完成后直接在 Agent 文本输出里写阶段性产出,由 openclaw 层用 bot 身份 reply 回触发会话,不自行调 IM 工具(避免敏感的 im:message.send_as_user scope)。
version: v6.0.0
visibility: A
---

# 品牌内容营销参谋 (v6.0.0)

给定一个品牌名,这个 Skill 把品牌侧的 7 维底层认知、目标人群画像、真实产品线与图库、4R 话题筛选策略、每日精选话题打分、以及抖音+小红书双平台内容生产,全自动跑到落到飞书多维表格.

---

## 触发条件

用户消息命中以下任一模式 → **立即从 Step 1 跑到 Step 6,中途不请示**.

- `跑 BCMA for <品牌>`
- `品牌内容 <品牌>` / `做内容 <品牌>`
- `brand content <品牌>` / `BCMA <品牌>`
- `为 <品牌> 生成内容` / `<品牌> 选题生成内容`

未指定品牌时 → 问用户"要跑哪个品牌?",拿到品牌名后直接进入 Step 1.

## 执行原则

1. **一次触发,跑到底**:6 步全自动串联,任何一步不请示用户、不暂停.
2. **静默执行,只输出阶段性产出文本**:每步完成后在 Agent 文本输出里写一小段阶段性产出(见"阶段性产出文本模板"),由 openclaw 层用 bot 身份 reply 回发起会话.**绝不调 `feishu_im_*` 工具主动发消息** —— 那会要 `im:message.send_as_user` 敏感 scope.失败才额外输出一段 fail 摘要.
3. **数据只进多维表格,不写 workspace**:全程不生成 `workspace/*.md` 或 `workspace/*.json` 数据 dump,所有产出落在飞书多维表格里.
4. **产品必须真实**:Step 3 严禁虚构 SKU,见 `references/step-3-products.md` 的红线.Step 6 选品严格按品牌过滤,禁止跨品牌捞 SKU.
5. **平台名称中文**:`content_matrix.platforms` 字段值只写"抖音"/"小红书"/"B站"/"微博",**禁止** `douyin` / `xhs` / `bilibili` / `weibo`.
6. **失败处理**:遇到 `UserScopeInsufficientError` → openclaw-lark auto-auth 会自动弹 OAuth 卡片,用户点同意后重试即可,不要手动跑 authorize 命令.遇到 `FieldNameNotFound`(1254045) → 调 `feishu_bitable_app_table_field.create` 补字段后重试,字段名从 config.yaml 的 fields 映射取.
7. **授权面最小化**:只调 `feishu_bitable_*` / `feishu_drive_*` 工具,**不调** `feishu_im_*` / `feishu_oauth_*` / 任何带 `send_as_user` 的工具.

---

## 配置

读 `~/.openclaw/skills/brand-content-marketing-advisor/config.yaml`:

- `app.app_token` = BCMA 主 base 的 app_token(环境变量 `BCMA_APP_TOKEN` 覆盖优先)
- `tables.{brands,brand_audience,products,brand_topic_rules,topic_selection,content_matrix}.table_id` = 6 张业务表的 table_id
- `fields.{...}` = 各表的字段名映射
- `scoring.{threshold_main=16, threshold_candidate=13, r4_veto_threshold=2}` = Step 5 决策阈值
- `regex_filters.{blacklist_patterns}` = Step 5 正则预过滤
- `daily_topics.{app_token, table_id, fields, top_k, max_rollback_days}` = Step 5 跨 base 读 hot-topic-insight 的配置
- `downstream.{default_platforms=[小红书,抖音], max_products_per_topic=2, brand_topic_lookback_hours=240, brand_top_k_assets=5}` = Step 6 参数

**首次运行(app_token 空)**:
- 用 `feishu_bitable_app.create` 新建 base,把返回的 app_token 回写 config.yaml 的 `app.app_token`
- 用 `feishu_bitable_app_table.batch_create` **一次性**建 6 张表,每张表的 `fields` 数组在**同一次调用**里传完。附件字段用 ui_type=Attachment、多选用 MultiSelect、数字用 Number 等
- 把 table_id 回写 config.yaml 的 `tables.*.table_id`

⚠️ **索引列红线**(踩过坑):Bitable 每张表的第一列是索引列(primary field),由 `batch_create` 里 `fields` 数组的**首个元素**决定。以下 6 个字段必须作为各自表的 `fields[0]` 传入,类型一律 `type=1` (Text):

| 表 | `fields[0]` 必须是 |
|---|---|
| brands | 品牌名称 |
| brand_audience | 品牌名称 |
| products | 所属品牌 |
| brand_topic_rules | 品牌名称 |
| topic_selection | 话题名称 |
| content_matrix | 匹配话题 |

**禁止的写法**:先 `batch_create` 只传 name 不传 fields、再逐个 `app_table_field.create` 补字段 —— 这会让 Bitable 自动生成一个空白"文本"索引列,你后补的字段全部追加在它之后,每张子表第一列都变成空白。一旦出现这种情况,索引列不可删除,只能 `app_table_field.update` 把它 rename 成目标 primary 字段名再把原字段删掉。

---

## Step 1 — 品牌 7 维度

**目的**:保证 `brands` 表有当前品牌的一行,7 维度齐全.

执行剧本见 `references/step-1-init-brand.md`.

完成后直接在 Agent 文本输出里写:
```
🏷️ Step 1/6 完成 · <品牌> 品牌底层认知
- category_price: <值>
- differentiation: <值>
- competitors: <值>
- high_value_scenes: <值>
(其余 3 维已写入 brands 表)
```

## Step 2 — 目标人群画像

**目的**:基于 `brand_info` 生成 1-3 个人群,写入 `brand_audience` 表.

执行剧本见 `references/step-2-audience.md`.

完成后直接在 Agent 文本输出里写:
```
👥 Step 2/6 完成 · <品牌> 目标人群
- 人群 1: <audience1> | <persona_tags 前 40 字>...
- 人群 2: <audience2> | <persona_tags 前 40 字>...
(已写入 brand_audience 表, 共 <N> 行)
```

## Step 3 — 真实产品线 + 图库

**目的**:生成 5-10 款真实 SKU 写入 `products` 表,每款补 3 张真实产品图到"产品图库(真实大片)"附件字段.

执行剧本见 `references/step-3-products.md` **(含产品真实性红线,必读)**.

产品图搜索用 Bash:
```bash
python3 ~/.openclaw/skills/brand-content-marketing-advisor/bcma/image_search.py \
  --query "<品牌> <产品名> 官方产品图" \
  --num 3 \
  --out /tmp/bcma_imgs/<品牌>/<产品名>/
```

返回本地路径列表 → `feishu_drive_file.upload` 上传 → file_token → `feishu_bitable_app_table_record.update` 写入附件字段.

完成后直接在 Agent 文本输出里写:
```
🛍️ Step 3/6 完成 · <品牌> 产品线
- 新增 <N> 款 (新品 <a> / 经典爆品 <b> / 常规款 <c>)
- 跳过 <M> 款（已存在或缺 source_url）
- 产品图库已补 <K> 张
```

## Step 4 — 4R 话题筛选策略

**目的**:生成该品牌专属的《小红书话题筛选策略》markdown,写入 `brand_topic_rules` 表.

执行剧本 + 骨架 + 填充 Prompt 见 `references/step-4-topic-rules.md`.

完成后直接在 Agent 文本输出里写:
```
🎯 Step 4/6 完成 · <品牌> 4R 话题筛选策略
- 长度: <N> 字
- 节选: <前 80 字摘要>...
(已写入 brand_topic_rules 表)
```

## Step 5 — 每日选题 4R 批量打分

**目的**:跨 base 读 hot-topic-insight 当日候选 → 正则预过滤 → 一次 LLM 批量打分 → Top K 写入 `topic_selection` 表.

跨 base 读取方法见 `references/cross-base-topics.md`.
打分 rubric + 决策阈值 + Batch Prompt 见 `references/step-5-scoring.md`.

⚠️ 打分决策标签在 Python-side 用硬编码规则重算(不依赖 LLM 返回的 decision):
```
if r4 <= 2: label = "❌ PASS"
elif total >= 16 and r4 >= 4: label = "✅ 主推"
elif total >= 13 and r4 >= 3: label = "🟡 备选"
else: label = "❌ PASS"
```

完成后直接在 Agent 文本输出里写:
```
🔥 Step 5/6 完成 · <品牌> 每日选题打分
- 锚定日: <YYYY-MM-DD> (hot-topic-insight)
- 候选 <N> 条 → 过滤后 <M> 条 → Top <K> 入库
- 主推 <a> / 备选 <b>
- Top 1: #<topic>  R1-R4 = <r1>/<r2>/<r3>/<r4>  一句话: <reason>
```

## Step 6 — 双平台内容生成

**目的**:为 Top K 话题逐条生成抖音脚本 + 封面 + 视频 + 小红书笔记 + 封面,一行写入 `content_matrix` 表.

执行剧本 + 双平台 Prompt + dreamina 调用 Bash 命令见 `references/step-6-copywriting.md`.

Dreamina CLI 调用(每条话题 2 次 image2image + 1 次 text2video):
```bash
# 抖音 9:16 封面
dreamina image2image \
  --images="<产品底图本地路径>" \
  --prompt="<抖音封面 prompt>" \
  --ratio=9:16 \
  --resolution_type=2k \
  --poll=120

# 小红书 3:4 封面
dreamina image2image --images=... --ratio=3:4 --resolution_type=2k --poll=120

# 9:16 AI 视频(前 brand_top_k_assets=5 条才做)
dreamina text2video --prompt=... --duration=5 --ratio=9:16 --model_version=seedance2.0_vip --poll=180
```

本地 PNG/MP4 → `feishu_drive_file.upload` → file_token → `feishu_bitable_app_table_record.create` 写入附件字段.

完成后直接在 Agent 文本输出里写终态文本:
```
✨ Step 6/6 完成 · <品牌> 双平台内容生产
- 处理话题 <K> 条 → 写入 content_matrix
- 抖音脚本 <a> / 小红书笔记 <b>
- 封面 <a 抖 / b 红> · 视频 <N>
- 飞书多维表: https://feishu.cn/base/<app_token>?table=<content_matrix.table_id>
```

---

## 阶段性产出文本模板

每步完成后,Agent 直接在自己的文本回复里追加一段产出文本(openclaw 层会用 bot 身份 reply 回触发会话).**不要调任何 feishu_im_* 工具**.

格式建议:
- 成功/进度:以 emoji + `Step X/6 完成` 开头
- 跳过/警告:以 `⚠️ Step X/6 告警` 开头,说明原因 + 影响范围 + 是否继续
- 失败/中断:以 `❌ Step X/6 失败` 开头,给错误摘要 + 下一步建议(通常是让用户点 OAuth 同意或补 Bitable 字段)

## 飞书多维表格写入规范

- **字段形态**:详见 `references/bitable-write-shapes.md`(文本/数字/单选/多选/URL/附件/人员/日期的 JSON 形态).
- **附件**:两步,先 `feishu_drive_file.upload` 取 file_token,再写入附件字段 `[{"file_token": "..."}]`.
- **多选**:即便单值也必须传数组 `["抖音"]`.
- **批量**:Step 2 人群、Step 3 产品、Step 5 话题优先用 `feishu_bitable_app_table_record.batch_create`(单次最多 500 条).附件字段不能批量.

## 常见错误处理

| 错误 | 处理 |
|---|---|
| `UserScopeInsufficientError` / 权限不足 | auto-auth 会自动弹 OAuth 卡片,用户点同意后重试.不要手动跑 authorize 命令,不要调 `feishu_oauth_batch_auth`. |
| 1254045 `FieldNameNotFound` | 字段缺失 → 调 `feishu_bitable_app_table_field.create` 补字段(类型按 config.yaml 推断),重试写入 |
| 1254064 单选/多选值非法 | 检查 config 里的枚举是否与 LLM 输出对齐;多选必须是数组 |
| 1254066 附件 file_token 无效 | 重跑 `feishu_drive_file.upload` 取新 token |
| 1254015 多选传了字符串 | 包一层数组 |
| LLM 返回 JSON 解析失败 | 最多重试 1 次(用"你上次返回的 JSON 有以下问题:..."的修正 prompt),仍失败发 fail 卡中断当前 step |
| dreamina CLI 失败或未登录 | 封面/视频字段留空,记 skip_reasons,不阻塞流水线继续下一条 topic |
| hot-topic-insight 当日无数据 | 锚定日往前回退一天,最多 `max_rollback_days=14` 天.超过 → warn 卡"N 天内无候选",中断 Step 5 |
| Step 6 该品牌产品池为空 | warn 卡"请先跑 Step 3",中断 Step 6,不跨品牌捞 SKU |

## 授权说明(给新同学看)

本技能所有多维表格 / 消息 / Drive 操作都走 openclaw-lark 官方 MCP 工具.这些工具有自动授权层(auto-auth):

> 当工具调用缺少 OAuth scope 时,会自动发一张 OAuth 卡片到你的飞书会话,**只申请本次调用所需的单个 scope**(例如 `base:record:create`).你点同意即可,不需要做任何前置手工授权.

首次触发本技能,大约会在 Step 1-6 过程中陆续弹 5-7 张授权卡片(对应 `base:record:read/create/update/delete`、`base:app:create/update`、`drive:file`),全部点同意后后续运行零打扰.

**不会申请的 scope**:`im:message.send_as_user`(敏感)、`im:*`(进度文本由 Agent 输出,openclaw 层 reply 时走 bot 身份)、`contact:*`、`admin:*`.如果看到 OAuth 卡片里出现这些 scope,说明技能调用了错误的工具,点拒绝并反馈.

跨 base 读 hot-topic-insight 时会额外弹 1 张针对该 base 的 `base:record:read` 卡片,同样点同意即可.

## 保留的本地 Python CLI(非 MCP 工具)

两个叶子工具保留在 `bcma/`:

- `bcma/dreamina_cli.py` —— 封装 dreamina 外部 CLI(图/视频生成),Step 3 图库增强、Step 6 封面/视频都调它
- `bcma/image_search.py` —— DuckDuckGo + Bing 图片搜索(纯 stdlib,无 API Key),Step 3 产品真实图

都通过 Bash 调用,不在 Claude 会话内解释其内部逻辑.

## 表结构

6 张业务表及字段详见 `config.yaml.fields.*`.首次运行通过 `feishu_bitable_app_table.batch_create` 一次建好.

| 表 | 主键字段 | 关键字段 |
|---|---|---|
| brands | 品牌名称 | 品类与价格带、核心差异化、最大竞品、高价值场景等 7 维 |
| brand_audience | (品牌, 人群) | 典型人群受众、画像标签、消费动机、内容偏好、人群描述 |
| products | (品牌, 产品名称) | 产品卖点、人群标签、功能点、产品生命周期、官方来源链接、产品图库 |
| brand_topic_rules | 品牌名称 | 话题筛选及评估逻辑(长文本 markdown) |
| topic_selection | (品牌, 话题) | R1-R4 + 总分、决策、一句话理由、原始文本 |
| content_matrix | (品牌, 话题) | 抖音脚本/封面、小红书标题/笔记/封面、视频素材、适用平台 |
