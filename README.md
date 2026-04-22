# brand-content-marketing-advisor

**全链路品牌内容营销中枢 v6.0.0**(openclaw skill)。给定一个品牌名,从品牌底层认知一路自动跑到抖音 + 小红书双平台可发稿内容,全部落地到飞书多维表格。

> SKILL.md-driven 架构:主剧本在 [SKILL.md](./SKILL.md),六步执行手册在 [references/](./references/),数据读写全部走 `feishu_bitable_*` / `feishu_drive_*` 官方 MCP 工具,不再经过 Python 中间层。

---

## 能做什么

一条触发词 → 6 步流水线跑到底,中途不请示:

| Step | 输出 | 目标表 |
|---|---|---|
| 1 — 品牌 7 维度 | 品类/价格带、核心差异化、最大竞品、高价值场景等 7 个维度 | `brands` |
| 2 — 目标人群画像 | 1-3 个目标人群 + 画像标签 + 消费动机 + 内容偏好 | `brand_audience` |
| 3 — 真实产品线 + 图库 | 5-10 款真实 SKU(新品 / 经典爆品 / 常规款)+ 每款 3 张真实产品图 | `products` |
| 4 — 4R 话题筛选策略 | 品牌专属的小红书话题筛选策略(markdown 长文本) | `brand_topic_rules` |
| 5 — 每日选题 4R 打分 | 跨 base 读 hot-topic-insight 当日候选 → 批量打分 → Top K 入库 | `topic_selection` |
| 6 — 双平台内容生产 | 抖音脚本 + 9:16 封面 + AI 视频 + 小红书标题 + 种草笔记 + 3:4 封面 | `content_matrix` |

---

## 触发方式

在飞书里 @ openclaw bot,消息命中以下任一模式即可:

- `跑 BCMA for <品牌>`
- `品牌内容 <品牌>` / `做内容 <品牌>`
- `brand content <品牌>` / `BCMA <品牌>`
- `为 <品牌> 生成内容` / `<品牌> 选题生成内容`

触发后:每步完成 bot 会 reply 一段阶段性产出文本(`Step X/6 完成 · ...`);失败则 reply 一段 `Step X/6 失败` 摘要 + 下一步建议。全程数据只进飞书多维表格,不写任何本地 dump 文件。

---

## 首次运行

第一次触发(config.yaml 里 `app.app_token` 为空)时,技能会:

1. 用 `feishu_bitable_app.create` 新建一个 base,app_token 回写 config.yaml
2. 用 `feishu_bitable_app_table.batch_create` **一次性**建 6 张业务表(索引列 = `fields[0]`,详见 SKILL.md 的"索引列红线"章节),table_id 回写 config.yaml
3. 进入 Step 1

授权走 openclaw-lark 官方 auto-auth —— 首次调用缺 scope 时会自动弹一张 OAuth 卡片,**只申请本次调用所需的单个 scope**(例如 `base:record:create`),点同意即可。首次运行大约会陆续弹 5-7 张授权卡片,点完后后续运行零打扰。

**不申请的 scope**:`im:message.send_as_user`(敏感)、`im:*`、`contact:*`、`admin:*`。阶段性产出文本由 Agent 直接输出,openclaw 层用 bot 身份 reply 回触发会话,不调任何 `feishu_im_*` 工具。

---

## 配置

配置文件:[`config.yaml`](./config.yaml)(新用户参考 [`config.yaml.example`](./config.yaml.example) 占位字段)。

关键段:
- `app.app_token` — BCMA 主 base(环境变量 `BCMA_APP_TOKEN` 覆盖优先)
- `tables.{brands,brand_audience,products,brand_topic_rules,topic_selection,content_matrix}.table_id` — 6 张业务表
- `fields.*` — 各表字段名映射(中文,跨 step 共享)
- `scoring.{threshold_main=16, threshold_candidate=13, r4_veto_threshold=2}` — Step 5 决策阈值
- `daily_topics.{app_token, table_id, top_k=5, max_rollback_days=14}` — Step 5 跨 base 读 hot-topic-insight
- `downstream.{default_platforms, max_products_per_topic=2, brand_top_k_assets=5}` — Step 6 参数

---

## 目录结构

```
.
├── SKILL.md                           # 主剧本 (6 步流水线, 触发条件, 执行原则, 错误处理)
├── CHANGELOG.md                       # 版本记录
├── config.yaml                        # 运行配置 (app_token / table_id / fields / 阈值)
├── config.yaml.example                # 新用户配置模板
├── requirements.txt                   # Python 依赖 (仅 dreamina_cli 和 image_search 用)
├── bcma/                              # 保留的叶子 CLI (非 MCP 工具)
│   ├── dreamina_cli.py                # Dreamina 外部 CLI 适配 (Step 3 图库, Step 6 封面/视频)
│   └── image_search.py                # DuckDuckGo + Bing 图片搜索 (Step 3 真实产品图)
└── references/                        # 6 步执行手册 + 2 份基础手册
    ├── step-1-init-brand.md           # 品牌 7 维度生成
    ├── step-2-audience.md             # 目标人群画像
    ├── step-3-products.md             # 真实产品线 (含产品真实性红线)
    ├── step-4-topic-rules.md          # 4R 话题筛选策略
    ├── step-5-scoring.md              # 4R 批量打分 rubric + 阈值
    ├── step-6-copywriting.md          # 双平台内容生成 Prompt
    ├── bitable-write-shapes.md        # 飞书字段写入形态速查
    └── cross-base-topics.md           # 跨 base 读 hot-topic-insight 方法
```

---

## 硬约束(写在 SKILL.md 执行原则里)

1. **一次触发,跑到底** — 6 步全自动串联,中途不请示、不暂停
2. **静默执行,只输出阶段性产出文本** — 不调 `feishu_im_*`,不要 `im:message.send_as_user` scope
3. **数据只进多维表格** — 全程不生成 `workspace/*.md` / `workspace/*.json` 数据 dump
4. **产品必须真实** — Step 3 严禁虚构 SKU,Step 6 选品严格按品牌过滤,禁止跨品牌捞 SKU
5. **平台名称中文** — 只写"抖音"/"小红书"/"B站"/"微博",禁止 `douyin` / `xhs` / `bilibili` / `weibo`
6. **授权面最小化** — 只调 `feishu_bitable_*` / `feishu_drive_*`,不调 `feishu_im_*` / `feishu_oauth_*`

---

## 保留的本地 CLI(非 MCP 工具)

两个叶子工具保留在 [`bcma/`](./bcma/),由 SKILL.md 通过 Bash 调用:

- **`bcma/dreamina_cli.py`** —— Dreamina 外部 CLI 适配层,Step 3 图库增强、Step 6 封面(9:16 抖音 / 3:4 小红书)+ AI 视频(9:16, seedance2.0_vip)都调它
- **`bcma/image_search.py`** —— DuckDuckGo + Bing 图片搜索(纯 stdlib,无 API Key),Step 3 补真实产品图

---

## 依赖

- 飞书多维表格 + Drive MCP 工具(`feishu_bitable_*` / `feishu_drive_*`),由 openclaw-lark 扩展提供
- Dreamina CLI(Step 6 视频生成,可选 —— 失败时留空不阻塞流水线)
- Python 3 + `requirements.txt`(仅两个叶子 CLI 用)

---

## 相关链接

- 主剧本:[SKILL.md](./SKILL.md)
- 版本记录:[CHANGELOG.md](./CHANGELOG.md)
- openclaw 技能索引:参见 openclaw 项目主仓库
