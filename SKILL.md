---
name: brand-content-marketing-advisor
description: 全链路品牌内容营销中枢 v5.9.3。强制用户授权访问飞书多维表（禁止 tenant_access_token 降级，无 UAT 时直接报错引导 /feishu_auth）。首次运行自动建表 + 自动生成品牌数据，无需手工前置。推荐用 `run_all --brand "品牌名"` 一键执行 6 步全流程（init_brand → init_audience → init_products → init_topic_rules → select_topic → generate_brand_content），单步失败不中断后续。全程回填飞书多维表格，每步发卡片通知。
version: v5.9.3
---

# 品牌内容营销参谋 (v5.9.3)

本 Skill 提供一个全链路、6 步独立 CLI 的营销内容中枢系统。**首次运行自动创建飞书多维表格和全部表结构，品牌不存在时自动 LLM 生成基础数据，无需任何手工前置步骤。** 推荐使用 `run_all` 一键执行全流程。通过 6 个可独立运行的子命令，逐步完成品牌人群画像、产品线、专属 4R 话题筛选策略的数据建设，以及每日精选话题筛选和双平台内容生产的完整自动化流水线。

## 版本概览

- **v5.9.3（当前）**
  - **新增 `authorize` 子命令：走本技能专属的最小 scope device-flow 授权**，绕开 openclaw-lark 的 `/feishu_auth`（后者会一次性索要应用开通的全部 user scope，在 100+ scope 的共享应用里授权面过大）。
  - `bcma/authorize.py` 新模块：Python 实现 RFC 8628 device flow，scope 清单硬编码为本技能真正需要的 **13 个**（`base:app:create` / `base:table:{create,read}` / `base:field:{create,read,delete}` / `base:record:{retrieve,create,update,delete}` / `drive:file:{upload,download}` / `offline_access`），其余一概不申请。
  - UAT 加密存储复用 `~/.local/share/openclaw-feishu-uat/master.key`，与 openclaw-lark 完全兼容的 AES-256-GCM 格式，但写入到 **`.bcma.enc`** 后缀的独立文件（`{app_id}_{open_id}.bcma.enc`）。`bcma/bitable.py` 读取时优先 `.bcma.enc`，fallback 到 `.enc`，两套 UAT 和平共存，互不覆盖。
  - 典型流程：`python3 main.py authorize` → 终端打印 verification_uri_complete + user_code → 用户在浏览器点确认（授权页只显示 13 个 scope）→ 脚本每 5 秒轮询 token 端点 → 成功后调 `/authen/v1/user_info` 拿 open_id → 加密写入 `.bcma.enc`。
  - 一次授权后 `refresh_token` 默认 7 天有效期，过期前自动刷新；7 天后过期需要重新跑 `authorize`。

- **v5.9.3**
  - **鉴权强约束：禁止 tenant_access_token 降级**。`bcma/bitable.py` 的 `_get_token()` 优先级改为：显式 `user_token` → 环境变量 `LARK_USER_ACCESS_TOKEN` → `openclaw-feishu-uat` 加密存储 UAT（带自动 refresh）。三路全空时抛 `NoUserTokenError`（新增 import，供 `main.py` 捕获并做友好错误卡片），**不再回退 tenant_access_token**。
  - **动机**：之前 brands / brand_audience / products / brand_topic_rules / topic_selection / content_matrix 全走 tenant_access_token 降级，飞书返回 403 写入权限不足（应用没有对这些表的写 scope）。改为强制用户授权方式后，权限随登录用户走，天然满足写入诉求。
  - **影响面**：
    - `BitableClient._call_api` 收到 403 时不再自动把 `self._user_token` 置空退回 tenant token，而是记录 error log 直接上抛，避免"偷偷降级"制造隐性数据不一致；
    - `preflight_write_check` 错误文案改为引导用户在飞书 Bot 会话里发 `/feishu_auth` 重新授权；
    - `get_token_type()` 新增 `no_user_token` 枚举值（原来无 UAT 时返回 `tenant_access_token`）。
  - **触发 UAT 授权的方式**：在 openclaw-lark 机器人会话里发送 `/feishu_auth`（由 `@larksuite/openclaw-lark` 的 `commands/auth.js` 处理），完成后加密 UAT 会写入 `${XDG_DATA_HOME:-~/.local/share}/openclaw-feishu-uat/*.enc`，本技能自动读取。
  - 其他业务逻辑完全不变。

- **v5.8.0**
  - **CLI 重构为 6 步独立子命令**：移除 `init_brand`（原 4 步组合命令）、`run_upstream`、`run_downstream`、`run_full_pipeline`，将品牌数据建设拆分为 4 个独立 CLI：
    - `load_brand --brand "品牌名"` — Step 1，只读加载 Brand 表 7 维度
    - `init_audience --brand "品牌名"` — Step 2，LLM 生成品牌人群画像（自动加载 Step 1 依赖）
    - `init_products --brand "品牌名"` — Step 3，LLM 生成产品线 + 图库补全（自动加载 Step 1+2 依赖）
    - `init_topic_rules --brand "品牌名"` — Step 4，内置骨架 + LLM 生成品牌 4R 策略（自动加载 Step 1+2 依赖）
  - Step 5 `run_brand` 和 Step 6 `run_brand_content` 保持不变。
  - 每个独立步骤在运行时自动从飞书表加载前置步骤的数据（如 Step 3 会自动读取 Brand 表和 brand_audience 表），无需手动传递中间结果。依赖数据缺失时会明确报错提示先运行前置步骤。
  - 典型调用顺序：`init_brand → init_audience → init_products → init_topic_rules → run_brand → run_brand_content`
  - `brand_setup.py` 新增 `load_existing_audience` 公共辅助函数，供独立步骤从 brand_audience 表加载已有人群数据。
  - `__init__.py` 导出 `run_step1_load_brand` / `run_step2_brand_audience` / `run_step3_products` / `run_step4_topic_rules` / `load_existing_audience` 五个新增公共 API。
  - `run_init_brand` 函数在代码中保留（向后兼容），但不再作为 CLI 子命令暴露。

- **v5.7.0**
  - **新增表结构同步能力（bcma/schema_sync.py）**：提供 `sync_table_schema` / `sync_all_schemas` 两个入口，对 Skill 托管的 5 张表做结构对齐 —— 基于 `config.fields[table]` 登记的字段映射，与 Bitable `list_table_fields` 结果做 diff，**创建缺失字段**并在样本量足够时**安全清理未登记的整列全空字段**。
    - **类型推断**：按字段 key 名分层映射到 Bitable type_code / ui_type，命中 `_ATTACHMENT_KEY_HINTS`（如 `asset_gallery_field` / `cover_image_ai_field` / `douyin_cover` / `xhs_cover` / `video_asset_ai`）走 Attachment，命中 `_NUMBER_KEY_HINTS`（如 `r1..r4` / `total_score` / `base_weight`）走 Number，命中 `_DATETIME_KEY_HINTS`（如 `created_at` / `fetched_at` / `generated_at`）走 DateTime，命中 `_MULTISELECT_KEY_HINTS`（如 `audience` / `persona_tags` / `functions` / `platforms`）走 MultiSelect，其余一律兜底 Text。
    - **安全删除策略**：要同时满足「Bitable 有 ∧ config 未登记 ∧ 非系统保护列（记录ID / 创建时间 / 修改时间 / 创建人 / 修改人）∧ 整列全空」四条件；且记录数 < `MIN_RECORDS_FOR_CLEANUP`（=3）时跳过整个删除阶段，避免冷启动时误删用户手动加的自定义列。单次全表扫描判定所有列的非空状态，避免逐字段扫表。
    - **brands 表硬跳过**：`HARD_SKIP_TABLE_KEYS = {"brands"}`，作为人工维护的品牌知识库底座，Skill 永不改其结构。
    - **per-table 独立容错**：任一表失败只影响当前表，错误记入摘要 `errors[table_key]` 并继续处理其余表。
  - **每个业务命令支持 `--check-schema` 预飞开关**：`init_brand` / `run_upstream` / `run_downstream` / `run_full_pipeline` / `run_brand` / `run_brand_content` 六个业务子命令统一新增：
    - `--check-schema` — 显式触发结构同步预飞（未传入时行为完全与旧版本一致，**不会默默改表**）。
    - `--schema-tables a,b,c` — 限定预飞生效的 table key 子集，不传默认处理全部 5 张 Skill 托管表。
    - 预飞摘要会在 stdout 以 `=== schema check preflight ===` 分隔块打印，JSON 格式，含 `tables_processed` / `tables_skipped` / `created_fields` / `deleted_empty_fields` / `kept_non_empty_fields` / `errors` / `record_counts`，便于 CI/CD 或人工巡检归档。
  - **新增 `check_schema` 独立 ops 子命令**：纯运维用途，无需绑定业务流程即可做一次结构巡检。支持 `--tables` 限定子集，默认处理 `brand_audience` / `products` / `brand_topic_rules` / `topic_selection` / `content_matrix` 五张表，直接输出同步摘要 JSON。适用于上线前冷启动校验、表结构漂移巡检、升级版本后的预防性对齐。
  - **main.py 版本字符串与 description 同步到 v5.7.0**；`bcma/__init__.py` 额外导出 `sync_all_schemas` / `sync_table_schema` / `SCHEMA_SYNC_DEFAULT_TABLE_KEYS` 三个公共 API，便于其他自动化脚本直接调用。

- **v5.6.0**
  - **第六步 `run_brand_content` 升级为双平台 LLM 内容生产**：新增 `bcma/copywriting.py` 模块，把文案生成从 v5.5.0 及以前的确定性模板切换为基于品牌上下文的 **双平台 LLM 生成**：
    - 抖音短视频分镜脚本 — 首帧钩子大字 + 5 镜头（情绪开场 → 产品出场 → 对比切换 → 价值锤击 → 行动召唤）+ CTA + 视觉基调。
    - 小红书种草笔记 — 20 字以内带 emoji 标题 + **800-1200 字第一人称正文**（场景/痛点/体验/购买理由/适配人群 5 段结构）+ tags + 构图建议。
    - LLM 调用链沿用 `scoring.py` 的 Claude Opus 4.6 → Claude Sonnet 4.6 → `doubao-pro-32k` 降级；任一失败落到模板化兜底，不阻塞批次。
    - 两端 prompt 都注入「BrandTopicRules 三段品牌底层认知 + 品牌人群画像 + 主推产品详情 + 当前话题原始文本」四要素上下文，保证输出严格遵守品牌红线（排斥人群 / 冲突人设美学）。
  - **ContentMatrix 新增双平台字段（表结构）**：
    - 文案列：`抖音短视频脚本` / `小红书标题` / `小红书种草笔记`
    - 封面列：`抖音封面(9:16)` / `小红书封面(3:4)`
    - 原 `爆款标题/钩子` / `正文与脚本` / `视觉画面建议` 通用列继续回填（镜像自抖音输出），保证 v5.5.0 及以前看板不断图。
    - 关系追踪列（`匹配话题` / `适用品牌` / `目标人群` / `主推产品`）**保留不变**，v5.6.0 额外显式回填「适用品牌」列，让话题 × 品牌 × 人群 × 产品的四元组在表内一目了然。
  - **AI 封面从单张扩展为双平台**：原 v5.2.0 的单张「视频封面(AI生成)」升级为抖音 9:16 + 小红书 3:4 两张封面，共用同一张产品图库底图：
    - 抖音封面 prompt 以抖音首帧大字钩子为核心，冷调强对比、避开信息流安全区；
    - 小红书封面 prompt 以 emoji 标题 + 真实生活质感 + 博主审美为核心，3:4 构图、暖色调自然光；
    - 均严格保留原图的服装款式、版型结构、面料细节与品牌 Logo，只改背景/光影/色调/构图/文字排版；
    - 旧 `视频封面(AI生成)` 字段继续回填抖音 9:16 内容（向后兼容）。
  - **视频保持一条 9:16 AI 视频**：两个平台共用，仍由 `jimeng-video-generator` 生成，视频 Prompt 注入品牌 Logo 精准保护指令。
  - **CLI**：无新增命令；`run_brand_content` 的返回 JSON 新增 `asset_cover_douyin_uploaded` / `asset_cover_xhs_uploaded` 双平台统计字段，原 `asset_cover_ai_uploaded` 保留为两平台之和（向后兼容）。

- **v5.5.0**
  - **第五步重构为"每日精选话题筛选"**：`run_brand` 由原"近期高分话题 → 文案矩阵 → Top K 封面视频"的一键全链路，拆分为纯筛选阶段。新增 `bcma/daily_topics.py`：
    - 跨 base 从外部「每日精选话题」表（`daily_topics.app_token` / `table_id` / `view_id`）读取**北京时间当日**候选话题（按 `fields.created_at` 字段过滤），支持 `--date YYYY-MM-DD` 覆盖。
    - 并发调用 `compute_4r_score_with_model`，以 `BrandTopicRules` 中 Step 4 生成的品牌专属 4R prompt（经 `extract_scoring_sections` 截断后的三段）打分。
    - 按总分 + R4 做 tie-break 排序取 Top K（默认 5，`daily_topics.top_k` 可配，CLI `--top-k` 可覆盖），写入 `TopicSelection` 表并打上"适用品牌"字段。
    - 去重：按 (当日, 品牌, 话题名称) 查 `TopicSelection` 当日该品牌已有话题名，已存在则跳过，保证同一天重复跑 `run_brand` 幂等。
  - **第六步新增 `run_brand_content`**：文案矩阵 + Top K 封面/视频生成从 `run_brand` 剥离，由 `bcma/brand_pipeline.py::run_brand_content_pipeline` 承接。入参/逻辑与 v5.4.0 的 `run_brand` 完全一致（Products 空列清理 / 图库补全 / TopicSelection 按品牌加载 / 匹配主推产品 + 生成文案 / Top K 封面视频附件），只是语义上从"一键全链路"拆分为"内容生产"专职阶段。
  - **Config**：`config.yaml` 新增 `daily_topics` 段（`app_token` / `table_id` / `view_id` / `timezone_offset_hours` / `top_k` / `fields` 映射），默认字段名与 TopicSelection 表保持一致。
  - **CLI**：`run_brand` 新增 `--top-k` 与 `--date` 可选参数；新增 `run_brand_content` 子命令。典型调用顺序变为 `init_brand → run_brand → run_brand_content`。

- **v5.4.0**
  - **Brand 表升级为人工知识底座**：新增独立的 `brands` 表（原 `brands` 重命名为 `brand_audience`），由用户在飞书多维表格中手动维护 **7 维度品牌基础信息**（品牌名称 / 品类与价格带 / 核心差异化 / 最大竞品 / 排斥人群 / 适配人设/美学 / 冲突人设/美学 / 高价值场景）。`init_brand` 流水线只读取该表，绝不写入或改写用户输入。
  - **init_brand 重构为 4 步流水线**：`Step 1` 从 Brand 表只读加载 7 个维度（任意维度为空直接报错） → `Step 2` 基于 Brand 表 LLM 生成 `brand_audience` 品牌人群表，所有字段严格非空校验 + 2 次重试 → `Step 3` 基于 Brand 表 + 品牌人群表上下文 LLM 生成 Products 产品线并补全真实图库 → `Step 4` 基于**内置骨架** + Brand 表 + 品牌人群表动态填充生成该品牌专属 4R 筛选 prompt，upsert 写入 `BrandTopicRules` 表。
  - **Step 4 内置骨架模板**：`bcma/brand_setup.py` 中的 `_TOPIC_RULES_SKELETON` 常量包含一份结构完整的话题策略模板（Role / 品牌底层认知 / 4R 筛选法则 / 人设审美一致性校验 / 反漏斗机制 / 输出格式），所有 `<<>>` 槽位由 LLM 基于 Brand 表 + 品牌人群表内容填充。Step 4 不再从 Bitable 读取任何旧模板，**生成失败直接报错中断，不再使用任何默认兜底模板**。
  - **rules_prompt 严格校验**：生成结果必须满足「无 `<<>>` 残留 / 必要章节标题齐全 / 决策阈值逐字保留（≥ 16 主推、R4 ≤ 2 PASS）/ 长度 ≥ 1500 字」，不合格立即重试或报错。
  - **打分 prompt 分段抽取**：`upstream.py::load_brand_rules_prompt` 新增 `extract_scoring_sections`，per-topic JSON 打分时只注入「品牌底层认知 + 4R 筛选法则 + 人设审美一致性校验」三段，自动截断「反漏斗机制 / 输出格式」章节，避免 LLM 在 JSON 打分和批量表格输出之间混乱。完整 prompt 在 BrandTopicRules 表中原样保留，供周会批量筛选时手工复用。

- **v5.3.0**
  - **品牌数据建设一键流水线**：首次引入 `init_brand` 命令（3 步版本），一键完成品牌画像 / 产品线 / 专属 4R prompt。
  - **4R 打分改为 1–5 分制**：总分 = R1+R2+R3+R4（满分 20）。决策阈值改为 **总分 ≥16 且 R4 ≥4 → 主推；13–15 且 R4 ≥3 → 备选**。新增 **R4 否决机制**：R4 ≤ `r4_veto_threshold`（默认 2）时无论总分一律 PASS。
  - **品牌感知的上游筛选**：`run_upstream` 新增 `--brand` 参数，传入品牌名时会从 BrandTopicRules 表读取该品牌的专属 4R prompt 注入 LLM 打分，并将 `brand` / `content_direction` / `one_line_reason` 回写 TopicSelection。
  - **敏感凭证外移**：`app_token` 默认从环境变量 `BCMA_APP_TOKEN` 读取，`config.yaml` 不再硬编码。
  - **ContentMatrix 写入幂等**：下游写入前按「匹配话题」名去重，`run_downstream` 多次运行不会产生重复记录。
  - **空列清理优化**：Products 空列清理改为单次全表扫描 + 内存判定；记录数 < 3 时自动跳过，避免数据异常时误删。

- **v5.2.0**
  - 新增 **AI 封面生成**：`run_brand` Top K 阶段在真实产品图的基础上，叠加文案「爆款标题」生成 ContentMatrix 表「视频封面(AI生成)」附件，合规约束下只改背景和文字、不修改服装结构与 logo。
  - `config.yaml` 新增 `cover_image_ai_field` 指向「视频封面(AI生成)」。
  - 任一条 AI 封面失败时仅记录并跳过，不中断批次。

- **v5.1.0**
  - 上游产品视觉资产自动补全：在品牌入口 `run_brand` 中，自动为当前品牌的主推产品检查 Products 表「产品图库(真实大片)」附件字段，缺失时调用 image_search 工具抓取 3–5 张高清真实图片并回填。
  - 下游封面仅复用产品库：Top K 内容在生成视频封面时，仅从 Products 表「产品图库(真实大片)」中抽取一张图，自动写入 ContentMatrix 表「视频封面(真实大片)」字段；若图库缺失，则封面字段保持为空，不再调用任何 AI 封面兜底逻辑。
  - Products 表自动清理全空列：在 `run_upstream` 和 `run_brand` 入口阶段，自动扫描 Products 表，删除所有记录均为空的非核心字段。
  - 视频生成基于脚本+运镜指令：调用 `run_brand` 生成短视频时，严格以 ContentMatrix 表「正文与脚本」原文为核心提示词，从脚本文本中解析"特写/拉近/切换/走路"等关键词自动生成「运镜指令」列表。

- **v5.0.0**
  - 新增 **品牌一键入口**：支持 `--brand "品牌名"` 一键完成「近期高分话题 → 文案矩阵 → Top K 视觉封面+视频」全链路。
  - 4R 打分模块升级：旗舰模型优先（Claude Opus / Sonnet）进行 JSON 化打分，自动回退到本地启发式算法。
  - ContentMatrix 表支持自动字段创建：爆款逻辑拆解、视频封面、视频素材(AI生成)。
  - 新增 Top K 视觉资产补全能力：对本次生成的综合得分 Top K 内容自动生成封面和短视频并上传为附件。

- **v4.1.0**
  - 下游文案生成在正文末尾新增「爆款逻辑拆解」段落（100–200 字）。
  - ContentMatrix 表新增字段「爆款逻辑拆解(为什么会火)」，写入上述逻辑拆解文本。

- **v4.0.0**
  - 初始版本，提供上游极速筛选、下游智能联动与内容沉淀能力。

## 核心功能

- **Step 1: 初始化品牌基础信息（init_brand）**
  - 在 `Brands` 表中按品牌名精确查找：
    - **命中且 7 维度齐全** → 直接复用。
    - **命中但部分维度为空** → LLM 自动补齐缺失维度并回写。
    - **未命中** → LLM 生成全部 7 维度（`品类与价格带` / `核心差异化` / `最大竞品` / `排斥人群` / `适配人设/美学` / `冲突人设/美学` / `高价值场景`），新增记录写入。
  - **无需手工前置**：品牌不存在时自动创建；已存在时按需补齐空字段。

- **Step 2: 生成品牌人群画像（init_audience）**
  - 自动加载 Step 1 依赖（Brand 表 7 维度）。
  - 基于 Brand 表调用 LLM 生成人群画像（`audience` 多选、`persona_tags`、`motivation`、`content_preference`、`persona_description`），**所有字段严格非空 + 最小长度校验**，不合格自动重试（最多 2 次）。已有完整记录时直接复用。

- **Step 3: 生成产品线（init_products）**
  - 自动加载 Step 1 + Step 2 依赖（Brand 表 + brand_audience 表）。依赖缺失时明确报错提示先运行前置步骤。
  - 基于 Brand 表 + 品牌人群上下文调用 LLM 生成 5–10 款核心产品，按产品名去重写入 `Products` 表，随后补全「产品图库(真实大片)」附件。

- **Step 4: 生成品牌 4R 策略（init_topic_rules）**
  - 自动加载 Step 1 + Step 2 依赖（Brand 表 + brand_audience 表）。
  - 使用内置 `_TOPIC_RULES_SKELETON` 骨架，由 LLM 基于 Brand 表 + 品牌人群表内容填充所有 `<<>>` 槽位。生成结果经严格校验，通过后 upsert 写入 `BrandTopicRules` 表。

- **Step 5: 每日精选话题筛选（select_topic）**
  - 跨 base 从外部「每日精选话题」表（`daily_topics.app_token` / `table_id` / `view_id`）读取**北京时间当日**的候选话题；
  - 并发调用 4R 打分，使用 `BrandTopicRules` 中 Step 4 生成的品牌专属 prompt（经 `extract_scoring_sections` 截断后的三段）作为评分依据；
  - 按总分 + R4 tie-break 降序取 Top K（默认 5，可配置/可 CLI 覆盖），写入 `TopicSelection` 表并打上「适用品牌」字段；
  - 去重：按 (当日, 品牌, 话题名称) 三元组跳过已存在条目，保证同一天多次运行幂等；
  - **本步骤不再触碰文案/产品匹配/封面/视频**，这些全部交给第六步 `generate_brand_content`。

- **品牌内容矩阵与 Top K 视觉资产（generate_brand_content，第六步）**
  - 读取 `TopicSelection` 表中指定品牌的话题（时间窗口 `brand_topic_lookback_hours`，默认 48 小时），复用下游产品匹配与文案生成逻辑，将内容写入 `ContentMatrix`；
  - 根据 4R 综合得分对本次生成的内容进行排序，选取 Top K 条记录（默认 5 条）进入视觉资产补全流程。

- **视觉资产 Top K 自动补全**
  - **封面自动化**：
    - 基于品牌名、话题标题、目标人群与「视觉画面建议」，构造封面生成 Prompt；
    - 调用 `inner_skills/image-generate` 的 `image_generator.py` 脚本生成高质量封面图（竖版 9:16），并将结果上传至飞书 Drive；
    - 将上传得到的 `file_token` 写入 `ContentMatrix` 表的「视频封面」列（附件类型 Attachment）。
  - **视频自动化**：
    - 将「正文与脚本」+「视觉画面建议」组合为 Dreamina 专属 Prompt；
    - 调用 `user_skills/jimeng-video-generator` 的主脚本，由其内部使用 `dreamina` / `jimeng` CLI 完成视频生成与轮询；
    - 调用失败时自动退回到 `jimeng-video-generator` 的占位视频模拟逻辑；
    - 将本地视频文件上传至飞书 Drive，并写入 `ContentMatrix` 表的「视频素材(AI生成)」列（附件类型 Attachment）。

## 目录结构

```text
brand-content-marketing-advisor/
├── SKILL.md             - 本文档，核心指导
├── main.py              - Skill 主入口，提供 CLI
├── config.yaml          - 所有配置项，包括 app_token、table_id、模型参数等
└── bcma/                - 核心逻辑包
    ├── __init__.py
    ├── bitable.py         - 飞书多维表格读写与结构操作封装
    ├── config.py          - 配置加载与模型选择逻辑（app_token 优先读环境变量）
    ├── brand_setup.py     - 品牌数据建设 Step 1~4（各步独立入口 + load_existing_audience 辅助加载）
    ├── copywriting.py     - 第六步文案核心：基于品牌上下文调用 LLM 双平台生成（抖音短视频脚本 + 小红书种草笔记）
    ├── daily_topics.py    - 第五步：跨 base 读取每日精选话题 + 品牌 4R 打分 + Top K 写入 TopicSelection（含当日幂等去重）
    ├── brand_pipeline.py  - 第六步：基于 TopicSelection Top K 话题生成双平台文案矩阵 + Top K 双平台 AI 封面 + 9:16 AI 视频
    ├── product_assets.py  - 产品图库补全与 Products 表空列清理
    ├── schema_sync.py     - v5.7.0 新增：表结构同步（--check-schema 预飞 + check_schema ops 子命令；brands 硬跳过 + 安全清理空列）
    ├── scoring.py         - 正则过滤与 4R 打分（1–5 分制 + R4 否决 + 旗舰模型优先）
    └── utils.py           - 通用工具函数（字段解析、人群启发式、时间处理等）
```

## 配置说明（config.yaml）

所有业务参数均在 `config.yaml` 中配置。**敏感凭证（`app_token`）不走 YAML，而是通过环境变量 `BCMA_APP_TOKEN` 注入**；未设置时 `Config.app_token` 会显式 `ValueError`。

```bash
export BCMA_APP_TOKEN="<your_bitable_app_token>"
```

- **app.app_token**: 留空，凭证走环境变量。
- **tables**: 共 **6** 张表的 `table_id`：
  - `brands` — **品牌基础信息表**，`init_brand`（Step 1）自动维护：命中则复用，缺字段则 LLM 补齐并回写，未命中则 LLM 生成 7 维度后新增。`table_id` 留空时 Step 1 直接报错。
  - `brand_audience` — 品牌人群画像表（原 `brands` 表重命名，`table_id` 不变），`init_audience`（Step 2）写入。
  - `products` — 产品池表（`init_products`（Step 3）写入 + 图库补全）。
  - `brand_topic_rules` — 品牌专属 4R 筛选 prompt 表（`init_topic_rules`（Step 4）写入 / `select_topic`（Step 5）读取）。
  - `topic_selection` — 上游高分话题表。
  - `content_matrix` — 下游文案与视觉资产表。
- **fields**:
  - `brands` 表（**人工维护**，7 维度）：`name`（品牌名称）、`category_price`（品类与价格带）、`differentiation`（核心差异化）、`competitors`（最大竞品）、`excluded_audience`（排斥人群）、`compatible_persona`（适配人设/美学）、`conflict_persona`（冲突人设/美学）、`high_value_scenes`（高价值场景）。
  - `brand_audience` 表：`name`（品牌名称）、`audience`（典型人群受众，多选）、`persona_tags`（画像标签）、`motivation`（消费动机）、`content_preference`（内容偏好）、`persona_description`（人群描述）。Step 2 生成时所有字段**严格非空**。
  - `products` 表：`brand`、`series`、`name`、`selling_point`、`selling_point_detail`、`persona_tags`（多选）、`functions`（多选）、`season`、`price_band`、`material`、`asset_gallery_field`（真实大片图库）、`non_removable_fields`（空列清理白名单）。
  - `brand_topic_rules` 表：`name`（品牌名称）、`rules_prompt`（话题筛选及评估逻辑，长文本；Step 4 由内置骨架 + LLM 槽位填充生成）。
  - `topic_selection` 表：`topic` / `brand` / `audience` / `r1`–`r4` / `total_score` / `decision` / `one_line_reason` / `content_direction` / `created_at` / `fetched_at` / `source` / `raw_text` / `rule_hits`。
  - `content_matrix` 表（v5.6.0 新增双平台列）：
    - **关系追踪列**：`topic`（匹配话题）/ `brand`（适用品牌）/ `audience`（目标人群）/ `products`（主推产品）/ `platforms`（适用平台）。`topic × brand × audience × products` 四元组让每条 ContentMatrix 记录都能明确对应「为哪个品牌 × 哪个人群 × 哪款产品 × 围绕哪个话题」。
    - **通用文案列（向后兼容 v5.5.0 及以前）**：`hook` / `body` / `visuals` / `logic_breakdown` —— 由双平台 LLM 输出镜像自抖音端（hook = douyin hook_title；body = douyin 分镜脚本 + 爆款逻辑拆解；visuals = douyin visual_direction）。
    - **v5.6.0 双平台文案列**：`douyin_script`（抖音短视频脚本）/ `xhs_title`（小红书标题）/ `xhs_note`（小红书种草笔记，正文 + tags 拼接）。
    - **双平台封面与视频列**：`douyin_cover`（抖音封面 9:16, Attachment）/ `xhs_cover`（小红书封面 3:4, Attachment）/ `cover_image_ai_field`（视频封面(AI生成), Attachment，v5.2.0 旧字段，继续回填抖音 9:16 内容）/ `video_asset_ai`（视频素材(AI生成), Attachment，两个平台共用一条 9:16 AI 视频）。
    - **其他列**：`generated_at` / `source_topic_id` / `idempotent_key`。

- **scoring**（1–5 分制）:
  - `relevance_weight` / `resonance_weight` / `reach_weight` / `revenue_weight`: 兜底权重（仅本地启发式 fallback 时使用；LLM 打分由品牌 prompt 直接规定评分规则）。
  - `threshold_main: 16` —— 总分 ≥16 且 R4 ≥4 → ✅ 主推。
  - `threshold_candidate: 13` —— 总分 13–15 且 R4 ≥3 → 🟡 备选。
  - `r4_veto_threshold: 2` —— R4 ≤ 此值时无论总分一律 PASS（风险一票否决）。
- **regex_filters**: 上游本地正则初筛的黑白名单（`blacklist_patterns` 硬过滤、`whitelist_patterns` 正向信号加权、`allow_patterns` 必须命中白名单）。坏规则会被 `_safe_search` 捕获并跳过，不影响批次。
- **concurrency**: 上游 4R 打分线程池与 HTTP 重试策略（`max_workers` / `max_retries` / `backoff_factor`）。
- **model**: 文案与 4R 评分所用的大语言模型配置：
  - `provider` / `model_name` / `temperature` / `top_p` / `max_tokens`: 兜底配置。
  - `candidates`: 按优先级排序的模型列表，本版本正确写为 `claude-opus-4-6` / `claude-sonnet-4-6` / `doubao-pro-32k`。
- **downstream**:
  - `default_platforms`: 下游文案默认适用平台列表（如「小红书」「抖音」）。
  - `max_products_per_topic`: 每个话题最多匹配的主推产品数量（默认 2）。
  - `brand_topic_lookback_hours`: 品牌入口中，参与评估话题的时间窗口（小时，默认 48）。
  - `brand_top_k_assets`: 品牌入口中参与视觉资产补全的 Top K 条数（默认 5）。

## 使用方法 (CLI)

本 Skill 通过 `main.py` 提供 **6 步独立 CLI** + 1 个 ops 子命令。每个步骤可单独运行，自动从飞书表加载前置依赖数据。

> 命令均假定在 workspace 根目录下执行，Skill 根目录为 `user_skills/brand-content-marketing-advisor/`。执行前请先 `export BCMA_APP_TOKEN=<your_app_token>`。

> **首次运行自动建表**：如果 `config.yaml` 中 `app_token` 为空且所有 `table_id` 为空，首次运行会自动创建飞书多维表格和全部 6 张表（含附件字段），并回写 `config.yaml`。之后每次运行直接复用。

> **Brand 表自动生成**：如果 `Brands` 表中没有目标品牌的记录，Step 1 会自动用 LLM 生成 7 维度基础信息并写入。已有记录中如有空字段也会自动补齐。**无需手动提前填写，直接运行即可。**

### 一键全流程（run_all）— 推荐

**收到用户「运行 brand-content-marketing-advisor」或「执行品牌内容营销」指令时，直接执行 `run_all`，不要询问确认、不要检查前置条件。** 所有前置数据（Brand 表、人群画像、产品线、4R 策略）均会在流程中自动创建。

```bash
python3 user_skills/brand-content-marketing-advisor/main.py run_all --brand "品牌名"
```

执行 Step 1→6 全链路，每步完成发飞书卡片通知。单步失败不中断后续步骤。

### Step 1. 初始化品牌基础信息（init_brand）

判断品牌在 `Brands` 表是否存在，不存在则用 LLM 生成 7 维度并写入；存在但部分维度为空则自动补齐。返回该品牌完整的 7 维度信息供后续步骤使用。

```bash
python3 user_skills/brand-content-marketing-advisor/main.py init_brand --brand "Moose Knuckles"
```

**输出摘要（JSON）：**

```json
{
  "step": 1,
  "brand": "Moose Knuckles",
  "record_id": "rec...",
  "brand_info": {
    "name": "Moose Knuckles",
    "category_price": "加拿大高端重磅羽绒服 6000-18000 元",
    "differentiation": "重磅面料 + 修身剪裁 + 街头美学",
    "competitors": "Canada Goose、Moncler",
    "excluded_audience": "户外硬核极限玩家",
    "compatible_persona": "都市精致女孩 / 松弛感中产",
    "conflict_persona": "户外 gorpcore / 工装粗犷",
    "high_value_scenes": "都市通勤 / 机场 / 跨年派对 / 滑雪度假"
  }
}
```

### Step 2. 生成品牌人群画像（init_audience）

基于 Brand 表 7 维度调用 LLM 生成品牌人群画像并写入 `brand_audience` 表。**自动加载 Step 1 依赖**。已有完整记录时直接复用。

```bash
python3 user_skills/brand-content-marketing-advisor/main.py init_audience --brand "Moose Knuckles"
```

### Step 3. 生成产品线（init_products）

基于 Brand 表 + 品牌人群表调用 LLM 生成 5–10 款核心产品并补全图库。**自动加载 Step 1 + Step 2 依赖**，Step 2 数据缺失时报错提示先运行 `init_audience`。

```bash
python3 user_skills/brand-content-marketing-advisor/main.py init_products --brand "Moose Knuckles"
```

### Step 4. 生成品牌 4R 策略（init_topic_rules）

使用内置骨架模板，由 LLM 基于 Brand 表 + 品牌人群表内容填充所有槽位，严格校验后 upsert 写入 `BrandTopicRules` 表。**自动加载 Step 1 + Step 2 依赖**。

```bash
python3 user_skills/brand-content-marketing-advisor/main.py init_topic_rules --brand "Moose Knuckles"
```

### Step 5. 每日精选话题筛选（select_topic）

跨 base 从「每日精选话题」表读取**北京时间当日**候选话题，使用 `BrandTopicRules` 中该品牌的 4R prompt 打分，按总分降序取 Top K 写入 `TopicSelection` 表。**本步骤不生成任何文案/封面/视频**，只负责筛选。

**前置条件：** 配置 `daily_topics` 段并已跑过 Step 1~4（保证 `BrandTopicRules` 中存在该品牌的 4R prompt，否则会回落通用 prompt 并打 warning）。

**命令格式：**

```bash
python3 user_skills/brand-content-marketing-advisor/main.py select_topic --brand "品牌名" [--top-k 5] [--date 2026-04-12]
```

- `--brand`：必填，品牌名称（须与 `BrandTopicRules` 中记录一致）。
- `--top-k`：可选，Top K 数量；默认读 `daily_topics.top_k`（默认 5）。
- `--date`：可选，指定日期 `YYYY-MM-DD`；默认按北京时间当日。

**执行流程：**

1. 计算当日时间窗口（北京时间 00:00 ~ 次日 00:00 的毫秒时间戳）。
2. 从 `BrandTopicRules` 加载该品牌专属 4R prompt（自动截断「反漏斗/输出格式」章节）。
3. 从 `daily_topics.app_token` / `table_id` 跨 base 读取当日候选话题（字段映射见 `daily_topics.fields`）。
4. 并发调用 `compute_4r_score_with_model`，线程数走 `concurrency.max_workers`。
5. 按总分 + R4 tie-break 降序取 Top K。
6. 查 `TopicSelection` 当日该品牌已有话题名做幂等去重，写入剩余 Top K 并打上「适用品牌」字段。

**示例：**

```bash
# 按北京时间当日，为「加拿大鹅」筛选 Top 5 每日精选话题
python3 user_skills/brand-content-marketing-advisor/main.py select_topic --brand "加拿大鹅"

# 指定日期回溯筛选 Top 3
python3 user_skills/brand-content-marketing-advisor/main.py select_topic --brand "加拿大鹅" --date 2026-04-11 --top-k 3
```

执行成功后在 stdout 输出 JSON 摘要：

```json
{
  "brand": "加拿大鹅",
  "date": "2026-04-12",
  "daily_topics_total": 32,
  "scored_count": 32,
  "top_k": 5,
  "written_count": 5,
  "skipped_dedup": 0,
  "selected_record_ids": ["recxxx", "recyyy"]
}
```

### Step 6. 品牌内容矩阵与 Top K 视觉资产补全（generate_brand_content）

基于第五步写入 `TopicSelection` 的品牌 Top K 话题，围绕「话题 × 产品 × 人群 × 品牌」四要素做 **双平台 LLM 文案生成**（抖音短视频脚本 + 小红书种草笔记），并对 Top K 内容补全双平台封面（9:16 + 3:4）与 9:16 AI 视频附件后写入 `ContentMatrix`。

**命令格式：**

```bash
python3 user_skills/brand-content-marketing-advisor/main.py generate_brand_content --brand "品牌名" [--config <CONFIG_PATH>]
```

- `--brand`：必填，品牌名称（如 `"加拿大鹅"`、`"瑞幸咖啡"`）。
- `--config`：可选，指定 `config.yaml` 路径。

**执行流程：**

1. **Products 空列清理 + 图库补全**：与 v5.5.0 保持一致。
2. **加载近期品牌话题**：从 `TopicSelection` 按时间窗口 + 「适用品牌」字段加载候选话题。
3. **双平台 LLM 文案生成**（v5.6.0 核心变更）：对每条 (话题, 主推产品, 人群) 组合，调用 `bcma/copywriting.py::generate_dual_platform_copy`：
   - 从 `brand_audience` 拼接品牌人群画像长文本；
   - 从 `BrandTopicRules` 读取该品牌的 4R prompt 并经 `extract_scoring_sections` 截断为三段（品牌底层认知 / 4R 筛选法则 / 人设审美一致性校验）；
   - 构造产品 prompt block（名称 / 系列 / 卖点 / 人群标签 / 功能点 / 季节 / 材质 / 价格带）；
   - 调用 LLM 生成抖音 JSON（hook / 5 镜头 / CTA / 视觉基调）与小红书 JSON（标题 / 800-1200 字正文 / tags / 构图建议）；
   - LLM 失败落到模板化兜底，单条失败不影响批次。
4. **写入 ContentMatrix**：`匹配话题` / `适用品牌` / `目标人群` / `主推产品` / `适用平台`（关系追踪） + `抖音短视频脚本` / `小红书标题` / `小红书种草笔记`（双平台） + `爆款标题/钩子` / `正文与脚本` / `视觉画面建议` / `爆款逻辑拆解(为什么会火)`（通用向后兼容）。
5. **Top K 双平台 AI 封面生成**：按 4R 总分取 Top K（默认 5）；对每条记录基于主推产品图库底图调用 `image_edit` 两次，分别得到：
   - 9:16 抖音首帧封面 → 写入 `抖音封面(9:16)` + 回填旧 `视频封面(AI生成)`；
   - 3:4 小红书封面 → 写入 `小红书封面(3:4)`。
6. **Top K 9:16 AI 视频生成**：仍走 `user_skills/jimeng-video-generator`，结果写入 `视频素材(AI生成)`。

**示例：**

```bash
python3 user_skills/brand-content-marketing-advisor/main.py generate_brand_content --brand "加拿大鹅"
```

执行成功后输出 JSON 摘要：

```json
{
  "brand": "加拿大鹅",
  "topic_count": 5,
  "content_created_count": 5,
  "asset_top_k": 5,
  "asset_cover_douyin_uploaded": 5,
  "asset_cover_xhs_uploaded": 4,
  "asset_cover_ai_uploaded": 9,
  "asset_video_uploaded": 5,
  "created_record_ids": ["recx...", "recy..."],
  "top_record_ids": ["recx...", "recz..."]
}
```

> `asset_cover_ai_uploaded` 为双平台之和（向后兼容 v5.5.0 输出），`asset_cover_douyin_uploaded` / `asset_cover_xhs_uploaded` 为新增的分项统计。

## 4R 打分规则与模型选择

### 维度定义（1–5 分制）

| 维度 | 含义 | 5 分 | 1 分 |
| --- | --- | --- | --- |
| **R1 Relevance** | 与品牌核心基因的契合度 | 直接命中品牌核心关键词和场景 | 与品牌定位完全不符 |
| **R2 Resonance** | 能否植入真实高价值生活场景 / 引发人群共鸣 | 自然匹配品牌优先场景 | 匹配警惕场景或无场景 |
| **R3 Reach** | 传播广度与趋势 | 上升期、节点爆发、搜索量健康 | 衰退期、商业笔记饱和 |
| **R4 Risk** | 舆情/平台风险 | 完全安全 | 高危、限流、竞品翻车前科 |

总分 = R1 + R2 + R3 + R4（满分 20）。

### 决策规则

- **✅ 主推** — 总分 ≥ `threshold_main`（默认 16）**且** R4 ≥ 4
- **🟡 备选** — 总分 ∈ [`threshold_candidate`, `threshold_main`)（默认 13–15）**且** R4 ≥ 3
- **❌ PASS** — 其他情况
- **R4 一票否决** — R4 ≤ `r4_veto_threshold`（默认 2）时无论总分多高一律 PASS

### 模型调用策略

- 上游 4R 打分使用 `ThreadPoolExecutor` 并发执行，线程数量由 `concurrency.max_workers` 控制（默认 8）。
- 单条打分流程：
  1. **Prompt 选择** — 若 `BrandTopicRules` 表中查到该品牌记录，使用品牌专属 `_BRAND_SCORING_PROMPT`（注入品牌筛选逻辑，**仅包含「品牌底层认知 + 4R 筛选法则 + 人设审美一致性校验」三段**，由 `extract_scoring_sections` 从完整 prompt 中自动抽取；否则使用通用 `_GENERIC_SCORING_PROMPT`。
  2. **模型优先级** — 优先尝试 Anthropic Claude（需环境变量 `ANTHROPIC_API_KEY` + `anthropic` SDK），失败回落 `byted_aime_sdk` 的 `mcp:llm_chat` 工具调用豆包。
  3. **JSON 解析** — `_parse_4r_json` 兼容 markdown code block 包裹；解析失败记录 warning。
  4. **兜底** — 任意环节失败（密钥缺失、SDK 缺失、网络异常、解析失败）均自动回退到本地启发式 `compute_4r_score`（基于内容 SHA256 生成稳定的 1–5 分，白名单命中时 R1 +1），保证流水线不中断。
- LLM 响应除四个分项外，还需返回：
  - `one_line_reason` — 一句话理由（写入 TopicSelection「一句话理由」列）。
  - `content_direction` — 若是 主推/备选 给出内容方向建议，PASS 时留空（写入「内容方向建议」列）。

### ops. 表结构同步（check_schema / --check-schema）

v5.7.0 起 Skill 提供两种方式触发表结构同步 —— **两种方式都必须显式调用**，Skill **绝不会在用户未明确要求时默默改表**。

#### 7.1 业务命令内联预飞：`--check-schema`

为任何业务子命令追加 `--check-schema` 开关，即可在主流程执行前先做一次结构对齐：

```bash
# 示例：在 init_audience 前先同步全部 5 张托管表的结构
python3 user_skills/brand-content-marketing-advisor/main.py init_audience \
    --brand "加拿大鹅" --check-schema

# 示例：generate_brand_content 前只同步 content_matrix 一张表
python3 user_skills/brand-content-marketing-advisor/main.py generate_brand_content \
    --brand "加拿大鹅" --check-schema --schema-tables content_matrix

# 示例：select_topic 前同步 topic_selection + content_matrix 两张
python3 user_skills/brand-content-marketing-advisor/main.py select_topic \
    --brand "加拿大鹅" --check-schema --schema-tables topic_selection,content_matrix
```

- `--check-schema`：布尔开关，未传入时 v5.6.0 及以前的行为完全不变。
- `--schema-tables`：逗号分隔的 table key 子集，不传默认处理 5 张 Skill 托管表。
- 预飞摘要会以 `=== schema check preflight ===` 分隔块打印在主命令输出之前，方便日志抓取。

#### 7.2 独立运维子命令：`check_schema`

纯结构巡检用途，不驱动任何业务流程，适合冷启动校验、定期巡检或升级版本后的预防性对齐：

```bash
# 处理默认 5 张托管表
python3 user_skills/brand-content-marketing-advisor/main.py check_schema

# 只处理指定表
python3 user_skills/brand-content-marketing-advisor/main.py check_schema \
    --tables content_matrix,topic_selection
```

#### 7.3 同步规则

1. **硬跳过 `brands` 表**：brands 是人工维护的品牌知识库底座，Skill 永不改其结构，即使通过 `--schema-tables brands` 显式传入也会被跳过并记入 `tables_skipped`。
2. **创建缺失字段**：对 `config.fields[table]` 中登记、但 Bitable 未存在的字段，按字段 key 名推断类型创建：
   - `asset_gallery_field` / `cover_image_ai_field` / `douyin_cover` / `xhs_cover` / `video_asset_ai` 等 → Attachment
   - `r1..r4` / `total_score` / `base_weight` 等 → Number
   - `created_at` / `fetched_at` / `generated_at` 等 → DateTime
   - `audience` / `persona_tags` / `functions` / `platforms` 等 → MultiSelect
   - 其余一律兜底 Text
3. **安全清理空列**：同时满足以下 4 条才会被删除：
   - Bitable 有该列 ∧
   - `config.fields[table]` 未登记 ∧
   - 不在系统保护列白名单（记录ID / 创建时间 / 修改时间 / 创建人 / 修改人）∧
   - 整列所有记录均为空（None / 空字符串 / 空数组 / 空对象）
4. **样本量门槛**：`record_count < MIN_RECORDS_FOR_CLEANUP`（=3）时**跳过整个删除阶段**，避免冷启动或数据异常时误删用户手动加的自定义列。
5. **per-table 独立容错**：任一表的 `list_table_fields` / `ensure_field_exists` / `delete_field_if_exists` 抛错都不会中断其他表的处理，错误记入 `summary.errors[table_key]`。

#### 7.4 输出摘要 JSON 结构

```json
{
  "tables_processed": ["brand_audience", "products", "brand_topic_rules", "topic_selection", "content_matrix"],
  "tables_skipped": [
    {"table": "brands", "reason": "brands 表为人工维护底座，Skill 永不改其结构"}
  ],
  "created_fields": {
    "content_matrix": [
      {"field_key": "xhs_cover", "field_name": "小红书封面(3:4)", "type_code": 17, "ui_type": "Attachment"}
    ]
  },
  "deleted_empty_fields": {
    "topic_selection": ["废弃调试列"]
  },
  "kept_non_empty_fields": {
    "content_matrix": ["用户手动添加的非登记但有数据的列"]
  },
  "errors": {},
  "record_counts": {
    "brand_audience": 12,
    "products": 45,
    "brand_topic_rules": 8,
    "topic_selection": 137,
    "content_matrix": 86
  }
}
```

## 视觉封面与视频生成/回填规范（v5.6.0）

- **双平台 AI 封面生成**：
  - 封面底图**仅**来自主推产品在 Products 表「产品图库(真实大片)」附件字段中的第一张真实照片；图库为空时直接跳过封面生成。
  - 以同一张真实产品图为底图，调用 `inner_skills/image-generate` 的 `image_edit.py` 两次，分别生成：
    1. **抖音封面（9:16）** — `_build_douyin_cover_prompt` 构造，核心是首帧大字钩子：大标题放在画面上 1/3 黄金区、字重厚实、避开抖音信息流的进度条/头像/操作栏安全区、整体冷调强对比。写入 `ContentMatrix` 的 `抖音封面(9:16)` 附件列；同时兼容回填旧 `视频封面(AI生成)` 列。
    2. **小红书封面（3:4）** — `_build_xhs_cover_prompt` 构造，核心是 emoji 标题 + 真实生活质感：人物主体占比 50–65%、自然光、暖色调、背景留呼吸感，严格贴近小红书博主审美，禁止艺术字和动漫化处理。写入 `ContentMatrix` 的 `小红书封面(3:4)` 附件列。
  - **合规红线**：两张封面都必须严格保留原图中的服装款式、版型结构、面料细节和品牌 Logo；不得凭空生成新衣服、不得改变衣服轮廓或 Logo 形态，只允许调整背景、光影、色调、构图和文字排版。
  - 任一平台封面失败不影响另一平台，也不影响视频生成；若基础图不可下载或底图缺失，两张封面全部跳过。

- **视频生成（9:16，两个平台共用一条）**：
  - 从 `ContentMatrix` 的「正文与脚本」字段（镜像自抖音分镜脚本）中读取完整脚本；
  - 解析脚本中的“镜头段落”和关键词（如“特写”“拉近”“切换”“走路”等），生成结构化的「运镜指令」列表；
  - 构造极梦视频 Prompt：
    - `【脚本】<正文与脚本原文>`
    - `【运镜指令（由脚本解析）】` 下方为若干 `- 运镜说明` 项；
    - 末尾追加「品牌 Logo 精准保护」约束；
  - 调用 `user_skills/jimeng-video-generator/scripts/main.py`，由其检测本地 `jimeng` / `dreamina` CLI 是否可用：
    - 可用时真实调用 `dreamina text2video` 并轮询等待结果；
    - 不可用或调用失败时，自动生成本地占位视频文件。
  - Skill 会从脚本返回的 JSON 中读取 `video_path` 字段，并将该文件上传至飞书 Drive；
  - 写入 `ContentMatrix` 表的「视频素材(AI生成)」字段，字段类型为 Attachment，写入采用：
    - `[ {"file_token": "boxcn..."} ]`。

- **失败回退策略**：
  - 任一封面、视频、LLM 文案环节失败时，**仅影响当前记录对应字段**，不会中断整批任务；
  - 文字内容（通用字段 + 双平台字段 + 爆款逻辑拆解）仍然会按正常逻辑写入 `ContentMatrix`；
  - 具体成功数量会体现在 `generate_brand_content` 命令输出的 `asset_cover_douyin_uploaded` / `asset_cover_xhs_uploaded` / `asset_video_uploaded` 字段中（`asset_cover_ai_uploaded` 保留为两平台之和以兼容旧看板）。

## 飞书多维表格写入规范

本 Skill 严格遵循 `managing-lark-bitable-data` Skill 的数据写入规范（Write Shape）。所有字段的写入格式都经过 `bcma/bitable.py` 模块的封装，以确保数据一致性。

常见字段类型的 Write Shape 摘要如下（完整列表见 `inner_skills/managing-lark-bitable-data/references/record-fields.md`）：

| 字段类型 | Write Shape 示例 |
| --- | --- |
| 文本 | `"这是一段文本"` |
| 数字 | `123.45` |
| 单选 | `"选项A"` |
| 多选 | `["选项A", "选项B"]` |
| 日期时间 | `1674206443000` (毫秒时间戳) |
| 人员 | `[{"id": "ou_xxxx"}]` 或 `[{"email": "user@example.com"}]` |
| 附件 | `[{"file_token": "boxcn..."}]` |

**特别说明：**

- 所有与飞书多维表格的交互均通过 Agent Server RPC 完成，需要在调用时开启 `include_secrets=true` 以获取必要认证；
- 字段创建与更新严格遵循 `references/table-field-metadata.md` 与 `references/field-meta-*.md` 中的约定：
  - 文本字段使用 `type=1, ui_type="Text", property=null`；
  - 附件字段使用 `type=17, ui_type="Attachment", property=null`；
- `bcma/bitable.py` 中提供了 `ensure_field_exists`、`upload_attachment_file` 等辅助函数，所有自动化脚本应优先通过这些函数而非直接使用 CLI，以避免输出被格式化或截断。

## 并发与速率控制建议

- 上游 4R 打分：
  - 建议将 `concurrency.max_workers` 控制在 **8–16** 范围内，结合实际外部模型或接口限流情况进行调整；
  - 当环境中存在硬性 QPS 限制时，可适当降低线程数或增加 `backoff_factor` 以减少重试压力。

- 品牌入口 Top K 视觉资产补全：
  - 单次 `generate_brand_content` 默认仅对 Top 5 内容执行封面与视频生成，避免对图像/视频生成服务造成突发压力；
  - 如需同时对更多内容生成视觉资产，可通过调高 `downstream.brand_top_k_assets`，并根据生成服务额度适当串行化调用。

---

以上即为 `brand-content-marketing-advisor` v5.9.3 的整体设计与使用说明。典型接入顺序是：**（可选预飞）`check_schema` 做一次冷启动结构对齐 → `init_brand`（Step 1）判断品牌是否存在，不存在则 LLM 生成 7 维度并写入 → `init_audience`（Step 2）生成品牌人群画像 → `init_products`（Step 3）生成产品线 → `init_topic_rules`（Step 4）生成品牌 4R 策略 → `select_topic`（Step 5）从每日精选话题表按品牌 4R 选出 Top K 写入 TopicSelection → `generate_brand_content`（Step 6）围绕「话题 × 产品 × 人群 × 品牌」四要素生成双平台 LLM 文案 + 双平台 AI 封面 + AI 视频**，在品牌维度上实现从品牌知识底座自动建设，到话题筛选、双平台内容创作、双平台视觉资产的完整自动化流水线。每个业务命令都可追加 `--check-schema` 开关在主流程前显式做一轮结构同步。
