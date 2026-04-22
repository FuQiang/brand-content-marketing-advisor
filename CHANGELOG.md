
# v6.0.0
- **refactor(Architecture)**: 彻底抛弃 Python 业务胶水,改为 **SKILL.md-driven 主剧本 + references/ 执行手册**。6 步流水线(品牌 7 维 → 人群 → 产品 → 4R 策略 → 每日选题 → 双平台内容)由 Claude Agent 按 SKILL.md 直接编排,所有数据读写走 `feishu_bitable_*` / `feishu_drive_*` 官方 MCP 工具,不再经 `bcma.bitable` / `bcma.llm_client` / `bcma.brand_pipeline` 等 Python 中间层。
- **refactor(LLM)**: 所有 LLM 调用由 Claude Agent 直接完成,删除 `bcma/llm_client.py` 及 Anthropic → AIME → 豆包 的多层降级。prompts、4R rubric、产品真实性红线、双平台文案模板全部从 Python 抽出,落到 `references/step-{1..6}-*.md` 与 `references/bitable-write-shapes.md` / `references/cross-base-topics.md` 8 份执行手册。
- **refactor(Config)**: `config.yaml` 删除 `concurrency` / `model` 两段(v5.x 仅供旧 main.py 兼容),只保留 `app.app_token` / `tables.*` / `fields.*` / `scoring.{threshold_main,threshold_candidate,r4_veto_threshold}` / `regex_filters` / `downstream.*` / `daily_topics.*`。
- **remove(Python Glue)**: 删除 `main.py` / `backfill_assets.py` / `config.yaml.template` / `test_topics.csv`,以及 `bcma/` 下 `authorize.py` / `bitable*.py` / `brand_pipeline.py` / `brand_setup.py` / `card_sender.py` / `config.py` / `copywriting.py` / `daily_topics.py` / `downstream.py` / `llm_client.py` / `product_assets.py` / `schema_sync.py` / `scoring.py` / `tx.py` / `upstream.py` / `utils.py` / `__init__.py`。
- **keep(Leaf CLIs)**: `bcma/` 仅保留两个叶子工具 —— `dreamina_cli.py`(dreamina CLI 适配层,Step 3 图库/Step 6 封面+视频)与 `image_search.py`(DuckDuckGo + Bing 图片搜索,Step 3 真实产品图),均由 SKILL.md 通过 Bash 调用,不参与 Claude 会话编排。
- **feat(Auto-Auth)**: 飞书授权全量交给 openclaw-lark auto-auth —— 首次调用缺 scope 时自动弹 OAuth 卡片,只申请本次调用所需 scope(如 `base:record:create`),用户点同意后续运行零打扰。删除 v5.x 的 `bcma/authorize.py` 显式批量授权流程。**不申请** `im:message.send_as_user` 等敏感 scope —— 阶段性产出文本由 Agent 直接输出,openclaw 层用 bot 身份 reply 回触发会话。
- **feat(Silent Execution)**: 触发后 6 步跑到底,中途不请示。每步完成在 Agent 文本输出里写一小段 `emoji + Step X/6 完成 · ...` 阶段性产出;失败才额外输出 `❌ Step X/6 失败` 摘要 + 下一步建议。数据只进多维表格,不写 `workspace/*.md` / `workspace/*.json`。
- **docs(SKILL.md)**: 从 v5.7.0 的 description + 技术栈清单 → v6.0.0 的完整 6 步主剧本 + 触发条件 + 执行原则 + 阶段性产出模板 + 错误处理表 + 授权说明 + 保留 CLI 说明。

# v5.7.0
- **feat(Schema Sync Module)**: 新增 `bcma/schema_sync.py` 模块，提供对 Skill 托管表的**可重入、幂等、安全**的结构对齐能力：
  - `sync_table_schema(cfg, table_key)` — 单表同步：基于 `config.fields[table]` 登记映射与 `list_table_fields` 做 diff，对缺失字段按 `_infer_field_type` 推断 (type_code, ui_type) 调 `ensure_field_exists` 创建；对未登记且整列全空的非保护列走 `delete_field_if_exists` 清理。
  - `sync_all_schemas(cfg, table_keys=None)` — 批量同步：默认处理 `brand_audience` / `products` / `brand_topic_rules` / `topic_selection` / `content_matrix` 五张 Skill 托管表；per-table 独立容错，任一表失败只影响当前表，错误记入 `summary.errors[table_key]`。
  - **硬跳过 brands 表**：`HARD_SKIP_TABLE_KEYS = {"brands"}`，即使用户通过 `--schema-tables brands` 显式传入也会被跳过并记入 `tables_skipped`，brands 永远是人工维护的品牌知识库底座。
  - **安全删除四条件**：要同时满足「Bitable 有 ∧ config 未登记 ∧ 非系统保护列（记录ID / 创建时间 / 修改时间 / 创建人 / 修改人）∧ 整列全空（None / 空字符串 / 空列表 / 空字典）」才删除；并设 `MIN_RECORDS_FOR_CLEANUP = 3` 样本量门槛，记录数不足时跳过整个删除阶段以避免冷启动或数据异常时误删用户手动加的自定义列。
  - **单次全表扫描**：`_scan_non_empty_columns` 一次分页扫完整表，在内存中判定所有字段的非空状态，避免逐字段扫表；分页参数 `page_size=200`。
  - **类型推断分层兜底**：按字段 key 命中 `_ATTACHMENT_KEY_HINTS`（`asset_gallery_field` / `cover_image_ai_field` / `douyin_cover` / `xhs_cover` / `video_asset_ai`）→ Attachment 17；`_NUMBER_KEY_HINTS`（`r1..r4` / `total_score` / `base_weight`）→ Number 2；`_DATETIME_KEY_HINTS`（`created_at` / `fetched_at` / `generated_at`）→ DateTime 5；`_MULTISELECT_KEY_HINTS`（`audience` / `persona_tags` / `functions` / `platforms`）→ MultiSelect 4；其余一律兜底 Text 1。
- **feat(--check-schema 预飞开关)**: `main.py` 六个业务子命令（`init_brand` / `run_upstream` / `run_downstream` / `run_full_pipeline` / `run_brand` / `run_brand_content`）统一新增：
  - `--check-schema` — 布尔开关，默认 False；显式传入时在主命令执行前调 `sync_all_schemas` 做一轮预飞结构同步，摘要以 `=== schema check preflight ===` 分隔块打印在 stdout。
  - `--schema-tables a,b,c` — 逗号分隔的 table key 子集，限定预飞生效范围，不传默认处理全部 5 张 Skill 托管表。
  - 通过 `_add_schema_check_args` 工厂函数挂载，`_maybe_run_schema_check` 负责预飞触发与摘要打印 —— **未传入 `--check-schema` 时行为完全与 v5.6.0 一致，Skill 绝不会在用户未明确要求时动表结构**。
- **feat(check_schema 独立子命令)**: `main.py` 新增 `check_schema` 子命令作为纯 ops 用途的独立入口：不驱动任何业务流程，只做一次结构巡检，支持 `--tables` 限定子集，直接输出同步摘要 JSON。适用于上线前冷启动校验、表结构漂移巡检、升级版本后的预防性对齐。
- **feat(Package Exports)**: `bcma/__init__.py` 新增导出 `sync_all_schemas` / `sync_table_schema` / `SCHEMA_SYNC_DEFAULT_TABLE_KEYS` 三个公共 API，便于其他自动化脚本直接 `from bcma import sync_all_schemas` 调用。
- **docs(SKILL.md + main.py)**: 版本字符串与 description 全量从 v5.6.0 升级到 v5.7.0；SKILL.md 新增「7. 表结构同步」章节，覆盖两种触发方式、同步规则、输出摘要 JSON 示例；docstring 和目录结构章节同步更新指向 `bcma/schema_sync.py`。

# v5.6.0
- **feat(Step 6 双平台 LLM 文案)**: 新增 `bcma/copywriting.py` 模块，第六步 `run_brand_content` 的文案从 v5.5.0 的确定性模板切换为基于品牌上下文的 **LLM 双平台生成**：
  - `BrandContext` + `load_brand_context` — 从 `brand_audience` 表（audience / persona_tags / motivation / content_preference / persona_description 五字段拼接）+ `BrandTopicRules` 三段 scoring sections 构造品牌上下文 prompt block。
  - `_DOUYIN_SCRIPT_PROMPT` — 抖音短视频脚本 JSON 输出：`hook_title`（10–20 字首帧钩子）+ 5 镜头（`camera/action/subtitle`）+ `cta` + `visual_direction`；硬性要求「情绪开场 → 产品出场 → 对比切换 → 价值锤击 → 行动召唤」节奏，字幕必须独立可读，严守品牌排斥人群 / 冲突人设美学红线。
  - `_XHS_NOTE_PROMPT` — 小红书种草笔记 JSON 输出：`title`（20 字以内 + emoji）+ `body`（**严格 800–1200 字**第一人称，5 段结构：场景还原 / 真实痛点 / 使用体验 / 购买理由 / 适配人群）+ `tags`（3–6 个多维度）+ `visual_direction`。
  - `_call_llm_json` — Anthropic → AIME 双路降级，沿用 `scoring.py` 模式；解析失败或 body < 400 字时落到 `_fallback_douyin` / `_fallback_xhs` 模板兜底，保证流水线不断。
  - `generate_dual_platform_copy` — 主入口，返回 `{douyin, xhs, brand_context_used}` 结构化结果。
- **feat(ContentMatrix 双平台字段)**: `config.yaml` 的 `fields.content_matrix` 新增：
  - 文案列：`douyin_script`（抖音短视频脚本）/ `xhs_title`（小红书标题）/ `xhs_note`（小红书种草笔记 = 正文 + tags 拼接）；
  - 封面列：`douyin_cover`（抖音封面 9:16）/ `xhs_cover`（小红书封面 3:4）；
  - 关系追踪列（`topic` / `brand` / `audience` / `products` / `platforms`）保留不变，且 v5.6.0 起 `generate_content_items` 显式回填「适用品牌」列；
  - 通用列 `hook` / `body` / `visuals` / `logic_breakdown` 继续回填（镜像自抖音输出），向后兼容 v5.5.0 及以前看板；
  - 旧 `cover_image_ai_field` 保留并回填抖音 9:16 封面内容。
- **feat(双平台 AI 封面生成)**: `bcma/brand_pipeline.py` 原单张 AI 封面 `_generate_ai_cover_for_item` 替换为 `_generate_dual_covers_for_item`：
  - 基于同一张产品图库底图（从 Products 表 `asset_gallery_field` 抽取），分别调用 `_run_image_edit` 两次，得到 9:16 抖音封面与 3:4 小红书封面；
  - 新增 `_build_douyin_cover_prompt`（首帧大字钩子 / 冷调强对比 / 避开信息流安全区）与 `_build_xhs_cover_prompt`（emoji 标题 / 真实生活质感 / 人物主体占比 50–65% / 暖色调自然光）；
  - `_run_image_edit` 新增 `aspect_ratio` 参数，默认 `9:16`，`3:4` 用于小红书；
  - 合规红线：两张封面都必须严格保留原图服装款式 / 版型结构 / 面料细节 / 品牌 Logo，只改背景 / 光影 / 色调 / 构图 / 文字排版；
  - 任一平台封面失败不影响另一平台，也不影响视频生成。
- **feat(下游入口双平台化)**: `bcma/downstream.py::_build_copywriting` 改为调用 `generate_dual_platform_copy`，返回 dict 包含通用字段（hook/body/visual/logic_breakdown）与双平台字段（douyin_script/douyin_hook/douyin_visual/xhs_title/xhs_note/xhs_body/xhs_visual）。`generate_content_items` 新增 `brand` 可选参数，写入 ContentMatrix 时同时回填通用列 + 双平台列 + 品牌列。`_ensure_content_matrix_fields` 扩展为一次性确保 9 个列存在（含新的双平台 + 旧的兼容列）。
- **feat(run_brand_content 输出)**: 返回 JSON 新增 `asset_cover_douyin_uploaded` / `asset_cover_xhs_uploaded` 双平台统计；`asset_cover_ai_uploaded` 保留为两平台之和，向后兼容 v5.5.0 看板。

# v5.5.0
- **feat(Step 5 重构)**: 第五步 `run_brand` 重构为"每日精选话题筛选 + 品牌 4R 打分 + Top K 写入 TopicSelection"，不再耦合文案/封面/视频生成。新增 `bcma/daily_topics.py` 模块：
  - `_today_window_ms` — 按 `daily_topics.timezone_offset_hours`（默认北京时间 UTC+8）计算当日 00:00 ~ 次日 00:00 的毫秒时间戳区间，支持 `--date YYYY-MM-DD` 覆盖。
  - `_fetch_today_daily_topics` — 跨 base 读取外部「每日精选话题」表（`daily_topics.app_token` / `table_id` / `view_id`），按配置的 `fields.created_at` 字段筛当日记录；数值/ISO 字符串/自动 `created_time` 字段三种时间格式自动兜底。
  - `_score_candidates_concurrently` — 复用 `upstream.py::load_brand_rules_prompt` + `scoring.py::compute_4r_score_with_model`，以 `BrandTopicRules` 中的品牌专属 4R prompt（只含「品牌底层认知 / 4R 筛选法则 / 人设审美一致性校验」三段）并发打分，线程数走 `concurrency.max_workers`。每日精选话题视为预清洗数据，直接跳过本地正则黑白名单。
  - `_load_existing_topic_names_today` + `_write_top_k` — 按 (当日, 品牌, 话题名称) 三元组去重，查 TopicSelection 中当日该品牌已有话题名集合，写入 Top K 时跳过重名，保证同一天重复跑 `run_brand` 幂等。
- **feat(Step 6 新增)**: 第六步 `run_brand_content`：基于 TopicSelection 中的品牌 Top K 话题，生成文案矩阵 + Top K 视觉资产。由原 `run_brand_pipeline` 重命名为 `run_brand_content_pipeline`，函数体与逻辑保持不变（产品图库补全 / TopicSelection 品牌时间窗加载 / 文案生成 / 封面视频附件补全），仅语义上从"一键全链路"拆分为"内容生产"专职步骤。
- **feat(CLI)**: `main.py` 新增 `run_brand_content` 子命令；`run_brand` 子命令新增 `--top-k` 与 `--date` 两个可选参数。
- **feat(Config)**: `config.yaml` 新增 `daily_topics` 段（`app_token` / `table_id` / `view_id` / `timezone_offset_hours` / `top_k` / `fields` 映射），默认字段名与 TopicSelection 表保持一致（话题名称 / 来源 / 入库时间 / 原始文本）。跨 base 读取通过 `bitable.search_all_records(app_token=...)` 参数天然支持。
- **refactor(Package Exports)**: `bcma/__init__.py` 导出新入口 `run_brand_daily_selection` 与 `run_brand_content_pipeline`，同时从 `__all__` 移除旧的 `run_brand_pipeline` 别名。

# v5.4.0
- **feat(Brand Table)**: 引入独立的 **Brand 表**（人工维护的品牌基础信息知识库），包含 7 个维度：品牌名称 / 品类与价格带 / 核心差异化 / 最大竞品 / 排斥人群 / 适配人设/美学 / 冲突人设/美学 / 高价值场景。该表完全由用户在飞书多维表格中手动创建与维护，Skill 只读加载，绝不写入或改写。
- **feat(Rename)**: 原 `brands` 表重命名为 `brand_audience`（品牌人群画像表），`table_id` 保持不变以兼容历史数据。`config.yaml` 的 `tables.brands` / `fields.brands` 改为指向新的人工 Brand 表。
- **feat(init_brand 4-step)**: `bcma/brand_setup.py` 彻底重构为 4 步流水线：
  - `Step 1 _step1_load_brand` — 从 Brand 表只读加载 7 维度并严格校验非空，任一维度缺失即 `ValueError` 中断。
  - `Step 2 _step2_init_brand_audience` — 基于 Brand 表 LLM 生成品牌人群表，通过 `_validate_brand_audience` 严格校验字段非空 + 最小长度（persona_description ≥ 150 字等），失败自动重试（最多 2 次），已有完整记录直接复用。
  - `Step 3 _step3_populate_products` — 基于 Brand 表（category_price / differentiation / high_value_scenes）+ 品牌人群上下文 LLM 生成 Products 产品线，并补全真实图库。
  - `Step 4 _step4_generate_topic_rules` — 使用内置 `_TOPIC_RULES_SKELETON` 骨架 + `_STEP4_GEN_PROMPT` 填充指引，LLM 基于 Brand 表 + 品牌人群表内容填充所有 `<<>>` 槽位，生成品牌专属 4R 话题筛选策略并 upsert 到 BrandTopicRules 表。
- **feat(Built-in Skeleton)**: 新增 `_TOPIC_RULES_SKELETON` 常量（结构完整的 Role / 品牌底层认知 / 4R 筛选法则 / 人设审美一致性校验 / 反漏斗机制 / 输出格式 6 段话题策略模板），Step 4 不再从 Bitable 读取任何旧模板，生成失败直接抛 `RuntimeError` 中断（**移除 `_DEFAULT_TEMPLATE` 兜底**）。
- **feat(rules_prompt Validation)**: `_validate_rules_prompt` 严格校验生成结果：无 `<<>>` 残留 / 必要章节标题齐全 / 决策阈值 ≥ 16 主推 + R4 ≤ 2 PASS 逐字保留 / 长度 ≥ 1500 字，任一不满足立即重试或抛错。
- **feat(Scoring Section Extraction)**: `bcma/upstream.py` 新增 `extract_scoring_sections()` 函数，`load_brand_rules_prompt` 在返回给 per-topic JSON 打分前自动截断「# 反漏斗机制」和「# 输出格式」章节，只保留「品牌底层认知 + 4R 筛选法则 + 人设审美一致性校验」三段，避免 LLM 在表格批量输出格式和 JSON 打分格式之间混乱。完整 prompt 在 BrandTopicRules 表中原样保留，供周会手工批量筛选使用。
- **refactor(Config)**: `config.yaml` 重组表配置：`tables.brands` 指向新的人工维护表（`table_id` 留空，需用户填入），新增 `tables.brand_audience` 承接原 `brands` 的 table_id。`fields.brands` 改为 7 维度字段映射，新增 `fields.brand_audience` 映射原 6 字段。

# v5.3.0
- **feat(Brand Setup)**: 新增 `init_brand` 一键品牌数据建设流水线（`bcma/brand_setup.py`），依次执行 Step 1 写入 Brands 表 + LLM 生成人群画像、Step 2 LLM 生成产品线 + 补全产品图库、Step 3 基于 BrandTopicRules 表模板改写品牌专属 4R 筛选 prompt 并回写。
- **feat(Tables)**: `config.yaml` 新增 `brands` 与 `brand_topic_rules` 两张表的配置（table_id + 字段映射），ContentMatrix 新增 `source_topic_id` / `idempotent_key` 字段。
- **feat(4R Scoring)**: 4R 打分体系从百分制重构为 **1–5 分制**，总分 = R1+R2+R3+R4（满分 20）。决策阈值：总分 ≥16 且 R4 ≥4 → ✅ 主推；13–15 且 R4 ≥3 → 🟡 备选。新增 **R4 否决机制**：R4 ≤ `r4_veto_threshold`（默认 2）时无论总分一律 PASS。
- **feat(Brand-aware Scoring)**: `run_upstream` 新增 `--brand` 参数，传入品牌名时会从 BrandTopicRules 表读取该品牌的专属 4R prompt 注入 LLM 打分；未提供或查无记录时走通用 prompt。LLM 响应多带 `one_line_reason` 与 `content_direction` 两个字段，回写 TopicSelection。
- **feat(Config)**: `app_token` 默认从环境变量 `BCMA_APP_TOKEN` 读取，未配置时显式 `ValueError`；`config.yaml` 中不再硬编码敏感凭证。`model.candidates` 修正为 `claude-opus-4-6` / `claude-sonnet-4-6` 合法模型名。
- **refactor(Downstream)**: 私有 helper `_load_topics_from_bitable` / `_ensure_persona_for_topics` / `_load_products` 去下划线公开化，供 `brand_setup` 等模块复用。
- **perf(ContentMatrix)**: 新增 `_ensure_content_matrix_fields()`，把原先循环内重复调用的字段确保逻辑提到循环外一次完成；写入前按「匹配话题」名去重，保证 `run_downstream` 多次运行的幂等性。
- **perf(Products Cleanup)**: 空列清理重写为**单次全表扫描 + 内存判定**（原实现对每个字段单独全表扫描），并新增 `MIN_RECORDS_FOR_CLEANUP=3` 安全阈值防止在数据异常时误删。
- **refactor(Logging)**: 上游/下游/品牌 pipeline 的异常分支全部改用 `logger.warning` 记录上下文，替代原先的静默 `pass`；正则过滤新增 `_safe_search` 包装，坏规则不再炸批次。
- **fix(Brand Pipeline)**: 修复 `_extract_cover_tokens` 中 `tokens` 累加导致的 unreachable return 路径，改为命中第一张图即返回。

# v5.2.0
- **feat(AI Cover Generation)**: Integrated AI-powered cover generation using `image-generate` skill's editing capabilities. The system now creates a "视频封面(AI生成)" by overlaying the marketing hook (爆款标题) onto a real product image from the Products table.
- **feat(Compliance)**: The AI cover generation process strictly adheres to compliance by only using real product photos as a base. It stylizes the background and adds text but does not alter the clothing's structure, details, or logo, ensuring the final image remains true to the actual product.
- **feat(Config)**: Added `cover_image_ai_field` to `config.yaml` to specify the new attachment field "视频封面(AI生成)" in the ContentMatrix table.
- **feat(Pipeline)**: Introduced a new stage in the brand pipeline to handle AI cover creation. This stage fetches a real product image, generates a compliant AI-edited cover with the marketing title, and uploads it to the "视频封面(AI生成)" field. This feature coexists with the existing "视频封面(真实大片)" logic.
- **fix(Error Handling)**: Enhanced error handling ensures that any failure during the AI cover generation for a specific record will be logged and skipped, allowing the batch process to continue without interruption.
