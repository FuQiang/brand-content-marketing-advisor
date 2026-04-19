"""Core package for brand-content-marketing-advisor skill.

提供 6 步独立 CLI 入口 + 表结构同步：
  Step 1  init_brand         — 判断品牌是否存在，不存在则 LLM 生成 7 维度并写入 Brand 表
  Step 2  init_audience      — LLM 生成品牌人群画像
  Step 3  init_products      — LLM 生成产品线 + 图库补全
  Step 4  init_topic_rules   — 内置骨架 + LLM 生成品牌 4R 策略
  Step 5  select_topic            — 每日精选话题品牌化筛选
  Step 6  generate_brand_content  — 双平台 LLM 文案 + 封面 + 视频

业务参数与表结构全部来自 config.yaml。
"""

from .authorize import SKILL_SCOPES, run_authorize
from .brand_pipeline import run_brand_content_pipeline
from .brand_setup import (
    load_existing_audience,
    run_init_brand,
    run_init_brand_transactional,
    run_step1_init_brand,
    run_step2_brand_audience,
    run_step3_products,
    run_step4_topic_rules,
)
from .bitable import (
    NoUserTokenError,
    check_write_permission,
    needs_user_authorization,
    preflight_check_tables,
)
from .card_sender import send_permission_error_card, send_step_card, send_step_card_to_current_user
from .config import load_config
from .daily_topics import run_brand_daily_selection
from .schema_sync import (
    DEFAULT_TABLE_KEYS as SCHEMA_SYNC_DEFAULT_TABLE_KEYS,
    sync_all_schemas,
    sync_table_schema,
)
from .tx import TxLog

__all__ = [
    "load_config",
    "run_init_brand",
    "run_init_brand_transactional",
    "run_step1_init_brand",
    "run_step2_brand_audience",
    "run_step3_products",
    "run_step4_topic_rules",
    "load_existing_audience",
    "run_brand_daily_selection",
    "run_brand_content_pipeline",
    "sync_all_schemas",
    "sync_table_schema",
    "SCHEMA_SYNC_DEFAULT_TABLE_KEYS",
    "check_write_permission",
    "preflight_check_tables",
    "NoUserTokenError",
    "needs_user_authorization",
    "run_authorize",
    "SKILL_SCOPES",
    "send_step_card",
    "send_step_card_to_current_user",
    "send_permission_error_card",
    "TxLog",
]
