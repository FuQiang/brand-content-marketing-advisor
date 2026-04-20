"""brand-content-marketing-advisor v5.9.3

本脚本作为 Skill 的主入口，提供 **6 步独立 CLI** + 一键全流程 + 1 个 ops 子命令：

  run_all                   — 一键执行 Step 1-6 全流程（推荐），每步完成发飞书卡片
  Step 1  init_brand        — 判断品牌是否存在，不存在则 LLM 生成 7 维度并写入 Brand 表
  Step 2  init_audience     — 基于 Brand 表 LLM 生成品牌人群画像（每人群一行）
  Step 3  init_products     — 基于 Brand 表 + 品牌人群表 LLM 生成产品线 + 图库补全
  Step 4  init_topic_rules  — 基于内置骨架 + Brand 表 + 品牌人群表生成品牌专属 4R 策略
  Step 5  select_topic     — 从每日精选话题表读取当日话题，按品牌 4R 打分 Top K 写入 TopicSelection
  Step 6  generate_brand_content — 基于 Top K 话题生成双平台 LLM 文案 + 封面 + 视频
  ops     check_schema      — 对 Skill 托管表做结构对齐

典型调用：
  python3 main.py run_all --brand "双汇"

所有表结构与参数均由 config.yaml 管理。
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any, Dict, List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

from bcma import (
    NoUserTokenError,
    SCHEMA_SYNC_DEFAULT_TABLE_KEYS,
    SKILL_SCOPES,
    check_write_permission,
    load_config,
    needs_user_authorization,
    preflight_check_tables,
    run_authorize,
    run_init_brand_transactional,
    run_step1_init_brand,
    run_step2_brand_audience,
    run_step3_products,
    run_step4_topic_rules,
    run_brand_daily_selection,
    run_brand_content_pipeline,
    send_permission_error_card,
    sync_all_schemas,
    send_step_card_to_current_user,
)


def _parse_schema_tables(raw: str | None) -> List[str] | None:
    """解析 `--schema-tables` 参数为 table key 列表；为空返回 None（走默认 5 张表）。"""
    if not raw:
        return None
    parts = [x.strip() for x in raw.split(",")]
    tables = [p for p in parts if p]
    return tables or None


def _add_schema_check_args(sub_parser: argparse.ArgumentParser) -> None:
    """为业务子命令挂载 `--check-schema` / `--schema-tables` 开关。"""
    sub_parser.add_argument(
        "--check-schema",
        dest="check_schema",
        action="store_true",
        default=False,
        help="在执行主命令前先做一轮表结构同步（创建缺失字段 + 安全清理空列）",
    )
    sub_parser.add_argument(
        "--schema-tables",
        dest="schema_tables",
        required=False,
        default=None,
        help=(
            "限定 --check-schema 生效的 table key 列表（逗号分隔），默认处理全部 5 张"
            " Skill 托管表；brands 表永远跳过"
        ),
    )


def _preflight_write_check(cfg, brand: str, table_keys: list) -> None:
    """对指定表做写入权限预检，失败则发卡片并抛错终止。

    两级检查：
      1) 是否有任何可用的 UAT 来源 —— 无则直接发"需要授权"卡，不再去空打 Bitable API。
      2) UAT 存在时，对目标表挨个做写入探测；任一 403 或业务级权限错误即终止。
    """
    from bcma.bitable import get_client  # 局部 import 避开循环依赖风险

    table_map = {}
    for key in table_keys:
        tbl_cfg = cfg.tables.get(key) or {}
        tbl_id = tbl_cfg.get("table_id", "")
        table_map[key] = tbl_id

    _log = logging.getLogger("bcma.main")
    _log.info("===== Step 0: 写入权限预检 [%s] =====", ", ".join(table_keys))

    # 1) 完全未授权态：直接发授权引导卡 + 抛错，不浪费 API quota
    if needs_user_authorization(cfg.app_token):
        token_type = get_client(cfg.app_token).get_token_type()  # "no_user_token"
        hint = (
            "尚未完成飞书用户授权。请在本技能目录运行 `python3 main.py authorize` 完成授权"
            "（只申请本技能所需的 13 个 scope，比 /feishu_auth 拉 100+ scope 更合理）。"
        )
        failures = {k: hint for k in table_map}
        _log.error("预检短路：%s (token_type=%s)", hint, token_type)
        send_permission_error_card(brand, failures, token_type=token_type)
        raise PermissionError(
            f"[{token_type}] {hint} 目标表: " + ", ".join(failures.keys())
        )

    # 2) 有 UAT：逐表做写入探测
    try:
        failures = preflight_check_tables(cfg.app_token, table_map)
    except NoUserTokenError as e:
        # 理论上 step 1 已经挡住了，这里兜底（例如 UAT 在这一刻刚过期）
        token_type = get_client(cfg.app_token).get_token_type()
        failures = {k: str(e) for k in table_map}
        send_permission_error_card(brand, failures, token_type=token_type)
        raise PermissionError(f"[{token_type}] {e}")

    if failures:
        token_type = get_client(cfg.app_token).get_token_type()
        _log.error("写入权限预检失败: %s (token_type=%s)", failures, token_type)
        send_permission_error_card(brand, failures, token_type=token_type)
        raise PermissionError(
            f"写入权限预检失败（{token_type}）。无法写入: "
            + ", ".join(failures.keys())
            + "。请在本技能目录运行 `python3 main.py authorize` 重新授权（13 个最小 scope）。"
        )
    _log.info("写入权限预检通过 ✓")


# 每个步骤需要写入的目标表
_STEP_WRITE_TABLES = {
    "init_brand": ["brands"],
    "init_audience": ["brand_audience"],
    "init_products": ["products"],
    "init_topic_rules": ["brand_topic_rules"],
    "select_topic": ["topic_selection"],
    "generate_brand_content": ["content_matrix"],
    "run_all": ["brands", "brand_audience", "products", "brand_topic_rules",
                "topic_selection", "content_matrix"],
}


def _maybe_run_schema_check(cfg, args) -> None:
    """若命令行传入 `--check-schema`，在进入主命令前做一轮 schema 同步并打印摘要。"""
    if not getattr(args, "check_schema", False):
        return
    tables = _parse_schema_tables(getattr(args, "schema_tables", None))
    logging.getLogger("bcma.main").info(
        "--check-schema 预飞：对 %s 做结构同步",
        tables or list(SCHEMA_SYNC_DEFAULT_TABLE_KEYS),
    )
    summary = sync_all_schemas(cfg, table_keys=tables)
    print("=== schema check preflight ===")
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    print("=== schema check preflight end ===")


def main() -> None:
    parser = argparse.ArgumentParser(description="brand-content-marketing-advisor v5.9.3 pipeline")
    parser.add_argument(
        "--config",
        dest="config_path",
        help="config.yaml path override",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── Step 1: init_brand ──
    p_s1 = subparsers.add_parser(
        "init_brand",
        help="Step 1: 判断品牌是否存在于 Brand 表；不存在则 LLM 生成 7 维度信息并写入",
    )
    p_s1.add_argument("--brand", required=True, help="品牌名称")
    _add_schema_check_args(p_s1)

    # ── Step 2: init_audience ──
    p_s2 = subparsers.add_parser(
        "init_audience",
        help="Step 2: 基于 Brand 表 LLM 生成品牌人群画像并写入 brand_audience 表",
    )
    p_s2.add_argument("--brand", required=True, help="品牌名称")
    _add_schema_check_args(p_s2)

    # ── Step 3: init_products ──
    p_s3 = subparsers.add_parser(
        "init_products",
        help="Step 3: 基于 Brand 表 + 品牌人群表 LLM 生成产品线 + 图库补全",
    )
    p_s3.add_argument("--brand", required=True, help="品牌名称")
    _add_schema_check_args(p_s3)

    # ── Step 4: init_topic_rules ──
    p_s4 = subparsers.add_parser(
        "init_topic_rules",
        help="Step 4: 基于内置骨架 + Brand 表 + 品牌人群表生成品牌专属 4R 策略",
    )
    p_s4.add_argument("--brand", required=True, help="品牌名称")
    _add_schema_check_args(p_s4)

    # ── Step 5: select_topic ──
    p_s5 = subparsers.add_parser(
        "select_topic",
        help="Step 5: 从每日精选话题表读取当日话题，按品牌 4R 打分选出 Top K 写入 TopicSelection",
    )
    p_s5.add_argument("--brand", required=True, help="品牌名称")
    p_s5.add_argument(
        "--top-k",
        dest="top_k",
        type=int,
        required=False,
        help="Top K 数量，默认读 config.daily_topics.top_k（默认 5）",
    )
    p_s5.add_argument(
        "--date",
        required=False,
        help="指定日期 YYYY-MM-DD，默认按北京时间当日",
    )
    p_s5.add_argument(
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="跳过 TopicSelection 去重，强制重写（用于手动重跑或修正 4R 打分）",
    )
    _add_schema_check_args(p_s5)

    # ── Step 6: generate_brand_content ──
    p_s6 = subparsers.add_parser(
        "generate_brand_content",
        help="Step 6: 基于 TopicSelection 中的品牌 Top K 话题，生成双平台文案 + 封面 + 视频",
    )
    p_s6.add_argument("--brand", required=True, help="品牌名称")
    _add_schema_check_args(p_s6)

    # ── run_all: Step 1-6 一键执行 ──
    p_all = subparsers.add_parser(
        "run_all",
        help="一键执行 Step 1-6 全流程：自动初始化品牌 + 话题选择 + 内容生成，每步完成发卡片",
    )
    p_all.add_argument("--brand", required=True, help="品牌名称")
    p_all.add_argument(
        "--top-k", dest="top_k", type=int, required=False,
        help="Step 5 Top K 数量，默认读 config",
    )
    p_all.add_argument(
        "--force",
        dest="force",
        action="store_true",
        default=False,
        help="Step 5 跳过 TopicSelection 去重，强制重写",
    )
    _add_schema_check_args(p_all)

    # ── ops: authorize ──
    p_auth = subparsers.add_parser(
        "authorize",
        help=(
            "走飞书 device-flow 授权并把 UAT 写入 .bcma.enc。"
            f"只申请本技能需要的 {len(SKILL_SCOPES)} 个 scope，"
            "避免 /feishu_auth 一次性拉全 100+ scope 的问题。"
        ),
    )

    # ── ops: check_schema ──
    p_schema = subparsers.add_parser(
        "check_schema",
        help="对 Skill 托管表做结构对齐：创建缺失字段 + 安全清理全空未登记列（brands 跳过）",
    )
    p_schema.add_argument(
        "--tables",
        dest="schema_tables",
        required=False,
        default=None,
        help=(
            "限定要处理的 table key 列表（逗号分隔），默认处理全部 5 张 Skill 托管表；"
            "brands 表始终跳过"
        ),
    )

    args = parser.parse_args()

    # authorize 子命令不走 Bitable 路径，跳过 load_config 的 app_token 校验 +
    # ensure_provisioned（后者需要 UAT，这里正是要获取 UAT）
    if args.command == "authorize":
        result = run_authorize()
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    cfg = load_config(args.config_path)

    # 自动建表：首次运行创建 Bitable + 表并回写 config.yaml，之后永远复用
    cfg.ensure_provisioned()

    if args.command == "init_brand":
        _maybe_run_schema_check(cfg, args)
        _preflight_write_check(cfg, args.brand, _STEP_WRITE_TABLES["init_brand"])
        result = run_step1_init_brand(cfg, brand=args.brand)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        send_step_card_to_current_user(1, "init_brand", args.brand, result)

    elif args.command == "init_audience":
        _maybe_run_schema_check(cfg, args)
        _preflight_write_check(cfg, args.brand, _STEP_WRITE_TABLES["init_audience"])
        result = run_step2_brand_audience(cfg, brand=args.brand)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        send_step_card_to_current_user(2, "init_audience", args.brand, result)

    elif args.command == "init_products":
        _maybe_run_schema_check(cfg, args)
        _preflight_write_check(cfg, args.brand, _STEP_WRITE_TABLES["init_products"])
        result = run_step3_products(cfg, brand=args.brand)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        send_step_card_to_current_user(3, "init_products", args.brand, result)

    elif args.command == "init_topic_rules":
        _maybe_run_schema_check(cfg, args)
        _preflight_write_check(cfg, args.brand, _STEP_WRITE_TABLES["init_topic_rules"])
        result = run_step4_topic_rules(cfg, brand=args.brand)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        send_step_card_to_current_user(4, "init_topic_rules", args.brand, result)

    elif args.command == "select_topic":
        _maybe_run_schema_check(cfg, args)
        _preflight_write_check(cfg, args.brand, _STEP_WRITE_TABLES["select_topic"])
        result = run_brand_daily_selection(
            cfg,
            brand=args.brand,
            top_k=getattr(args, "top_k", None),
            date=getattr(args, "date", None),
            force=getattr(args, "force", False),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        send_step_card_to_current_user(5, "select_topic", args.brand, result)

    elif args.command == "generate_brand_content":
        _maybe_run_schema_check(cfg, args)
        _preflight_write_check(cfg, args.brand, _STEP_WRITE_TABLES["generate_brand_content"])
        result = run_brand_content_pipeline(cfg, brand=args.brand)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        send_step_card_to_current_user(6, "generate_brand_content", args.brand, result)

    elif args.command == "run_all":
        _maybe_run_schema_check(cfg, args)
        brand = args.brand

        # Step 0: 写入权限预检 — 对全部 6 张表做一次探测，失败立即终止
        _preflight_write_check(cfg, brand, _STEP_WRITE_TABLES["run_all"])

        logger = logging.getLogger("bcma.main")

        # Step 0.5: 自动 schema 对齐 — run_all 承诺"首次运行自动建表 + 无手工前置"，
        # 旧版本上线新字段时若不在这里自动建列，Step 3 写 Products 会 FieldNameNotFound
        # 全部失败，Step 6 再跨品牌捞别家 SKU 做文案。brands 表被 HARD_SKIP_TABLE_KEYS
        # 硬跳过；记录数 < 3 时 schema_sync 也会跳过删除阶段——只会幂等地补齐缺失字段。
        logger.info("===== run_all Step 0.5: 自动 schema 对齐 =====")
        try:
            schema_summary = sync_all_schemas(cfg)
            created = schema_summary.get("created_fields", {}) or {}
            if any(v for v in created.values()):
                logger.info("schema 对齐新建字段: %s", {k: [x.get("field_name") for x in v] for k, v in created.items() if v})
            else:
                logger.info("schema 对齐完成，所有托管表字段齐全，无新建")
            errs = schema_summary.get("errors", {}) or {}
            if errs:
                logger.warning("schema 对齐存在表级错误: %s", errs)
        except Exception as e:
            logger.warning("schema 对齐失败（继续后续步骤）: %s", e, exc_info=True)
        step_errors: Dict[int, str] = {}

        def _run_step(step_num: int, step_name: str, func, *a, **kw) -> Any:
            """执行单步并捕获异常，失败时记录错误并发卡片通知，返回 None。"""
            logger.info("===== run_all Step %d/6: %s '%s' =====", step_num, step_name, brand)
            try:
                result = func(*a, **kw)
                print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
                send_step_card_to_current_user(step_num, step_name, brand, result)
                return result
            except Exception as e:
                err_msg = f"Step {step_num} {step_name} 失败: {e}"
                logger.error(err_msg, exc_info=True)
                step_errors[step_num] = err_msg
                err_result = {"step": step_num, "error": str(e), "status": "failed"}
                send_step_card_to_current_user(step_num, step_name, brand, err_result)
                return None

        # Step 1-4: 事务化执行——任一步失败即回滚前面已写入的所有数据
        logger.info(
            "===== run_all Step 1-4 (transactional): '%s' =====", brand,
        )
        tx_ok = False
        try:
            tx_result = run_init_brand_transactional(cfg, brand=brand)
            tx_ok = True
            # 逐步发卡片（回放事务内部结果）
            for step_num, key, step_name in [
                (1, "step1_brand", "init_brand"),
                (2, "step2_audience", "init_audience"),
                (3, "step3_products", "init_products"),
                (4, "step4_topic_rules", "init_topic_rules"),
            ]:
                step_result = tx_result.get(key)
                if step_result is not None:
                    print(json.dumps(step_result, ensure_ascii=False, indent=2, default=str))
                    send_step_card_to_current_user(step_num, step_name, brand, step_result)
            logger.info(
                "===== Step 1-4 事务提交成功，共 %d 条写入 =====",
                tx_result.get("tx_ops", 0),
            )
        except Exception as e:
            rollback_summary = getattr(e, "_tx_rollback_summary", None)
            err_msg = f"Step 1-4 事务失败并已回滚: {e}"
            logger.error(err_msg, exc_info=True)
            # 所有四步统一记作失败
            for step_num, step_name in [
                (1, "init_brand"), (2, "init_audience"),
                (3, "init_products"), (4, "init_topic_rules"),
            ]:
                step_errors[step_num] = err_msg
            err_card = {
                "step": "1-4 (transactional)",
                "error": str(e),
                "status": "rolled_back",
                "rollback": rollback_summary,
            }
            # 发一张汇总卡片，避免 4 张重复失败卡刷屏
            send_step_card_to_current_user(1, "init_brand (tx rollback)", brand, err_card)

        # Step 5: select_topic —— 事务失败时跳过，不基于残缺数据做后续
        if tx_ok:
            result5 = _run_step(
                5, "select_topic", run_brand_daily_selection,
                cfg, brand=brand,
                top_k=getattr(args, "top_k", None),
                force=getattr(args, "force", False),
            )
        else:
            logger.warning("跳过 Step 5: Step 1-4 事务失败已回滚")
            step_errors[5] = "跳过: Step 1-4 事务失败"
            result5 = None

        # Step 6: generate_brand_content (依赖 Step 5 写入的 TopicSelection)
        result6 = None
        if result5 is not None and result5.get("written_count", 0) > 0:
            result6 = _run_step(6, "generate_brand_content", run_brand_content_pipeline, cfg, brand=brand)
        elif result5 is not None:
            logger.warning("跳过 Step 6: Step 5 未写入任何话题")
            step_errors[6] = "跳过: Step 5 未写入话题"
        else:
            logger.warning("跳过 Step 6: Step 5 失败")
            step_errors[6] = "跳过: Step 5 失败"

        if step_errors:
            logger.warning("===== run_all 完成（有错误）'%s': %s =====", brand, step_errors)
        else:
            logger.info("===== run_all 全流程完成 '%s' =====", brand)

    elif args.command == "check_schema":
        tables = _parse_schema_tables(getattr(args, "schema_tables", None))
        summary = sync_all_schemas(cfg, table_keys=tables)
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
