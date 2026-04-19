import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


CONFIG_PATH_DEFAULT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yaml")

logger = logging.getLogger("bcma.config")

# ── 建表 schema（与 setup 一致）─────────────────────────────────────
FT_TEXT = 1
FT_NUMBER = 2
FT_MULTISELECT = 4
FT_ATTACHMENT = 17

_TABLE_SCHEMAS: Dict[str, List[Dict[str, Any]]] = {
    "brands": [
        {"field_name": "品牌名称", "type": FT_TEXT},
        {"field_name": "品类与价格带", "type": FT_TEXT},
        {"field_name": "核心差异化", "type": FT_TEXT},
        {"field_name": "最大竞品", "type": FT_TEXT},
        {"field_name": "排斥人群", "type": FT_TEXT},
        {"field_name": "适配人设/美学", "type": FT_TEXT},
        {"field_name": "冲突人设/美学", "type": FT_TEXT},
        {"field_name": "高价值场景", "type": FT_TEXT},
    ],
    "brand_audience": [
        {"field_name": "品牌名称", "type": FT_TEXT},
        {"field_name": "典型人群受众", "type": FT_MULTISELECT},
        {"field_name": "画像标签", "type": FT_MULTISELECT},
        {"field_name": "消费动机", "type": FT_TEXT},
        {"field_name": "内容偏好", "type": FT_TEXT},
        {"field_name": "人群描述", "type": FT_TEXT},
    ],
    "products": [
        {"field_name": "所属品牌", "type": FT_TEXT},
        {"field_name": "产品系列", "type": FT_TEXT},
        {"field_name": "产品名称", "type": FT_TEXT},
        {"field_name": "产品卖点", "type": FT_TEXT},
        {"field_name": "卖点详细阐述", "type": FT_TEXT},
        {"field_name": "目标人群标签", "type": FT_MULTISELECT},
        {"field_name": "人群痛点", "type": FT_TEXT},
        {"field_name": "季节", "type": FT_TEXT},
        {"field_name": "价格带", "type": FT_TEXT},
        {"field_name": "材质", "type": FT_TEXT},
        {"field_name": "功能点", "type": FT_MULTISELECT},
        {"field_name": "基础权重", "type": FT_NUMBER},
    ],
    "brand_topic_rules": [
        {"field_name": "品牌名称", "type": FT_TEXT},
        {"field_name": "话题筛选及评估逻辑", "type": FT_TEXT},
    ],
    "topic_selection": [
        {"field_name": "话题名称", "type": FT_TEXT},
        {"field_name": "适用品牌", "type": FT_TEXT},
        {"field_name": "适用人群", "type": FT_MULTISELECT},
        {"field_name": "R1 相关度", "type": FT_NUMBER},
        {"field_name": "R2 场景力", "type": FT_NUMBER},
        {"field_name": "R3 流量趋势", "type": FT_NUMBER},
        {"field_name": "R4 舆情风险", "type": FT_NUMBER},
        {"field_name": "总分", "type": FT_NUMBER},
        {"field_name": "决策结果", "type": FT_TEXT},
        {"field_name": "一句话理由", "type": FT_TEXT},
        {"field_name": "内容方向建议", "type": FT_TEXT},
        {"field_name": "入库时间", "type": FT_TEXT},
        {"field_name": "来源", "type": FT_TEXT},
        {"field_name": "抓取时间", "type": FT_TEXT},
        {"field_name": "规则命中说明", "type": FT_TEXT},
        {"field_name": "原始文本", "type": FT_TEXT},
    ],
    "content_matrix": [
        {"field_name": "匹配话题", "type": FT_TEXT},
        {"field_name": "适用品牌", "type": FT_TEXT},
        {"field_name": "目标人群", "type": FT_MULTISELECT},
        {"field_name": "主推产品", "type": FT_TEXT},
        {"field_name": "适用平台", "type": FT_MULTISELECT},
        {"field_name": "爆款标题/钩子", "type": FT_TEXT},
        {"field_name": "正文与脚本", "type": FT_TEXT},
        {"field_name": "视觉画面建议", "type": FT_TEXT},
        {"field_name": "爆款逻辑拆解(为什么会火)", "type": FT_TEXT},
        {"field_name": "抖音短视频脚本", "type": FT_TEXT},
        {"field_name": "小红书标题", "type": FT_TEXT},
        {"field_name": "小红书种草笔记", "type": FT_TEXT},
        {"field_name": "抖音封面(9:16)", "type": FT_ATTACHMENT},
        {"field_name": "小红书封面(3:4)", "type": FT_ATTACHMENT},
        {"field_name": "视频封面(AI生成)", "type": FT_ATTACHMENT},
        {"field_name": "视频素材(AI生成)", "type": FT_ATTACHMENT},
        {"field_name": "生成时间", "type": FT_TEXT},
        {"field_name": "来源话题ID", "type": FT_TEXT},
        {"field_name": "幂等键", "type": FT_TEXT},
    ],
}

ALL_TABLE_KEYS = list(_TABLE_SCHEMAS.keys())


@dataclass
class Config:
    raw: Dict[str, Any]
    _config_path: str = ""

    @property
    def app_token(self) -> str:
        token = os.environ.get("BCMA_APP_TOKEN") or self.raw.get("app", {}).get("app_token", "")
        if not token:
            raise ValueError(
                "app_token 未配置。请设置环境变量 BCMA_APP_TOKEN 或在 config.yaml 的 app.app_token 中填写。"
            )
        return token

    @property
    def tables(self) -> Dict[str, Any]:
        return self.raw["tables"]

    @property
    def fields(self) -> Dict[str, Any]:
        return self.raw["fields"]

    @property
    def scoring(self) -> Dict[str, Any]:
        return self.raw.get("scoring", {})

    @property
    def regex_filters(self) -> Dict[str, Any]:
        return self.raw.get("regex_filters", {})

    @property
    def concurrency(self) -> Dict[str, Any]:
        return self.raw.get("concurrency", {})

    @property
    def external_scoring(self) -> Dict[str, Any]:
        return self.raw.get("external_scoring", {})

    @property
    def model(self) -> Dict[str, Any]:
        return self.raw.get("model", {})

    @property
    def downstream(self) -> Dict[str, Any]:
        return self.raw.get("downstream", {})

    def select_model(self) -> Dict[str, Any]:
        """Select the best-available model config based on priority list.

        返回第一个 enabled 的 candidate 配置。llm_client 的调用链会在
        该模型不可用时自动降级到 openclaw providers 中任意可用模型 →
        Anthropic SDK → AIME SDK，所以这里只做静态优先级选择，不会因为
        选错模型而卡死。
        """
        model_cfg = self.model or {}
        base = {k: v for k, v in model_cfg.items() if k != "candidates"}
        candidates = model_cfg.get("candidates") or []

        if isinstance(candidates, list):
            for cand in candidates:
                if not isinstance(cand, dict):
                    continue
                if cand.get("enabled", True) is False:
                    continue
                merged = base.copy()
                merged.update(cand)
                merged.setdefault("provider", "doubao")
                merged.setdefault("model_name", "doubao-pro-32k")
                return merged

        base.setdefault("provider", "doubao")
        base.setdefault("model_name", "doubao-pro-32k")
        return base

    # ── 持久化 ───────────────────────────────────────────────────────

    def _save(self) -> None:
        """把 self.raw 写回 config.yaml。"""
        if not self._config_path:
            return
        with open(self._config_path, "w", encoding="utf-8") as f:
            yaml.dump(self.raw, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
        logger.info("config 已持久化: %s", self._config_path)

    # ── 自动建表 ─────────────────────────────────────────────────────

    def ensure_provisioned(self) -> None:
        """确保 Bitable + 全部表存在。首次自动创建并回写 config.yaml，之后复用。

        逻辑：
        1. app_token 已有 + 全部 table_id 已有 → 直接返回（零 API 调用）
        2. app_token 已有 + 部分 table_id 缺失 → 只建缺失的表，回写
        3. app_token 为空 → 创建新 Bitable + 全部表，回写
        """
        from .bitable import BitableClient

        app_section = self.raw.setdefault("app", {})
        tables_section = self.raw.setdefault("tables", {})

        app_token = os.environ.get("BCMA_APP_TOKEN") or app_section.get("app_token", "")

        # 检查哪些表缺 table_id
        missing: List[str] = []
        for key in ALL_TABLE_KEYS:
            tid = tables_section.get(key, {}).get("table_id", "")
            if not tid:
                missing.append(key)

        if app_token and not missing:
            return  # 全部就绪

        dirty = False
        client = BitableClient(app_token) if app_token else None

        # 3) 没有 app_token → 创建 Bitable
        if not app_token:
            import json
            import urllib.request
            logger.info("app_token 为空，创建新 Bitable…")
            # 需要一个有效 token
            tmp_client = BitableClient("dummy")
            token = tmp_client._get_token()
            url = "https://open.feishu.cn/open-apis/bitable/v1/apps"
            body = json.dumps({"name": "品牌内容营销中枢"}).encode("utf-8")
            req = urllib.request.Request(url, data=body, method="POST", headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))
            if result.get("code") != 0:
                raise RuntimeError(f"创建 Bitable 失败: {result}")
            app_token = result["data"]["app"]["app_token"]
            bitable_url = result["data"]["app"].get("url", "")
            app_section["app_token"] = app_token
            logger.info("新 Bitable 已创建: %s (%s)", app_token, bitable_url)
            missing = ALL_TABLE_KEYS  # 新库，全部表都要建
            client = BitableClient(app_token)
            dirty = True

        # 2) 建缺失的表
        if missing:
            import json
            import urllib.request
            token = client._get_token()
            # 先列出已有表名做幂等
            list_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables?page_size=100"
            req = urllib.request.Request(list_url, method="GET", headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                existing = json.loads(resp.read().decode("utf-8"))
            existing_by_name: Dict[str, str] = {}
            for t in existing.get("data", {}).get("items", []):
                existing_by_name[t.get("name", "")] = t.get("table_id", "")

            for key in missing:
                schema = _TABLE_SCHEMAS.get(key)
                if not schema:
                    continue
                # 幂等：如果同名表已存在就复用
                if key in existing_by_name:
                    tid = existing_by_name[key]
                    logger.info("表已存在，复用: %s → %s", key, tid)
                else:
                    # 创建
                    create_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables"
                    body = json.dumps({
                        "table": {"name": key, "default_view_name": "全部", "fields": schema}
                    }).encode("utf-8")
                    req = urllib.request.Request(create_url, data=body, method="POST", headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    })
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        result = json.loads(resp.read().decode("utf-8"))
                    if result.get("code") != 0:
                        raise RuntimeError(f"创建表 {key} 失败: {result}")
                    tid = result["data"]["table_id"]
                    logger.info("表已创建: %s → %s", key, tid)

                tables_section.setdefault(key, {})["table_id"] = tid
                dirty = True

        if dirty:
            self._save()


def load_config(path: str | None = None) -> Config:
    """Load YAML config and return typed wrapper."""
    cfg_path = path or os.environ.get("BCMA_CONFIG_PATH", CONFIG_PATH_DEFAULT)
    if not os.path.isfile(cfg_path):
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    with open(cfg_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return Config(raw=data, _config_path=cfg_path)
