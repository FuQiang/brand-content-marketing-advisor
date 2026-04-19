"""Bitable 操作封装 - 使用飞书 HTTP API (User Access Token ONLY)

替代原 managing-lark-bitable-data 依赖，**强制使用用户个人权限访问**。

认证优先级（不再降级到 tenant_access_token）:
  1) 显式传入 user_token / 环境变量 LARK_USER_ACCESS_TOKEN
  2) openclaw-feishu-uat 加密存储的 UAT（自动刷新 refresh_token）
  3) 以上均无 → 抛 NoUserTokenError，提示用户到飞书 Bot 里跑 /feishu_auth
     重新授权；**不再回退 tenant_access_token**（其权限不足以写目标表）
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import urllib.request
import urllib.error
import urllib.parse


logger = logging.getLogger("bcma.bitable")

# ── openclaw-feishu-uat 常量 ──────────────────────────────────────
_UAT_DIR = Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share"))) / "openclaw-feishu-uat"
_UAT_IV_BYTES = 12
_UAT_TAG_BYTES = 16
_TOKEN_REFRESH_BUFFER = 300  # 提前 5 分钟刷新

# 进程内 UAT 缓存
_UAT_CACHE: Dict[str, Any] = {"token": "", "expires_at": 0.0}


def _decrypt_uat_file(enc_path: Path, key_path: Path) -> Optional[Dict[str, Any]]:
    """用 AES-256-GCM 解密 openclaw-feishu-uat 的加密文件。"""
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        key = key_path.read_bytes()
        data = enc_path.read_bytes()
        if len(data) < _UAT_IV_BYTES + _UAT_TAG_BYTES:
            return None
        iv = data[:_UAT_IV_BYTES]
        tag = data[_UAT_IV_BYTES:_UAT_IV_BYTES + _UAT_TAG_BYTES]
        ciphertext = data[_UAT_IV_BYTES + _UAT_TAG_BYTES:]
        plaintext = AESGCM(key).decrypt(iv, ciphertext + tag, None)
        return json.loads(plaintext)
    except Exception as exc:
        logger.debug("UAT 解密失败: %s", exc)
        return None


def _refresh_uat(token_data: Dict[str, Any], app_id: str, app_secret: str) -> Optional[str]:
    """用 refresh_token 换取新的 access_token。"""
    refresh_token = (token_data.get("refreshToken") or "").strip()
    if not refresh_token:
        return None
    refresh_expires_ms = float(token_data.get("refreshExpiresAt") or 0)
    if time.time() * 1000 >= refresh_expires_ms:
        logger.debug("UAT refresh_token 已过期")
        return None

    url = "https://open.feishu.cn/open-apis/authen/v2/oauth/token"
    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": app_id,
        "client_secret": app_secret,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "Content-Type": "application/x-www-form-urlencoded",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.debug("UAT refresh 请求失败: %s", exc)
        return None

    if result.get("code", 0) != 0 or result.get("error"):
        logger.debug("UAT refresh API 失败: %s", result)
        return None

    new_token = (result.get("access_token") or "").strip()
    if not new_token:
        return None

    expires_in = int(result.get("expires_in") or 7200)
    _UAT_CACHE["token"] = new_token
    _UAT_CACHE["expires_at"] = time.time() + expires_in - _TOKEN_REFRESH_BUFFER
    logger.info("UAT 刷新成功, expires_in=%ds", expires_in)
    return new_token


def _get_uat_from_encrypted_store(app_id: str, app_secret: str) -> Optional[str]:
    """从 openclaw-feishu-uat 加密存储读取有效 UAT。"""
    now = time.time()
    if _UAT_CACHE.get("token") and now < float(_UAT_CACHE.get("expires_at", 0)):
        return str(_UAT_CACHE["token"])

    key_path = _UAT_DIR / "master.key"
    if not key_path.is_file():
        logger.debug("UAT master.key 不存在: %s", key_path)
        return None

    # 查找加密文件：
    #   1) 本技能专属授权（.bcma.enc） —— 优先使用，scope 是本技能精简的 13 个
    #   2) openclaw-lark 通用授权（.enc）  —— fallback，scope 可能是整个应用的全集
    enc_file: Optional[Path] = None
    uat_app_id = os.environ.get("LARK_UAT_APP_ID", app_id)
    uat_user_id = os.environ.get("LARK_UAT_USER_ID", "")
    if uat_app_id and uat_user_id:
        for ext in (".bcma.enc", ".enc"):
            candidate = _UAT_DIR / f"{uat_app_id}_{uat_user_id}{ext}"
            if candidate.is_file():
                enc_file = candidate
                break
    if enc_file is None:
        # glob 优先匹配 .bcma.enc，fallback 到通用 .enc
        for pattern in ("*.bcma.enc", "*.enc"):
            # 排除 .bcma.enc 已经被第一轮找到的情况 —— 第二轮 *.enc 会同时匹配 .bcma.enc
            candidates = [p for p in _UAT_DIR.glob(pattern)
                          if not (pattern == "*.enc" and p.name.endswith(".bcma.enc"))]
            if candidates:
                enc_file = candidates[0]
                break

    if enc_file is None:
        logger.debug("无 UAT .enc 文件")
        return None

    token_data = _decrypt_uat_file(enc_file, key_path)
    if not token_data:
        return None

    expires_at_ms = float(token_data.get("expiresAt") or 0)
    now_ms = now * 1000
    refresh_ahead_ms = _TOKEN_REFRESH_BUFFER * 1000

    if now_ms < expires_at_ms - refresh_ahead_ms:
        access_token = (token_data.get("accessToken") or "").strip()
        if access_token:
            _UAT_CACHE["token"] = access_token
            _UAT_CACHE["expires_at"] = (expires_at_ms - refresh_ahead_ms) / 1000
            logger.debug("UAT 命中加密存储, expires_in=%.0fs", (expires_at_ms / 1000 - now))
            return access_token

    # 尝试 refresh
    refreshed = _refresh_uat(token_data, app_id, app_secret)
    if refreshed:
        return refreshed

    logger.debug("UAT 过期且无法刷新")
    return None


class RetryableRateLimitError(Exception):
    """可重试的速率限制错误"""
    pass


class NoUserTokenError(RuntimeError):
    """无可用用户令牌（UAT）。调用方应引导用户到飞书 Bot 里跑 /feishu_auth 重新授权。"""
    pass


_NO_UAT_HINT = (
    "无可用飞书用户令牌 (UAT)。当前技能已禁止 tenant_access_token 降级，"
    "因其权限不足以写入 brands / brand_audience / products / brand_topic_rules / "
    "topic_selection / content_matrix 等表。\n"
    "请在飞书 Bot 会话中发送 `/feishu_auth` 完成用户授权，或设置环境变量 "
    "LARK_USER_ACCESS_TOKEN 后重试。"
)


class BitableClient:
    """Bitable 客户端 - 使用飞书 HTTP API (User Access Token)"""

    def __init__(self, app_token: str, user_token: Optional[str] = None):
        self.app_token = app_token
        self._user_token = user_token
        # 仅用于 UAT refresh_token 刷新；不再用于换 tenant_access_token
        self._app_id = os.environ.get("LARK_APP_ID", "").strip()
        self._app_secret = os.environ.get("LARK_APP_SECRET", "").strip()

    def _get_token(self) -> str:
        """获取访问 token，仅接受用户令牌 (UAT)。

        优先级: 显式传入 > 环境变量 LARK_USER_ACCESS_TOKEN > openclaw-feishu-uat 加密存储。
        任一 UAT 源获取失败都抛 NoUserTokenError —— **绝不降级 tenant_access_token**。
        """
        if self._user_token:
            return self._user_token

        # 1) 环境变量
        env_token = os.environ.get("LARK_USER_ACCESS_TOKEN", "").strip()
        if env_token:
            self._user_token = env_token
            return env_token

        # 2) openclaw-feishu-uat 加密存储（与 trending-topic-radar 同源）
        uat = _get_uat_from_encrypted_store(self._app_id, self._app_secret)
        if uat:
            self._user_token = uat
            return uat

        raise NoUserTokenError(_NO_UAT_HINT)

    def _call_api(self, method: str, path: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict:
        """调用飞书 API"""
        token = self._get_token()

        url = f"https://open.feishu.cn/open-apis{path}"
        if params:
            query = "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())
            url = f"{url}?{query}"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        body = json.dumps(data).encode("utf-8") if data else None
        req = urllib.request.Request(url, data=body, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                raise RetryableRateLimitError(f"Rate limited: {e}")
            # 读一次 error body，避免二次 read 为空
            error_body = ""
            try:
                error_body = e.read().decode("utf-8")
            except Exception:
                pass
            logger.error("API 错误: %d - URL: %s", e.code, url)
            logger.error("请求体: %s", json.dumps(data, ensure_ascii=False) if data else "None")
            logger.error("错误详情: %s", error_body)
            if e.code == 403:
                # 不再降级 tenant_access_token。403 一律当作权限不足上抛，
                # 由调用方决定是提示用户重新授权还是跳过。
                logger.error(
                    "飞书返回 403，当前 UAT 可能缺少此表/应用的权限 scope，"
                    "请运行 /feishu_auth 重新授权或联系管理员补齐 scope"
                )
            raise

    def list_fields(self, table_id: str) -> List[Dict]:
        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/fields"
        result = self._call_api("GET", path)
        if result.get("code") != 0:
            logger.error("获取字段列表失败: %s", result)
            return []
        return result.get("data", {}).get("items", [])

    def create_field(
        self,
        table_id: str,
        field_name: str,
        field_type: int,
        ui_type: Optional[str] = None,
        property: Optional[Dict[str, Any]] = None,
    ) -> Dict:
        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/fields"
        data: Dict[str, Any] = {"field_name": field_name, "type": field_type}
        if ui_type:
            data["ui_type"] = ui_type
        if property:
            data["property"] = property
        result = self._call_api("POST", path, data=data)
        if result.get("code") != 0:
            raise RuntimeError(f"创建字段失败: {result}")
        return result.get("data", {})

    def search_records(
        self,
        table_id: str,
        filter_conditions: Optional[List[Dict]] = None,
        page_size: int = 500,
        view_id: Optional[str] = None,
        automatic_fields: bool = False,
    ) -> List[Dict]:
        """列出记录，支持 view_id 过滤和 automatic_fields。"""
        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"
        all_records: List[Dict] = []
        page_token = None
        while True:
            params: Dict[str, Any] = {"page_size": min(page_size, 500)}
            if page_token:
                params["page_token"] = page_token
            if view_id:
                params["view_id"] = view_id
            if automatic_fields:
                params["automatic_fields"] = "true"
            result = self._call_api("GET", path, params=params)
            if result.get("code") != 0:
                logger.error("获取记录失败: %s", result)
                break
            items = result.get("data", {}).get("items", [])
            all_records.extend(items)
            page_token = result.get("data", {}).get("page_token")
            has_more = result.get("data", {}).get("has_more", False)
            if not has_more or not page_token:
                break
        return all_records

    def add_record(self, table_id: str, fields: Dict[str, Any]) -> Dict:
        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"
        data = {"fields": fields}
        result = self._call_api("POST", path, data=data)
        if result.get("code") != 0:
            raise RuntimeError(f"添加记录失败: {result}")
        return result.get("data", {})

    def batch_add_records(self, table_id: str, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """批量添加，使用飞书批量 API（每批最多 500 条）。"""
        success = 0
        failed = 0
        for i in range(0, len(records), 500):
            batch = records[i:i + 500]
            path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/batch_create"
            data = {"records": [{"fields": r} for r in batch]}
            try:
                result = self._call_api("POST", path, data=data)
                if result.get("code") == 0:
                    success += len(batch)
                else:
                    logger.error("批量添加失败: %s", result)
                    failed += len(batch)
            except Exception as e:
                logger.error("批量添加异常: %s，降级为逐条写入", e)
                for record in batch:
                    try:
                        self.add_record(table_id, record)
                        success += 1
                    except Exception as e2:
                        logger.error("添加记录失败: %s", e2)
                        failed += 1
        return success, failed

    def update_record(self, table_id: str, record_id: str, fields: Dict[str, Any]) -> Dict:
        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/{record_id}"
        data = {"fields": fields}
        result = self._call_api("PUT", path, data=data)
        if result.get("code") != 0:
            raise RuntimeError(f"更新记录失败: {result}")
        return result.get("data", {})

    def batch_update_records(self, table_id: str, records: List[Tuple[str, Dict[str, Any]]]) -> Tuple[int, int]:
        """批量更新，使用飞书批量 API（每批最多 500 条）。"""
        success = 0
        failed = 0
        for i in range(0, len(records), 500):
            batch = records[i:i + 500]
            path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/batch_update"
            data = {
                "records": [
                    {"record_id": rid, "fields": fields}
                    for rid, fields in batch
                ]
            }
            try:
                result = self._call_api("POST", path, data=data)
                if result.get("code") == 0:
                    success += len(batch)
                else:
                    logger.error("批量更新失败: %s", result)
                    failed += len(batch)
            except Exception as e:
                logger.error("批量更新异常: %s，降级为逐条更新", e)
                for record_id, fields in batch:
                    try:
                        self.update_record(table_id, record_id, fields)
                        success += 1
                    except Exception as e2:
                        logger.error("更新记录失败: %s", e2)
                        failed += 1
        return success, failed

    def delete_record(self, table_id: str, record_id: str) -> bool:
        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/{record_id}"
        try:
            result = self._call_api("DELETE", path)
            return result.get("code") == 0
        except Exception:
            return False

    def batch_delete_records(self, table_id: str, record_ids: List[str]) -> Tuple[int, int]:
        success = 0
        failed = 0
        for record_id in record_ids:
            if self.delete_record(table_id, record_id):
                success += 1
            else:
                failed += 1
        return success, failed

    def preflight_write_check(self, table_id: str) -> Tuple[bool, str]:
        """预检写入权限：用表中第一个文本字段创建临时记录再删除。

        Returns:
            (True, "") 表示写入权限正常；
            (False, error_message) 表示权限不足，附带具体原因。
        """
        # 1) 获取表字段，找到一个可写的文本字段
        fields = self.list_fields(table_id)
        write_field = None
        for f in fields:
            # type=1 是文本，跳过系统自动字段
            if f.get("type") == 1 and f.get("field_name") not in (
                "记录ID", "创建时间", "修改时间", "创建人", "修改人",
            ):
                write_field = f.get("field_name")
                break
        if not write_field:
            # 没有文本字段，用空 fields 尝试（某些表允许）
            write_field = None

        sentinel = {write_field: "__preflight_check__"} if write_field else {}

        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/records"
        try:
            result = self._call_api("POST", path, data={"fields": sentinel})
        except NoUserTokenError as e:
            return False, str(e)
        except urllib.error.HTTPError as e:
            if e.code == 403:
                return False, (
                    "写入权限不足 (HTTP 403)。当前使用 user_access_token，"
                    "但该用户对此表/应用未授予写入 scope。"
                    "请在飞书 Bot 会话里发 /feishu_auth 重新授权，或联系管理员补齐 scope。"
                )
            return False, f"写入预检失败 (HTTP {e.code}): {e}"
        except Exception as e:
            return False, f"写入预检异常: {e}"

        if result.get("code") != 0:
            msg = result.get("msg", "")
            # 业务级权限错误（如 No permission）
            if "permission" in msg.lower() or "权限" in msg:
                return False, (
                    f"写入权限不足: {msg}。当前使用 user_access_token，"
                    "请在飞书 Bot 会话里发 /feishu_auth 重新授权获取更高 scope。"
                )
            return False, f"写入预检失败: {msg}"

        # 写入成功，清理临时记录
        record_id = (result.get("data") or {}).get("record", {}).get("record_id", "")
        if record_id:
            self.delete_record(table_id, record_id)
        return True, ""

    def get_token_type(self) -> str:
        """返回当前实际使用的 token 类型（供日志/诊断用）。

        不再包含 tenant_access_token —— 无 UAT 时返回 no_user_token 以便调用方
        显式处理未授权态。
        """
        if self._user_token:
            return "user_access_token"
        env_token = os.environ.get("LARK_USER_ACCESS_TOKEN", "").strip()
        if env_token:
            return "user_access_token (env)"
        uat = _get_uat_from_encrypted_store(self._app_id, self._app_secret)
        if uat:
            return "user_access_token (encrypted store)"
        return "no_user_token"


_client_cache: Dict[str, BitableClient] = {}


def get_client(app_token: str) -> BitableClient:
    if app_token not in _client_cache:
        _client_cache[app_token] = BitableClient(app_token)
    return _client_cache[app_token]


# ── 写入权限预检 ─────────────────────────────────────────────────

def check_write_permission(app_token: str, table_id: str) -> Tuple[bool, str]:
    """检查指定表的写入权限。返回 (ok, error_message)。"""
    client = get_client(app_token)
    return client.preflight_write_check(table_id)


def preflight_check_tables(
    app_token: str,
    table_map: Dict[str, str],
) -> Dict[str, str]:
    """批量预检多张表的写入权限。

    Args:
        app_token: 飞书 Bitable app_token
        table_map: {table_key: table_id} 需要检查的表

    Returns:
        失败的表 {table_key: error_message}，全部通过则返回空 dict。
    """
    failures: Dict[str, str] = {}

    # 先做一次 token 获取尝试：如果根本无 UAT，所有表都是同一个全局原因，
    # 没必要挨个去打 Bitable API，直接整批标记需要授权即可。
    client = get_client(app_token)
    try:
        client._get_token()
    except NoUserTokenError as e:
        msg = str(e)
        return {k: msg for k in table_map}

    for table_key, table_id in table_map.items():
        if not table_id:
            failures[table_key] = "table_id 未配置"
            continue
        ok, err = check_write_permission(app_token, table_id)
        if not ok:
            failures[table_key] = err
            # 同一个 token 失败了，后续表大概率也会失败，快速终止
            if "403" in err:
                for remaining_key in table_map:
                    if remaining_key not in failures and remaining_key != table_key:
                        failures[remaining_key] = err
                break
    return failures


def needs_user_authorization(app_token: str) -> bool:
    """判断当前是否处于未授权态（没有任何 UAT 来源可用）。

    供 main.py 入口在预检前快速区分"完全没授权" vs "授权了但 scope 不够"。
    """
    client = get_client(app_token)
    try:
        client._get_token()
        return False
    except NoUserTokenError:
        return True


# ── 模块级兼容接口 ─────────────────────────────────────────────────

def ensure_field_exists(
    app_token: str,
    table_id: str,
    field_name: str,
    field_type: Optional[int] = None,
    *,
    type_code: Optional[int] = None,
    ui_type: Optional[str] = None,
    property_obj: Optional[Dict[str, Any]] = None,
) -> bool:
    """确保字段存在。type_code 与 field_type 等价（callers 历史上两种叫法都用过）。"""
    resolved_type = field_type if field_type is not None else type_code
    if resolved_type is None:
        logger.warning("ensure_field_exists 缺少 field_type/type_code: %s", field_name)
        return False
    try:
        client = get_client(app_token)
        for f in client.list_fields(table_id):
            if f.get("field_name") == field_name:
                return True
        client.create_field(
            table_id,
            field_name,
            resolved_type,
            ui_type=ui_type,
            property=property_obj,
        )
        return True
    except Exception as e:
        logger.warning("确保字段存在失败: %s", e)
        return False


def search_all_records(app_token: str, table_id: str, **kwargs) -> List[Dict]:
    """搜索所有记录（兼容接口），支持 view_id / automatic_fields / page_size。"""
    client = get_client(app_token)
    page_size = kwargs.get("page_size", 500)
    filter_conditions = kwargs.get("filter_conditions")
    view_id = kwargs.get("view_id")
    automatic_fields = kwargs.get("automatic_fields", False)
    return client.search_records(
        table_id,
        filter_conditions,
        page_size,
        view_id=view_id,
        automatic_fields=automatic_fields,
    )


def update_single(app_token: str, table_id: str, record_id: str, fields: Dict[str, Any]) -> bool:
    """单条更新（兼容接口）"""
    try:
        client = get_client(app_token)
        client.update_record(table_id, record_id, fields)
        return True
    except Exception:
        return False


def add_single(app_token: str, table_id: str, fields: Dict[str, Any]) -> Optional[str]:
    """添加单条记录，返回 record_id（兼容接口）"""
    try:
        client = get_client(app_token)
        result = client.add_record(table_id, fields)
        return result.get("record", {}).get("record_id")
    except Exception as e:
        logger.warning("添加记录失败: %s", e)
        return None


def add_record(app_token: str, table_id: str, fields: Dict[str, Any]) -> Dict:
    """添加记录（兼容接口）"""
    client = get_client(app_token)
    return client.add_record(table_id, fields)


def update_record(app_token: str, table_id: str, record_id: str, fields: Dict[str, Any]) -> Dict:
    """更新记录（兼容接口）"""
    client = get_client(app_token)
    return client.update_record(table_id, record_id, fields)


def list_fields(app_token: str, table_id: str) -> List[Dict]:
    """获取字段列表（兼容接口）"""
    client = get_client(app_token)
    return client.list_fields(table_id)


def search_records(app_token: str, table_id: str, filter_conditions: Optional[List[Dict]] = None) -> List[Dict]:
    """搜索记录（兼容接口）"""
    client = get_client(app_token)
    return client.search_records(table_id, filter_conditions)


def list_table_fields(app_token: str, table_id: str) -> List[Dict]:
    """获取表字段列表（兼容接口）"""
    return list_fields(app_token, table_id)


def search_records_with_filter(app_token: str, table_id: str, filter_conditions: List[Dict]) -> List[Dict]:
    """带过滤条件搜索记录（兼容接口）"""
    return search_records(app_token, table_id, filter_conditions)


def delete_field_if_exists(app_token: str, table_id: str, field_name: str) -> bool:
    """删除字段（兼容接口）"""
    logger.warning("删除字段未实现: %s", field_name)
    return False


def upload_attachment_file(app_token: str, file_path: str, file_type: str = "bitable_image") -> Optional[str]:
    """上传附件到飞书 drive，返回 file_token。

    使用 /drive/v1/medias/upload_all（单次上传，适合 < 20MB 文件）。
    file_type: bitable_image | bitable_file
    """

    if not file_path or not os.path.isfile(file_path):
        logger.warning("上传附件: 文件不存在 %s", file_path)
        return None

    client = get_client(app_token)
    token = client._get_token()

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    # multipart/form-data 手动构建（避免 requests 依赖）
    boundary = f"----WebKitFormBoundary{int(time.time() * 1000)}"

    parts: List[bytes] = []

    # field: file_name
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"file_name\"\r\n\r\n{file_name}".encode())
    # field: parent_type
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"parent_type\"\r\n\r\n{file_type}".encode())
    # field: parent_node (app_token for bitable)
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"parent_node\"\r\n\r\n{app_token}".encode())
    # field: size
    parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"size\"\r\n\r\n{file_size}".encode())

    # 推断 mime type
    ext = os.path.splitext(file_name)[1].lower()
    mime_map = {
        ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".gif": "image/gif", ".webp": "image/webp", ".mp4": "video/mp4",
        ".mov": "video/quicktime", ".avi": "video/x-msvideo",
    }
    mime_type = mime_map.get(ext, "application/octet-stream")

    # field: file (binary)
    with open(file_path, "rb") as f:
        file_data = f.read()
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\n"
        f"Content-Type: {mime_type}\r\n\r\n".encode() + file_data
    )

    body = b"\r\n".join(parts) + f"\r\n--{boundary}--\r\n".encode()

    url = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
    }

    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = ""
        try:
            error_body = e.read().decode("utf-8")
        except Exception:
            pass
        logger.error("附件上传 HTTP 错误 %d: %s", e.code, error_body[:500])
        return None
    except Exception as e:
        logger.error("附件上传异常: %s", e)
        return None

    if result.get("code") != 0:
        logger.error("附件上传失败: %s", result.get("msg", result))
        return None

    file_token = result.get("data", {}).get("file_token")
    if file_token:
        logger.info("附件上传成功: %s -> %s", file_name, file_token)
    return file_token


def download_attachment_file(app_token_or_file_token: str, output_path_or_none: Optional[str] = None) -> str:
    """下载飞书附件到本地，返回本地文件路径。

    兼容两种调用方式：
    - download_attachment_file(app_token, file_token)  # 旧签名
    - download_attachment_file(file_token, output_path)  # 旧签名

    实际使用 /drive/v1/medias/{file_token}/download 接口。
    """

    # 判断第一个参数是 app_token 还是 file_token
    if output_path_or_none and output_path_or_none.startswith("/"):
        # download_attachment_file(file_token, output_path) 模式
        file_token = app_token_or_file_token
        output_path = output_path_or_none
        # 用任意缓存的 client 获取 token
        clients = list(_client_cache.values())
        if not clients:
            logger.warning("下载附件: 无可用 client")
            return ""
        token = clients[0]._get_token()
    else:
        # download_attachment_file(app_token, file_token) 模式
        app_token = app_token_or_file_token
        file_token = output_path_or_none or ""
        if not file_token:
            return ""
        client = get_client(app_token)
        token = client._get_token()
        # 生成临时输出路径
        import tempfile
        ext = ".bin"
        output_path = os.path.join(tempfile.mkdtemp(prefix="feishu_dl_"), f"{file_token}{ext}")

    url = f"https://open.feishu.cn/open-apis/drive/v1/medias/{file_token}/download"
    headers = {"Authorization": f"Bearer {token}"}
    req = urllib.request.Request(url, headers=headers, method="GET")

    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with urllib.request.urlopen(req, timeout=120) as resp:
            with open(output_path, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
        logger.info("附件下载成功: %s -> %s", file_token, output_path)
        return output_path
    except Exception as e:
        logger.error("附件下载失败 file_token=%s: %s", file_token, e)
        return ""
