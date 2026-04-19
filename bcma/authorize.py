"""本技能专属的飞书 OAuth device-flow 授权入口。

**为什么独立做一套授权，不复用 openclaw-lark 的 `/feishu_auth`？**

openclaw-lark 的 `/feishu_auth` 会把**应用已开通的全部 user scope 一次性申请**，
对于本账号下应用开了 100+ scope 的场景，会导致用户一次授权拿到远超本技能需要
的权限面。本技能只需要 **13 个 scope**（多维表格 + 附件 + offline_access），
所以这里自己跑一遍 RFC 8628 device flow，把 scope 清单硬编码到代码里。

**与 openclaw-lark 的 UAT 存储共存**
- 两者使用同一把 `master.key` 和同一套 AES-256-GCM 加密格式，完全兼容
- 文件命名区分：
    - openclaw-lark 通用 UAT：  `{app_id}_{user_id}.enc`
    - 本技能专属 UAT：          `{app_id}_{user_id}.bcma.enc`
- `bitable.py` 读取时优先读 `.bcma.enc`，没有再 fallback 到 `.enc`

**典型调用流程（CLI）**
    python3 main.py authorize
        → 打印 verification_uri_complete + user_code
        → 用户在浏览器里点确认（飞书授权页只列 13 个 scope）
        → 脚本每 5 秒轮询一次 token endpoint
        → 成功后调 /authen/v1/user_info 拿 open_id，写入 .bcma.enc
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64encode
from pathlib import Path
from typing import Dict, Optional, Tuple


logger = logging.getLogger("bcma.authorize")


# ── 本技能最小 scope 清单 ────────────────────────────────────────
# 若后续加/去功能需要动这个列表，同步更新 SKILL.md 的"最小授权面"章节。
SKILL_SCOPES: Tuple[str, ...] = (
    # 多维表格 App 级（首次自动建表）
    "base:app:create",
    # 多维表格 Table 级
    "base:table:create",
    "base:table:read",
    # 多维表格 Field 级（schema_sync 需要 delete）
    "base:field:create",
    "base:field:read",
    "base:field:delete",
    # 多维表格 Record 级（preflight 探针需要 delete）
    "base:record:retrieve",
    "base:record:create",
    "base:record:update",
    "base:record:delete",
    # 附件（封面上传 + 产品底图下载）
    "drive:file:upload",
    "drive:file:download",
    # 令牌自动续期
    "offline_access",
)

# ── 飞书 OAuth 端点（与 openclaw-lark/src/core/device-flow.js 一致）─
_DEVICE_AUTH_URL = "https://accounts.feishu.cn/oauth/v1/device_authorization"
_TOKEN_URL = "https://open.feishu.cn/open-apis/authen/v2/oauth/token"
_USER_INFO_URL = "https://open.feishu.cn/open-apis/authen/v1/user_info"

# ── UAT 存储路径（与 openclaw-lark Linux backend 完全兼容）──────────
_UAT_DIR = Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local/share"))) / "openclaw-feishu-uat"
_MASTER_KEY_PATH = _UAT_DIR / "master.key"
_IV_BYTES = 12
_TAG_BYTES = 16


# ─────────────────────────────────────────────────────────────────
# AES-256-GCM 加解密（与 openclaw-lark 同格式：[IV(12)][TAG(16)][ciphertext]）
# ─────────────────────────────────────────────────────────────────

def _get_master_key() -> bytes:
    """读取或生成 master.key。

    openclaw-lark 首次运行时会创建该文件；若本技能先于插件运行，则由我们生成。
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # noqa: F401  # 仅用于校验 cryptography 已装

    if _MASTER_KEY_PATH.is_file():
        key = _MASTER_KEY_PATH.read_bytes()
        if len(key) == 32:
            return key
        logger.warning("master.key 长度异常 (%d)，重新生成", len(key))

    _UAT_DIR.mkdir(parents=True, mode=0o700, exist_ok=True)
    key = os.urandom(32)
    _MASTER_KEY_PATH.write_bytes(key)
    os.chmod(_MASTER_KEY_PATH, 0o600)
    logger.info("已生成新的 master.key: %s", _MASTER_KEY_PATH)
    return key


def _encrypt_payload(payload: Dict, key: bytes) -> bytes:
    """AES-256-GCM 加密 → [IV(12)][TAG(16)][ciphertext]"""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    iv = os.urandom(_IV_BYTES)
    plaintext = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    # AESGCM.encrypt 返回 ciphertext + tag（tag 在末尾 16 字节）
    ct_and_tag = AESGCM(key).encrypt(iv, plaintext, None)
    ciphertext = ct_and_tag[:-_TAG_BYTES]
    tag = ct_and_tag[-_TAG_BYTES:]
    return iv + tag + ciphertext


def _safe_filename(account: str) -> str:
    """与 openclaw-lark `linuxSafeFileName` 完全一致的命名规则。"""
    import re
    return re.sub(r"[^a-zA-Z0-9._-]", "_", account) + ".bcma.enc"


# ─────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────

def _http_post_form(url: str, form: Dict[str, str], *, basic_auth: Optional[str] = None) -> Dict:
    body = urllib.parse.urlencode(form).encode("utf-8")
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    if basic_auth:
        headers["Authorization"] = f"Basic {basic_auth}"
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
            return json.loads(err_body)
        except Exception:
            raise RuntimeError(f"HTTP {e.code}: {e}") from e


def _http_get(url: str, *, bearer: str) -> Dict:
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {bearer}"}, method="GET"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ─────────────────────────────────────────────────────────────────
# Device Flow (RFC 8628)
# ─────────────────────────────────────────────────────────────────

def _request_device_authorization(app_id: str, app_secret: str, scope: str) -> Dict:
    basic = b64encode(f"{app_id}:{app_secret}".encode()).decode()
    data = _http_post_form(
        _DEVICE_AUTH_URL,
        {"client_id": app_id, "scope": scope},
        basic_auth=basic,
    )
    if data.get("error"):
        raise RuntimeError(f"device_authorization 失败: {data}")
    return data


def _poll_token(app_id: str, app_secret: str, device_code: str, interval: int, expires_in: int) -> Dict:
    deadline = time.time() + expires_in
    cur_interval = max(1, interval)
    attempts = 0
    while time.time() < deadline and attempts < 200:
        attempts += 1
        time.sleep(cur_interval)
        data = _http_post_form(
            _TOKEN_URL,
            {
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": device_code,
                "client_id": app_id,
                "client_secret": app_secret,
            },
        )
        err = data.get("error")
        if not err and data.get("access_token"):
            return data
        if err == "authorization_pending":
            continue
        if err == "slow_down":
            cur_interval = min(cur_interval + 5, 60)
            continue
        if err in ("access_denied", "expired_token"):
            raise RuntimeError(f"授权失败: {err} — {data.get('error_description') or ''}")
        # 未知错误：继续轮询但延长间隔
        cur_interval = min(cur_interval + 2, 60)
        logger.warning("未知 token 错误，继续轮询: %s", data)
    raise RuntimeError("设备码过期或超时，请重新运行 authorize")


def _fetch_open_id(access_token: str) -> str:
    data = _http_get(_USER_INFO_URL, bearer=access_token)
    if data.get("code") != 0:
        raise RuntimeError(f"user_info 失败: {data}")
    open_id = (data.get("data") or {}).get("open_id", "")
    if not open_id:
        raise RuntimeError(f"user_info 未返回 open_id: {data}")
    return open_id


# ─────────────────────────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────────────────────────

def run_authorize() -> Dict:
    """执行 device-flow 授权，把 UAT 加密写入 .bcma.enc 文件。

    返回: {"status": "ok", "open_id": "...", "scope": "...", "file": "...", "expires_in": ...}
    """
    app_id = os.environ.get("LARK_APP_ID", "").strip()
    app_secret = os.environ.get("LARK_APP_SECRET", "").strip()
    if not app_id or not app_secret:
        raise RuntimeError(
            "缺少飞书应用凭证：请设置环境变量 LARK_APP_ID / LARK_APP_SECRET "
            "为当前调用方飞书应用的 App ID / App Secret 后重试。"
        )
    scope = " ".join(SKILL_SCOPES)

    print(f"\n📋 本技能需要的 scope ({len(SKILL_SCOPES)} 个):")
    for s in SKILL_SCOPES:
        print(f"   • {s}")
    print()

    # Step 1: 拿 device_code
    print("🔑 正在向飞书申请 device_code ...")
    auth = _request_device_authorization(app_id, app_secret, scope)
    user_code = auth.get("user_code", "")
    device_code = auth.get("device_code", "")
    verification_uri = auth.get("verification_uri", "")
    verification_uri_complete = auth.get("verification_uri_complete") or verification_uri
    interval = int(auth.get("interval", 5))
    expires_in = int(auth.get("expires_in", 240))

    print("\n" + "=" * 60)
    print("✅ device_code 获取成功，请在浏览器完成授权:")
    print()
    print(f"🌐 一键访问（推荐）: {verification_uri_complete}")
    if verification_uri_complete != verification_uri:
        print(f"🌐 或手动访问:       {verification_uri}")
        print(f"🔢 手动输入 code:    {user_code}")
    print()
    print(f"⏱️  有效期: {expires_in // 60} 分钟，轮询间隔: {interval} 秒")
    print("=" * 60 + "\n")
    print("正在等待你在浏览器里确认授权 ... (Ctrl+C 取消)\n")

    # Step 2: 轮询
    token_data = _poll_token(app_id, app_secret, device_code, interval, expires_in)
    access_token = token_data["access_token"]
    refresh_token = token_data.get("refresh_token", "")
    token_expires_in = int(token_data.get("expires_in", 7200))
    refresh_expires_in = int(token_data.get("refresh_token_expires_in", 604800))
    granted_scope = token_data.get("scope", scope)

    print("🎉 授权成功!")

    # Step 3: 拿 open_id
    open_id = _fetch_open_id(access_token)
    print(f"👤 open_id: {open_id}")

    # Step 4: 加密落盘
    now_ms = int(time.time() * 1000)
    payload = {
        "userOpenId": open_id,
        "appId": app_id,
        "accessToken": access_token,
        "refreshToken": refresh_token,
        "expiresAt": now_ms + token_expires_in * 1000,
        "refreshExpiresAt": now_ms + refresh_expires_in * 1000,
        "scope": granted_scope,
        "grantedAt": now_ms,
    }
    key = _get_master_key()
    encrypted = _encrypt_payload(payload, key)

    _UAT_DIR.mkdir(parents=True, mode=0o700, exist_ok=True)
    account = f"{app_id}_{open_id}"
    out_path = _UAT_DIR / _safe_filename(account)
    out_path.write_bytes(encrypted)
    os.chmod(out_path, 0o600)

    print(f"💾 UAT 已加密写入: {out_path}")
    print(f"📅 access_token 有效期: {token_expires_in // 60} 分钟")
    print(f"📅 refresh_token 有效期: {refresh_expires_in // 86400} 天（到期自动用 refresh 续期）")
    print(f"🔐 授权 scope: {granted_scope}\n")

    return {
        "status": "ok",
        "open_id": open_id,
        "scope": granted_scope,
        "file": str(out_path),
        "expires_in": token_expires_in,
        "refresh_expires_in": refresh_expires_in,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    try:
        result = run_authorize()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except KeyboardInterrupt:
        print("\n⛔ 用户取消授权", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ 授权失败: {e}", file=sys.stderr)
        sys.exit(1)
