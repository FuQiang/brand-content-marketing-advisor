"""Bitable 操作封装 - 使用飞书 HTTP API (User Access Token)

替代原 managing-lark-bitable-data 依赖，使用用户个人权限访问。
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
import urllib.request
import urllib.error


logger = logging.getLogger("bcma.bitable")


class RetryableRateLimitError(Exception):
    """可重试的速率限制错误"""
    pass


class BitableClient:
    """Bitable 客户端 - 使用飞书 HTTP API (User Access Token)"""

    def __init__(self, app_token: str, user_token: Optional[str] = None):
        self.app_token = app_token
        self._user_token = user_token
        self._app_id = os.environ.get("LARK_APP_ID", "").strip()
        self._app_secret = os.environ.get("LARK_APP_SECRET", "").strip()
        self._tenant_token: Optional[str] = None
        self._tenant_token_expires: float = 0

    def _get_tenant_access_token(self) -> str:
        """获取 tenant_access_token 作为 fallback"""
        if self._tenant_token and time.time() < self._tenant_token_expires - 300:
            return self._tenant_token

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        data = json.dumps({
            "app_id": self._app_id,
            "app_secret": self._app_secret
        }).encode("utf-8")
        headers = {"Content-Type": "application/json"}

        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            if result.get("code") != 0:
                raise RuntimeError(f"获取 token 失败: {result}")
            self._tenant_token = result["tenant_access_token"]
            self._tenant_token_expires = time.time() + result.get("expire", 7200)
            return self._tenant_token

    def _get_token(self) -> str:
        """获取访问 token"""
        if self._user_token:
            return self._user_token
        return self._get_tenant_access_token()

    def _call_api(self, method: str, path: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict:
        """调用飞书 API"""
        token = self._get_token()

        url = f"https://open.feishu.cn/open-apis{path}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
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
            error_body = e.read().decode("utf-8") if e.read() else ""
            if e.code == 403 and self._user_token:
                logger.warning("User token 403，尝试使用 tenant token...")
                self._user_token = None
                return self._call_api(method, path, data, params)
            logger.error(f"API 错误: {e.code} - {error_body}")
            raise

    def list_fields(self, table_id: str) -> List[Dict]:
        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/fields"
        result = self._call_api("GET", path)
        if result.get("code") != 0:
            logger.error(f"获取字段列表失败: {result}")
            return []
        return result.get("data", {}).get("items", [])

    def create_field(self, table_id: str, field_name: str, field_type: int, **kwargs) -> Dict:
        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/fields"
        data = {"field_name": field_name, "type": field_type}
        if kwargs:
            data["property"] = kwargs
        result = self._call_api("POST", path, data=data)
        if result.get("code") != 0:
            raise RuntimeError(f"创建字段失败: {result}")
        return result.get("data", {})

    def search_records(self, table_id: str, filter_conditions: Optional[List[Dict]] = None, page_size: int = 500) -> List[Dict]:
        path = f"/bitable/v1/apps/{self.app_token}/tables/{table_id}/records/search"
        all_records = []
        page_token = None
        while True:
            data = {"page_size": min(page_size, 500)}
            if page_token:
                data["page_token"] = page_token
            if filter_conditions:
                data["filter"] = {"conjunction": "and", "conditions": filter_conditions}
            result = self._call_api("POST", path, data=data)
            if result.get("code") != 0:
                logger.error(f"搜索记录失败: {result}")
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
        success = 0
        failed = 0
        for record in records:
            try:
                self.add_record(table_id, record)
                success += 1
            except Exception as e:
                logger.error(f"添加记录失败: {e}")
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
        success = 0
        failed = 0
        for record_id, fields in records:
            try:
                self.update_record(table_id, record_id, fields)
                success += 1
            except Exception as e:
                logger.error(f"更新记录失败: {e}")
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


_client_cache: Dict[str, BitableClient] = {}


def get_client(app_token: str) -> BitableClient:
    if app_token not in _client_cache:
        _client_cache[app_token] = BitableClient(app_token)
    return _client_cache[app_token]


def ensure_field_exists(app_token: str, table_id: str, field_name: str, field_type: int, **kwargs) -> bool:
    """确保字段存在（兼容接口）"""
    try:
        client = get_client(app_token)
        fields = client.list_fields(table_id)
        for f in fields:
            if f.get("field_name") == field_name:
                return True
        client.create_field(table_id, field_name, field_type, **kwargs)
        return True
    except Exception as e:
        logger.warning(f"确保字段存在失败: {e}")
        return False


def search_all_records(app_token: str, table_id: str, **kwargs) -> List[Dict]:
    """搜索所有记录（兼容接口）"""
    client = get_client(app_token)
    return client.search_records(table_id, kwargs.get("filter_conditions"))


def update_single(app_token: str, table_id: str, record_id: str, fields: Dict[str, Any]) -> bool:
    """单条更新（兼容接口）"""
    try:
        update_record(app_token, table_id, record_id, fields)
        return True
    except Exception:
        return False


def add_single(app_token: str, table_id: str, fields: Dict[str, Any]) -> Optional[str]:
    """添加单条记录，返回 record_id（兼容接口）"""
    try:
        result = add_record(app_token, table_id, fields)
        return result.get("record", {}).get("record_id")
    except Exception as e:
        logger.warning(f"添加记录失败: {e}")
        return None


def list_table_fields(app_token: str, table_id: str) -> List[Dict]:
    """获取表字段列表（兼容接口）"""
    return list_fields(app_token, table_id)


def search_records_with_filter(app_token: str, table_id: str, filter_conditions: List[Dict]) -> List[Dict]:
    """带过滤条件搜索记录（兼容接口）"""
    return search_records(app_token, table_id, filter_conditions)


def delete_field_if_exists(app_token: str, table_id: str, field_name: str) -> bool:
    """删除字段（兼容接口）"""
    logger.warning(f"删除字段未实现: {field_name}")
    return False


def upload_attachment_file(app_token: str, file_path: str) -> Optional[str]:
    """上传附件（兼容接口）"""
    logger.warning(f"附件上传未实现: {file_path}")
    return None


def download_attachment_file(file_token: str, output_path: str) -> bool:
    """下载附件（兼容接口）"""
    logger.warning(f"附件下载未实现: {file_token}")
    return False
