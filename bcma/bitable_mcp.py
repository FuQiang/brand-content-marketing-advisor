"""Bitable 操作封装 - 使用 OpenClaw MCP 飞书工具

替代原 HTTP API 调用，使用 OpenClaw 提供的 feishu_bitable_* 工具，
自动利用用户授权访问多维表格。
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple


logger = logging.getLogger("bcma.bitable")


class RetryableRateLimitError(Exception):
    """可重试的速率限制错误"""
    pass


class BitableClient:
    """Bitable 客户端 - 使用 OpenClaw MCP 工具"""

    def __init__(self, app_token: str, user_token: Optional[str] = None):
        self.app_token = app_token
        self._user_token = user_token

    def _call_tool(self, tool_name: str, params: Dict[str, Any]) -> Dict:
        """调用 OpenClaw 工具"""
        # 构建 JSON 输入
        json_input = json.dumps({"tool": tool_name, "params": params})

        # 调用 openclaw tool 命令
        cmd = ["openclaw", "tool", "--json"]
        try:
            result = subprocess.run(
                cmd,
                input=json_input,
                capture_output=True,
                text=True,
                timeout=60
            )
            if result.returncode != 0:
                logger.error(f"工具调用失败: {result.stderr}")
                raise RuntimeError(f"工具调用失败: {result.stderr}")

            # 解析输出
            output = result.stdout.strip()
            # 查找 JSON 结果（可能在多行输出中）
            for line in output.split('\n'):
                line = line.strip()
                if line and line.startswith('{'):
                    try:
                        return json.loads(line)
                    except json.JSONDecodeError:
                        continue
            return {}
        except subprocess.TimeoutExpired:
            raise RetryableRateLimitError("工具调用超时")
        except Exception as e:
            logger.error(f"调用工具 {tool_name} 失败: {e}")
            raise

    def list_fields(self, table_id: str) -> List[Dict]:
        """获取字段列表"""
        try:
            result = self._call_tool("feishu_bitable_app_table_field", {
                "action": "list",
                "app_token": self.app_token,
                "table_id": table_id,
                "page_size": 100
            })
            items = result.get("items", [])
            # 转换为统一格式
            return [
                {
                    "field_id": item.get("field_id"),
                    "field_name": item.get("field_name"),
                    "type": item.get("type"),
                    "ui_type": item.get("ui_type")
                }
                for item in items
            ]
        except Exception as e:
            logger.error(f"获取字段列表失败: {e}")
            return []

    def create_field(self, table_id: str, field_name: str, field_type: int, **kwargs) -> Dict:
        """创建字段"""
        result = self._call_tool("feishu_bitable_app_table_field", {
            "action": "create",
            "app_token": self.app_token,
            "table_id": table_id,
            "field_name": field_name,
            "type": field_type
        })
        if result.get("code") != 0:
            raise RuntimeError(f"创建字段失败: {result}")
        return result.get("data", {})

    def search_records(self, table_id: str, filter_conditions: Optional[List[Dict]] = None, page_size: int = 500) -> List[Dict]:
        """搜索记录"""
        all_records = []
        page_token = None

        while True:
            params = {
                "action": "list",
                "app_token": self.app_token,
                "table_id": table_id,
                "page_size": min(page_size, 500)
            }
            if page_token:
                params["page_token"] = page_token
            if filter_conditions:
                params["filter"] = {"conjunction": "and", "conditions": filter_conditions}

            result = self._call_tool("feishu_bitable_app_table_record", params)

            items = result.get("records", [])
            for item in items:
                all_records.append({
                    "record_id": item.get("record_id"),
                    "fields": item.get("fields", {})
                })

            page_token = result.get("page_token")
            has_more = result.get("has_more", False)
            if not has_more or not page_token:
                break

        return all_records

    def add_record(self, table_id: str, fields: Dict[str, Any]) -> Dict:
        """添加记录"""
        result = self._call_tool("feishu_bitable_app_table_record", {
            "action": "create",
            "app_token": self.app_token,
            "table_id": table_id,
            "fields": fields
        })
        if result.get("code") != 0 and result.get("code") is not None:
            raise RuntimeError(f"添加记录失败: {result}")
        return result.get("data", result)

    def batch_add_records(self, table_id: str, records: List[Dict[str, Any]]) -> Tuple[int, int]:
        """批量添加记录"""
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
        """更新记录"""
        result = self._call_tool("feishu_bitable_app_table_record", {
            "action": "update",
            "app_token": self.app_token,
            "table_id": table_id,
            "record_id": record_id,
            "fields": fields
        })
        if result.get("code") != 0 and result.get("code") is not None:
            raise RuntimeError(f"更新记录失败: {result}")
        return result.get("data", result)

    def batch_update_records(self, table_id: str, records: List[Tuple[str, Dict[str, Any]]]) -> Tuple[int, int]:
        """批量更新记录"""
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
        """删除记录"""
        try:
            result = self._call_tool("feishu_bitable_app_table_record", {
                "action": "delete",
                "app_token": self.app_token,
                "table_id": table_id,
                "record_id": record_id
            })
            return result.get("code") == 0 or result.get("code") is None
        except Exception:
            return False

    def batch_delete_records(self, table_id: str, record_ids: List[str]) -> Tuple[int, int]:
        """批量删除记录"""
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


# 保持与原 bitable.py 相同的函数签名
def list_fields(app_token: str, table_id: str) -> List[Dict]:
    """获取字段列表"""
    client = get_client(app_token)
    return client.list_fields(table_id)


def add_record(app_token: str, table_id: str, fields: Dict[str, Any]) -> Dict:
    """添加记录"""
    client = get_client(app_token)
    return client.add_record(table_id, fields)


def update_record(app_token: str, table_id: str, record_id: str, fields: Dict[str, Any]) -> Dict:
    """更新记录"""
    client = get_client(app_token)
    return client.update_record(table_id, record_id, fields)


def search_records(app_token: str, table_id: str, filter_conditions: Optional[List[Dict]] = None) -> List[Dict]:
    """搜索记录"""
    client = get_client(app_token)
    return client.search_records(table_id, filter_conditions)