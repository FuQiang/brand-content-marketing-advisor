"""Transaction log + transactional Bitable helpers for atomic multi-step writes.

Feishu Bitable has no native transactions. This module provides a rollback-on-failure
layer used by the brand data bootstrap flow (Step 1~4) in `run_all` mode:

  - 每个写操作 (add / update / delete) 先在 TxLog 中登记还原信息，然后执行。
  - 任一步抛错 → 调用 `tx.rollback()` 按 LIFO 顺序撤销：add→delete、update→恢复旧值、
    delete→重新插入（基于调用时快照）。
  - 回滚过程中单条失败记 warning 继续，最终返回 summary 含成功/失败数。

Attachments 不纳入快照范围（brands / brand_audience / brand_topic_rules 表均无附件字段；
products 表虽有 asset_gallery_field，但 Step 3 仅做 INSERT，回滚时直接删除整条记录，
附件随记录一起消失）。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .bitable import add_single, get_client, update_single

logger = logging.getLogger("bcma.tx")


class TxLog:
    """Records Bitable write ops in order so they can be rolled back LIFO."""

    def __init__(self, app_token: str) -> None:
        self.app_token = app_token
        self._ops: List[Dict[str, Any]] = []

    def record_add(self, table_id: str, record_id: str) -> None:
        if not record_id:
            return
        self._ops.append({"type": "add", "table_id": table_id, "record_id": record_id})

    def record_update(
        self,
        table_id: str,
        record_id: str,
        prev_fields: Dict[str, Any],
    ) -> None:
        self._ops.append({
            "type": "update",
            "table_id": table_id,
            "record_id": record_id,
            "prev_fields": dict(prev_fields),
        })

    def record_delete(self, table_id: str, prev_fields: Dict[str, Any]) -> None:
        self._ops.append({
            "type": "delete",
            "table_id": table_id,
            "prev_fields": dict(prev_fields),
        })

    def op_count(self) -> int:
        return len(self._ops)

    def rollback(self) -> Dict[str, Any]:
        """Reverse all recorded ops in LIFO order. Single op failures are logged
        and skipped so a single glitch does not abort the whole rollback.
        """
        client = get_client(self.app_token)
        reversed_ops = 0
        failed_ops: List[Dict[str, Any]] = []
        for op in reversed(self._ops):
            try:
                if op["type"] == "add":
                    ok = client.delete_record(op["table_id"], op["record_id"])
                    if not ok:
                        raise RuntimeError("delete_record returned False")
                elif op["type"] == "update":
                    ok = update_single(
                        self.app_token,
                        op["table_id"],
                        op["record_id"],
                        op["prev_fields"],
                    )
                    if not ok:
                        raise RuntimeError("update_single returned False")
                elif op["type"] == "delete":
                    rid = add_single(self.app_token, op["table_id"], op["prev_fields"])
                    if not rid:
                        raise RuntimeError("add_single returned empty record_id")
                reversed_ops += 1
            except Exception as e:
                logger.warning(
                    "回滚失败 type=%s table=%s: %s",
                    op.get("type"), op.get("table_id"), e,
                )
                failed_ops.append({
                    "type": op.get("type"),
                    "table_id": op.get("table_id"),
                    "error": str(e),
                })
        summary = {
            "total": len(self._ops),
            "reversed": reversed_ops,
            "failed": failed_ops,
        }
        logger.info("rollback summary: %s", summary)
        return summary


def tx_add_single(
    app_token: str,
    table_id: str,
    fields: Dict[str, Any],
    tx: Optional[TxLog] = None,
) -> Optional[str]:
    """Insert a record; register it with the tx log so rollback can delete it."""
    record_id = add_single(app_token, table_id, fields)
    if tx is not None and record_id:
        tx.record_add(table_id, record_id)
    return record_id


def tx_update_single(
    app_token: str,
    table_id: str,
    record_id: str,
    fields: Dict[str, Any],
    snapshot: Optional[Dict[str, Any]] = None,
    tx: Optional[TxLog] = None,
) -> bool:
    """Update a record. If tx is set, snapshot the about-to-be-overwritten
    values for the *exact keys being updated* so rollback can restore them.

    `snapshot` 应为该记录更新前的完整 fields dict（通常来自 search_all_records）。
    缺失的键在回滚时会被置为空字符串，与"无原值"近似等价。
    """
    if tx is not None:
        prev: Dict[str, Any] = {}
        for k in fields.keys():
            if snapshot is not None and k in snapshot:
                prev[k] = snapshot[k]
            else:
                prev[k] = ""
        tx.record_update(table_id, record_id, prev)
    return update_single(app_token, table_id, record_id, fields)


def tx_delete_record(
    app_token: str,
    table_id: str,
    record_id: str,
    prev_fields: Dict[str, Any],
    tx: Optional[TxLog] = None,
) -> bool:
    """Delete a record. Snapshot its fields beforehand so rollback can re-insert."""
    if tx is not None:
        tx.record_delete(table_id, prev_fields)
    client = get_client(app_token)
    try:
        return client.delete_record(table_id, record_id)
    except Exception as e:
        logger.warning("delete_record 异常 record_id=%s: %s", record_id, e)
        return False
