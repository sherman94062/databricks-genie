"""Genie Conversation API client wrapper.

Thin layer over databricks-sdk's `w.genie` namespace adding:
  - polling with timeout
  - exponential-backoff retry on transient errors
  - rate-limit awareness (Free Edition: 5 QPM per workspace)
  - structured result extraction
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from databricks.sdk import WorkspaceClient

from .session_log import CallRecord, SessionLog, now_utc

log = logging.getLogger(__name__)

TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED"}
DEFAULT_POLL_INTERVAL_S = 2.0
DEFAULT_TIMEOUT_S = 600.0
MAX_RETRIES = 3


@dataclass
class GenieResult:
    conversation_id: str
    message_id: str
    status: str
    content: Optional[str]
    sql: Optional[str]
    columns: list[str]
    rows: list[list[Any]]
    raw_message: Any
    raw_result: Any


class GenieTimeoutError(Exception):
    pass


class GenieClient:
    def __init__(
        self,
        space_id: Optional[str] = None,
        workspace: Optional[WorkspaceClient] = None,
        poll_interval_s: float = DEFAULT_POLL_INTERVAL_S,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        session_log: Optional[SessionLog] = None,
        warehouse_id: Optional[str] = None,
    ):
        self.space_id = space_id or os.environ.get("GENIE_SPACE_ID")
        if not self.space_id:
            raise ValueError("space_id or GENIE_SPACE_ID env var required")
        self.w = workspace or WorkspaceClient()
        self.poll_interval_s = poll_interval_s
        self.timeout_s = timeout_s
        self.session_log = session_log
        self.warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID")

    def list_spaces(self) -> list[Any]:
        resp = self.w.genie.list_spaces()
        if hasattr(resp, "spaces"):
            return list(resp.spaces or [])
        return list(resp)

    def ask(self, question: str, conversation_id: Optional[str] = None) -> GenieResult:
        """Ask a question. Starts a new conversation unless conversation_id is given."""
        ts_start = now_utc()
        error: Optional[str] = None
        result: Optional[GenieResult] = None
        try:
            result = self._ask_impl(question, conversation_id)
            return result
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            raise
        finally:
            if self.session_log is not None:
                ts_end = now_utc()
                self.session_log.record(
                    CallRecord(
                        ts_start_utc=ts_start,
                        ts_end_utc=ts_end,
                        latency_s=ts_end - ts_start,
                        space_id=str(self.space_id),
                        warehouse_id=self.warehouse_id,
                        conversation_id=result.conversation_id if result else None,
                        message_id=result.message_id if result else None,
                        question=question,
                        status=result.status if result else "ERROR",
                        row_count=len(result.rows) if result else 0,
                        error=error,
                    )
                )

    def _ask_impl(self, question: str, conversation_id: Optional[str]) -> GenieResult:
        if conversation_id:
            log.debug("Sending follow-up to conversation %s", conversation_id)
            resp = self._retry(
                self.w.genie.create_message,
                space_id=self.space_id,
                conversation_id=conversation_id,
                content=question,
            )
            conv_id = conversation_id
            msg_id = resp.message_id if hasattr(resp, "message_id") else resp.id
        else:
            log.debug("Starting new conversation")
            resp = self._retry(
                self.w.genie.start_conversation,
                space_id=self.space_id,
                content=question,
            )
            conv_id = resp.conversation_id
            msg_id = resp.message_id

        msg = self._poll_until_done(conv_id, msg_id)
        result = self._fetch_result(conv_id, msg_id, msg)
        return result

    def _poll_until_done(self, conv_id: str, msg_id: str) -> Any:
        deadline = time.monotonic() + self.timeout_s
        while True:
            msg = self._retry(
                self.w.genie.get_message,
                space_id=self.space_id,
                conversation_id=conv_id,
                message_id=msg_id,
            )
            status = str(getattr(msg, "status", "")).upper()
            log.debug("Message %s status=%s", msg_id, status)
            if any(t in status for t in TERMINAL_STATES):
                return msg
            if time.monotonic() > deadline:
                raise GenieTimeoutError(
                    f"Genie message {msg_id} did not complete within {self.timeout_s}s"
                )
            time.sleep(self.poll_interval_s)

    def _fetch_result(self, conv_id: str, msg_id: str, msg: Any) -> GenieResult:
        content = None
        sql = None
        columns: list[str] = []
        rows: list[list[Any]] = []
        raw_result = None

        for attachment in getattr(msg, "attachments", None) or []:
            text = getattr(attachment, "text", None)
            if text is not None:
                content = getattr(text, "content", None) or content
            query = getattr(attachment, "query", None)
            if query is not None:
                sql = getattr(query, "query", None) or getattr(query, "sql", None)

        status = str(getattr(msg, "status", "")).upper()
        if "COMPLETED" in status:
            try:
                raw_result = self.w.genie.get_message_query_result(
                    space_id=self.space_id,
                    conversation_id=conv_id,
                    message_id=msg_id,
                )
                sr = getattr(raw_result, "statement_response", None)
                if sr is not None:
                    manifest = getattr(sr, "manifest", None)
                    schema = getattr(manifest, "schema", None) if manifest else None
                    if schema is not None:
                        columns = [c.name for c in getattr(schema, "columns", []) or []]
                    data = getattr(sr, "result", None)
                    if data is not None:
                        rows = getattr(data, "data_array", None) or []
            except Exception as e:
                log.debug("No query result available: %s", e)

        return GenieResult(
            conversation_id=conv_id,
            message_id=msg_id,
            status=status,
            content=content,
            sql=sql,
            columns=columns,
            rows=rows,
            raw_message=msg,
            raw_result=raw_result,
        )

    @staticmethod
    def _retry(fn, *args, **kwargs):
        delay = 1.0
        last_exc: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                last_exc = e
                msg = str(e).lower()
                transient = any(
                    s in msg for s in ("rate", "429", "timeout", "503", "temporarily")
                )
                if not transient or attempt == MAX_RETRIES:
                    raise
                log.warning(
                    "Transient error on %s (attempt %d/%d): %s; sleeping %.1fs",
                    getattr(fn, "__name__", "call"),
                    attempt,
                    MAX_RETRIES,
                    e,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
        assert last_exc is not None
        raise last_exc
