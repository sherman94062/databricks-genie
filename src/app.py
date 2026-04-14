"""Streamlit UI for the Genie API agent.

Run with:
    .venv/bin/streamlit run src/app.py
"""

from __future__ import annotations

import os
import sys
import time as _time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.client import GenieClient  # noqa: E402
from src.cost import CostReporter  # noqa: E402
from src.router import QueryRouter  # noqa: E402
from src.session_log import SessionLog  # noqa: E402

load_dotenv()


@st.cache_resource
def _get_spaces(_token_fingerprint: str) -> list[dict]:
    client = GenieClient(space_id=os.environ.get("GENIE_SPACE_ID", "placeholder"))
    return [
        {"id": getattr(s, "space_id", ""), "title": getattr(s, "title", "") or "(untitled)"}
        for s in client.list_spaces()
    ]


def _client(space_id: str) -> GenieClient:
    return GenieClient(space_id=space_id, session_log=SessionLog())


def _render_answer(turn: dict) -> None:
    if turn.get("routed") == "trusted":
        st.info(f"Trusted query: **{turn['trusted_name']}**")
        st.code(turn["sql"], language="sql")
        return
    status = turn.get("status", "")
    if "COMPLETED" not in status.upper():
        st.warning(f"Status: {status}")
    if turn.get("content"):
        st.markdown(turn["content"])
    if turn.get("sql"):
        with st.expander("Generated SQL"):
            st.code(turn["sql"], language="sql")
    if turn.get("columns"):
        df = pd.DataFrame(turn["rows"], columns=turn["columns"])
        st.dataframe(df, use_container_width=True)
        st.caption(f"{len(df)} rows")


def _chat_tab(space_id: str, use_router: bool) -> None:
    for turn in st.session_state.history:
        with st.chat_message("user"):
            st.write(turn["question"])
        with st.chat_message("assistant"):
            _render_answer(turn)

    question = st.chat_input("Ask a question about your data…")
    if not question:
        return

    with st.chat_message("user"):
        st.write(question)

    if use_router:
        decision = QueryRouter().route(question)
        if decision.route == "trusted" and decision.trusted:
            with st.chat_message("assistant"):
                st.info(f"Routed to trusted query: **{decision.trusted.name}**")
                st.code(decision.trusted.sql, language="sql")
                st.caption("Trusted-query execution not yet wired up — Phase 2.")
            st.session_state.history.append(
                {
                    "question": question, "routed": "trusted",
                    "trusted_name": decision.trusted.name, "sql": decision.trusted.sql,
                }
            )
            return

    with st.chat_message("assistant"):
        with st.spinner("Asking Genie…"):
            try:
                result = _client(space_id).ask(question, conversation_id=st.session_state.conv_id)
            except Exception as e:
                st.error(f"Genie call failed: {e}")
                return
        st.session_state.conv_id = result.conversation_id
        turn = {
            "question": question, "routed": "genie", "status": result.status,
            "content": result.content, "sql": result.sql,
            "columns": result.columns, "rows": result.rows,
        }
        st.session_state.history.append(turn)
        _render_answer(turn)


def _cost_tab() -> None:
    hours = st.slider("Look-back window (hours)", 1, 168, 24)
    since_utc = _time.time() - hours * 3600

    log = SessionLog()
    summary = log.summary(since_utc=since_utc)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Genie calls", summary["n"])
    c2.metric("Completed", summary["completed"])
    c3.metric("Errors", summary["errors"])
    c4.metric("Avg latency", f"{summary['avg_latency_s']:.2f}s")

    st.subheader("Recent activity")
    rows = log.recent(limit=50)
    if rows:
        df = pd.DataFrame(rows)
        df["when_utc"] = df["ts_start_utc"].map(
            lambda t: datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        )
        st.dataframe(
            df[["when_utc", "latency_s", "status", "row_count", "statement_id", "question"]],
            use_container_width=True,
        )
    else:
        st.caption("No activity yet — ask something in the Chat tab.")

    st.divider()
    st.subheader("Per-question cost (statement_id → query.history)")
    st.caption(
        "Looks up each Genie-generated SQL statement in `system.query.history` to get "
        "authoritative duration, rows read/produced, and bytes read per question."
    )
    if st.button("Fetch per-statement history"):
        sids = log.statement_ids(since_utc=since_utc)
        if not sids:
            st.info("No Genie statement_ids captured in window yet.")
        else:
            try:
                reporter = CostReporter(session_log=log)
                with st.spinner(f"Looking up {len(sids)} statements…"):
                    r = reporter.per_statement_history(sids)
                if r.rows:
                    st.dataframe(
                        pd.DataFrame(r.rows, columns=r.columns), use_container_width=True
                    )
                else:
                    st.info("No rows yet — query history has a short lag.")
            except Exception as e:
                st.error(f"Query history lookup failed: {e}")

    st.divider()
    st.subheader("Warehouse billing attribution")
    st.caption(
        "Queries `system.billing.usage` for the configured warehouse. Billing has "
        "a lag (hours to ~1 day); very recent activity may not appear."
    )
    if st.button("Fetch warehouse spend"):
        try:
            reporter = CostReporter(session_log=log)
            with st.spinner("Running billing query on warehouse…"):
                attrib = reporter.attribute_to_session(since_utc=since_utc)
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("Warehouse DBUs", f"{attrib['warehouse_dbus']:.2f}")
            col_b.metric("Warehouse est. USD", f"${attrib['warehouse_est_usd']:.2f}")
            col_c.metric(
                "Avg $/Genie call (naive)",
                f"${attrib['avg_usd_per_call_estimate']:.4f}",
            )
            st.caption(attrib["note"])

            with st.spinner("Fetching per-SKU breakdown…"):
                bd = reporter.spend_breakdown_since(since_utc)
            if bd.rows:
                st.dataframe(pd.DataFrame(bd.rows, columns=bd.columns), use_container_width=True)
            else:
                st.info("No billing rows in window.")
        except Exception as e:
            st.error(f"Billing query failed: {e}")
            st.caption(
                "Check that the caller has SELECT on `system.billing.usage` and that "
                "DATABRICKS_WAREHOUSE_ID is set."
            )


def main() -> None:
    st.set_page_config(page_title="Databricks Genie Agent", layout="wide")
    st.title("Databricks Genie Agent")

    host = os.environ.get("DATABRICKS_HOST", "")
    if not host or not os.environ.get("DATABRICKS_TOKEN"):
        st.error("Set DATABRICKS_HOST and DATABRICKS_TOKEN in .env")
        st.stop()
    st.caption(f"Host: `{host}`")

    if "history" not in st.session_state:
        st.session_state.history = []
    if "conv_id" not in st.session_state:
        st.session_state.conv_id = None

    with st.sidebar:
        st.subheader("Space")
        try:
            spaces = _get_spaces(os.environ["DATABRICKS_TOKEN"][:8])
        except Exception as e:
            st.error(f"Could not list spaces: {e}")
            st.stop()

        default_id = os.environ.get("GENIE_SPACE_ID", "")
        ids = [s["id"] for s in spaces]
        labels = {s["id"]: f"{s['title']} · {s['id'][:8]}" for s in spaces}
        default_idx = ids.index(default_id) if default_id in ids else 0
        space_id = st.selectbox(
            "Genie space", ids, index=default_idx,
            format_func=lambda i: labels.get(i, i),
        )
        use_router = st.checkbox("Use trusted-query router", value=True)
        st.divider()
        if st.button("New conversation", use_container_width=True):
            st.session_state.conv_id = None
            st.session_state.history = []
            st.rerun()
        if st.session_state.conv_id:
            st.caption(f"conv: `{st.session_state.conv_id[:16]}…`")

    tab_chat, tab_cost = st.tabs(["Chat", "Cost & Activity"])
    with tab_chat:
        _chat_tab(space_id, use_router)
    with tab_cost:
        _cost_tab()


if __name__ == "__main__":
    main()
