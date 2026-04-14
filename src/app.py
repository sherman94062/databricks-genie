"""Streamlit UI for the Genie API agent.

Run with:
    .venv/bin/streamlit run src/app.py
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from .client import GenieClient
from .router import QueryRouter

load_dotenv()


@st.cache_resource
def _get_spaces(_token_fingerprint: str) -> list[dict]:
    client = GenieClient(space_id=os.environ.get("GENIE_SPACE_ID", "placeholder"))
    return [
        {"id": getattr(s, "space_id", ""), "title": getattr(s, "title", "") or "(untitled)"}
        for s in client.list_spaces()
    ]


def _client(space_id: str) -> GenieClient:
    return GenieClient(space_id=space_id)


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
            "Genie space",
            ids,
            index=default_idx,
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
                    "question": question,
                    "routed": "trusted",
                    "trusted_name": decision.trusted.name,
                    "sql": decision.trusted.sql,
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
            "question": question,
            "routed": "genie",
            "status": result.status,
            "content": result.content,
            "sql": result.sql,
            "columns": result.columns,
            "rows": result.rows,
        }
        st.session_state.history.append(turn)
        _render_answer(turn)


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


if __name__ == "__main__":
    main()
