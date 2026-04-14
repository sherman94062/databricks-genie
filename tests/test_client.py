from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.client import GenieClient, GenieTimeoutError


def _fake_workspace(start_resp, messages, result=None):
    w = MagicMock()
    w.genie.start_conversation.return_value = start_resp
    w.genie.get_message.side_effect = messages
    w.genie.get_message_query_result.return_value = result
    return w


def test_ask_polls_until_completed():
    start = SimpleNamespace(conversation_id="c1", message_id="m1")
    messages = [
        SimpleNamespace(status="IN_PROGRESS", attachments=[]),
        SimpleNamespace(
            status="COMPLETED",
            attachments=[
                SimpleNamespace(
                    text=SimpleNamespace(content="42"),
                    query=SimpleNamespace(query="SELECT 42"),
                )
            ],
        ),
    ]
    result_obj = SimpleNamespace(
        statement_response=SimpleNamespace(
            manifest=SimpleNamespace(
                schema=SimpleNamespace(columns=[SimpleNamespace(name="n")])
            ),
            result=SimpleNamespace(data_array=[[42]]),
        )
    )
    w = _fake_workspace(start, messages, result_obj)
    c = GenieClient(space_id="s1", workspace=w, poll_interval_s=0, timeout_s=5)

    r = c.ask("q")
    assert r.status == "COMPLETED"
    assert r.content == "42"
    assert r.sql == "SELECT 42"
    assert r.columns == ["n"]
    assert r.rows == [[42]]


def test_ask_times_out():
    start = SimpleNamespace(conversation_id="c1", message_id="m1")
    messages = [SimpleNamespace(status="IN_PROGRESS", attachments=[])] * 20
    w = _fake_workspace(start, messages)
    c = GenieClient(space_id="s1", workspace=w, poll_interval_s=0, timeout_s=0)
    with pytest.raises(GenieTimeoutError):
        c.ask("q")


def test_retry_on_transient_error():
    start = SimpleNamespace(conversation_id="c1", message_id="m1")
    w = MagicMock()
    w.genie.start_conversation.side_effect = [
        Exception("429 rate limited"),
        start,
    ]
    w.genie.get_message.return_value = SimpleNamespace(status="COMPLETED", attachments=[])
    w.genie.get_message_query_result.side_effect = Exception("no result")

    c = GenieClient(space_id="s1", workspace=w, poll_interval_s=0, timeout_s=5)
    # monkey-patch sleep to avoid the 1s backoff
    import src.client as mod
    mod.time.sleep = lambda *_: None  # type: ignore

    r = c.ask("q")
    assert r.status == "COMPLETED"
    assert w.genie.start_conversation.call_count == 2
