from src.router import QueryRouter


def test_matches_cost_last_30d():
    r = QueryRouter().route("what was our total spend last 30 days?")
    assert r.route == "trusted"
    assert r.trusted is not None
    assert r.trusted.name == "workspace_cost_last_30d"


def test_matches_top_skus():
    r = QueryRouter().route("show me the top SKU by spend")
    assert r.route == "trusted"
    assert r.trusted.name == "top_skus_last_30d"


def test_falls_through_to_genie():
    r = QueryRouter().route("how many active users signed up yesterday in EMEA?")
    assert r.route == "genie"
    assert r.trusted is None
