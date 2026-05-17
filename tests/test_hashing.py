from app.hashing import canonical_json, compute_event_id, llm_cache_key


def test_canonical_json_sorts_keys():
    a = {"a": 1, "b": [3, 2, 1], "c": {"y": 2, "x": 1}}
    b = {"c": {"x": 1, "y": 2}, "b": [3, 2, 1], "a": 1}
    assert canonical_json(a) == canonical_json(b)


def test_canonical_json_preserves_array_order():
    assert canonical_json([1, 2, 3]) != canonical_json([3, 2, 1])


def test_event_id_deterministic_across_orderings():
    p1 = {"x": 1, "y": 2}
    p2 = {"y": 2, "x": 1}
    assert compute_event_id(p1) == compute_event_id(p2)


def test_event_id_changes_with_payload():
    assert compute_event_id({"a": 1}) != compute_event_id({"a": 2})


def test_event_id_includes_vendor_event_id():
    payload = {"a": 1}
    assert compute_event_id(payload) != compute_event_id(payload, "evt-123")
    assert compute_event_id(payload, "evt-123") == compute_event_id(payload, "evt-123")
    assert compute_event_id(payload, "evt-123") != compute_event_id(payload, "evt-124")


def test_llm_cache_key_versioned():
    payload = {"x": 1}
    k1 = llm_cache_key("v1", payload, "v1")
    k2 = llm_cache_key("v2", payload, "v1")
    k3 = llm_cache_key("v1", payload, "v2")
    assert k1 != k2 != k3 != k1


def test_llm_cache_key_vendor_scoped():
    payload = {"x": 1}
    shared = llm_cache_key("v1", payload, "v1")
    maersk = llm_cache_key("v1", payload, "v1", vendor_scope="maersk")
    one = llm_cache_key("v1", payload, "v1", vendor_scope="ocean_network_express")
    assert shared != maersk
    assert maersk != one
