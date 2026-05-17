"""Tests for the structural fingerprinting algorithm."""

from app.adapters.fingerprint import structural_fingerprint


class TestStructuralFingerprint:
    def test_same_shape_different_values_same_fingerprint(self):
        a = {"carrier_scac": "MAEU", "milestone": "Loaded", "port": {"code": "SGSIN"}}
        b = {"carrier_scac": "ONEY", "milestone": "Sailed", "port": {"code": "NLRTM"}}
        assert structural_fingerprint(a) == structural_fingerprint(b)

    def test_different_shape_different_fingerprint(self):
        a = {"carrier_scac": "MAEU", "milestone": "Loaded"}
        b = {"carrier_scac": "MAEU", "milestone": {"text": "Loaded", "code": "LO"}}
        assert structural_fingerprint(a) != structural_fingerprint(b)

    def test_edge_case_1_scalar_vs_object(self):
        """Same key name, scalar vs nested object → different fingerprints."""
        a = {"location": "Hamburg"}
        b = {"location": {"port": "Hamburg"}}
        assert structural_fingerprint(a) != structural_fingerprint(b)

    def test_edge_case_2_array_elements_unioned(self):
        """Arrays with optional fields in elements → same fingerprint."""
        a = {"items": [{"a": 1, "b": 2}, {"a": 3}]}
        b = {"items": [{"a": 1}, {"a": 2, "b": 3}]}
        assert structural_fingerprint(a) == structural_fingerprint(b)

    def test_edge_case_3_missing_vs_null(self):
        """Missing key vs null key → different fingerprints (accepted fragmentation)."""
        a = {"a": 1}
        b = {"a": 1, "b": None}
        assert structural_fingerprint(a) != structural_fingerprint(b)

    def test_edge_case_4_map_paths_ignored(self):
        """Value-map subtrees with variable keys → same fingerprint when map_paths supplied."""
        a = {"references": {"MBL": "X", "BOL": "Y"}, "carrier": "MAEU"}
        b = {"references": {"MBL": "X", "AWB": "Z"}, "carrier": "MAEU"}
        # Without map_paths: different (different keys inside references)
        assert structural_fingerprint(a) != structural_fingerprint(b)
        # With map_paths: same (references subtree keys are ignored)
        assert structural_fingerprint(a, map_paths=["$.references"]) == structural_fingerprint(
            b, map_paths=["$.references"]
        )

    def test_key_order_invariant(self):
        a = {"z": 1, "a": 2, "m": 3}
        b = {"a": 2, "m": 3, "z": 1}
        assert structural_fingerprint(a) == structural_fingerprint(b)

    def test_nested_arrays(self):
        a = {"events": [{"ts": "2024-01-01", "code": "LO"}]}
        b = {"events": [{"ts": "2024-06-15", "code": "DI"}]}
        assert structural_fingerprint(a) == structural_fingerprint(b)

    def test_boolean_vs_number_distinction(self):
        a = {"flag": True}
        b = {"flag": 1}
        assert structural_fingerprint(a) != structural_fingerprint(b)

    def test_empty_payload(self):
        fp = structural_fingerprint({})
        assert isinstance(fp, str) and len(fp) == 64

    def test_real_maersk_shape(self):
        maersk_a = {
            "carrier_scac": "MAEU",
            "transport_doc": {"number": "MAEU240498712"},
            "container": "MSKU7748112",
            "milestone": "Loaded onboard",
            "milestone_at": "2026-04-28T14:30:00Z",
            "port": {"code": "SGSIN", "name": "Singapore"},
            "vessel": {"name": "Maersk Seletar", "imo": "9876543", "voyage": "426E"},
            "event_msg_id": "MSG-SG-20260428-0042",
        }
        maersk_b = {
            "carrier_scac": "MAEU",
            "transport_doc": {"number": "MAEU999999999"},
            "container": "MSKU1111111",
            "milestone": "Vessel Arrived",
            "milestone_at": "2026-05-01T08:00:00Z",
            "port": {"code": "NLRTM", "name": "Rotterdam"},
            "vessel": {"name": "Other Vessel", "imo": "1234567", "voyage": "100W"},
            "event_msg_id": "MSG-NL-20260501-0001",
        }
        assert structural_fingerprint(maersk_a) == structural_fingerprint(maersk_b)
