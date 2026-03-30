"""Tests for resolve_decisions.py shared module."""
import json
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from resolve_decisions import (
    ResolvedDecisions,
    resolve_merge_chain,
    is_excluded,
    is_rejected,
    get_lcsh_decision,
    merge_appearances,
    merge_appears_in,
    transfer_counts,
    transfer_candidate_counts,
    apply_dedup_to_mapping,
    apply_merges_to_categories,
    load_all_decisions,
)


# ── resolve_merge_chain ──────────────────────────────────────────────


class TestResolveMergeChain:
    def test_no_merge(self):
        assert resolve_merge_chain("refA", {}) == "refA"

    def test_single_hop(self):
        assert resolve_merge_chain("refA", {"refA": "refB"}) == "refB"

    def test_chain(self):
        merge_map = {"refA": "refB", "refB": "refC"}
        assert resolve_merge_chain("refA", merge_map) == "refC"

    def test_cycle(self):
        merge_map = {"refA": "refB", "refB": "refA"}
        result = resolve_merge_chain("refA", merge_map)
        assert result in ("refA", "refB")

    def test_ref_not_in_map(self):
        assert resolve_merge_chain("refX", {"refA": "refB"}) == "refX"


# ── is_excluded / is_rejected / get_lcsh_decision ────────────────────


class TestDecisionChecks:
    def test_is_excluded_by_exclusion(self):
        d = ResolvedDecisions(exclusions={"refA", "refB"})
        assert is_excluded("refA", d)
        assert not is_excluded("refC", d)

    def test_is_excluded_by_global_rejection(self):
        d = ResolvedDecisions(global_rejections={"refX"})
        assert is_excluded("refX", d)

    def test_is_rejected_match(self):
        d = ResolvedDecisions(vol_rejections={"vol1": {"d1:refA:42"}})
        assert is_rejected("d1", "refA", 42, d, "vol1")

    def test_is_rejected_no_match(self):
        d = ResolvedDecisions(vol_rejections={"vol1": {"d1:refA:42"}})
        assert not is_rejected("d1", "refA", 99, d, "vol1")

    def test_is_rejected_wrong_volume(self):
        d = ResolvedDecisions(vol_rejections={"vol1": {"d1:refA:42"}})
        assert not is_rejected("d1", "refA", 42, d, "vol2")

    def test_get_lcsh_decision_global(self):
        d = ResolvedDecisions(lcsh_decisions={"refA": "accepted"})
        assert get_lcsh_decision("refA", d) == "accepted"

    def test_get_lcsh_decision_volume(self):
        d = ResolvedDecisions(vol_lcsh_decisions={"vol1": {"refA": "rejected"}})
        assert get_lcsh_decision("refA", d, "vol1") == "rejected"

    def test_get_lcsh_decision_global_overrides_volume(self):
        d = ResolvedDecisions(
            lcsh_decisions={"refA": "accepted"},
            vol_lcsh_decisions={"vol1": {"refA": "rejected"}},
        )
        assert get_lcsh_decision("refA", d, "vol1") == "accepted"

    def test_get_lcsh_decision_none(self):
        d = ResolvedDecisions()
        assert get_lcsh_decision("refA", d) is None


# ── merge_appearances ────────────────────────────────────────────────


class TestMergeAppearances:
    def test_empty(self):
        assert merge_appearances({}, {}) == {}

    def test_non_overlapping(self):
        target = {"vol1": ["d1", "d2"]}
        source = {"vol2": ["d3"]}
        result = merge_appearances(target, source)
        assert result == {"vol1": ["d1", "d2"], "vol2": ["d3"]}

    def test_overlapping_deduplicates(self):
        target = {"vol1": ["d1", "d2"]}
        source = {"vol1": ["d2", "d3"]}
        result = merge_appearances(target, source)
        assert result == {"vol1": ["d1", "d2", "d3"]}

    def test_sorts_output(self):
        target = {"vol1": ["d3"]}
        source = {"vol1": ["d1"]}
        result = merge_appearances(target, source)
        assert result["vol1"] == ["d1", "d3"]


# ── merge_appears_in ─────────────────────────────────────────────────


class TestMergeAppearsIn:
    def test_empty(self):
        assert merge_appears_in("", "") == ""

    def test_non_overlapping(self):
        result = merge_appears_in("vol1, vol2", "vol3")
        assert result == "vol1, vol2, vol3"

    def test_overlapping(self):
        result = merge_appears_in("vol1, vol2", "vol2, vol3")
        assert result == "vol1, vol2, vol3"

    def test_none_inputs(self):
        assert merge_appears_in(None, "vol1") == "vol1"
        assert merge_appears_in("vol1", None) == "vol1"


# ── transfer_counts ──────────────────────────────────────────────────


class TestTransferCounts:
    def test_basic_transfer(self):
        target = {"count": 10, "volumes": "2", "document_appearances": {}, "appears_in": "vol1"}
        source = {"count": 5, "volumes": "1", "document_appearances": {"vol2": ["d1"]}, "appears_in": "vol2"}
        transfer_counts(target, source)
        assert target["count"] == 15
        assert target["volumes"] == "3"
        assert target["document_appearances"] == {"vol2": ["d1"]}
        assert "vol1" in target["appears_in"]
        assert "vol2" in target["appears_in"]

    def test_transfer_with_zero_start(self):
        target = {"count": 0, "volumes": "0"}
        source = {"count": 100, "volumes": "5", "document_appearances": {"v1": ["d1", "d2"]}}
        transfer_counts(target, source)
        assert target["count"] == 100
        assert target["volumes"] == "5"

    def test_transfer_with_none_values(self):
        target = {}
        source = {"count": 50, "volumes": "3"}
        transfer_counts(target, source)
        assert target["count"] == 50

    def test_appearance_union(self):
        target = {"count": 0, "volumes": "0", "document_appearances": {"v1": ["d1"]}}
        source = {"count": 0, "volumes": "0", "document_appearances": {"v1": ["d1", "d2"], "v2": ["d3"]}}
        transfer_counts(target, source)
        assert target["document_appearances"]["v1"] == ["d1", "d2"]
        assert target["document_appearances"]["v2"] == ["d3"]


# ── transfer_candidate_counts ────────────────────────────────────────


class TestTransferCandidateCounts:
    def test_basic(self):
        target = {"count": 0, "volumes": "0"}
        candidate = {"doc_count": 50, "volume_count": 3, "volume_docs": {"v1": ["d1", "d2"]}}
        transfer_candidate_counts(target, candidate)
        assert target["count"] == 50
        assert target["volumes"] == "3"
        assert target["document_appearances"]["v1"] == ["d1", "d2"]

    def test_no_volume_docs(self):
        target = {"count": 10, "volumes": "1"}
        candidate = {"doc_count": 5, "volumes": ["v1", "v2"]}
        transfer_candidate_counts(target, candidate)
        assert target["count"] == 15
        assert target["volumes"] == "3"


# ── apply_dedup_to_mapping ───────────────────────────────────────────


class TestApplyDedupToMapping:
    def test_merges_counts(self):
        mapping = {
            "refA": {"name": "Term A", "count": 10, "appears_in": "vol1", "document_appearances": {}},
            "refB": {"name": "Term B", "count": 5, "appears_in": "vol2", "document_appearances": {}},
        }
        decisions = ResolvedDecisions(
            dedup_groups=[{"primary_ref": "refA", "all_refs": ["refA", "refB"]}]
        )
        result = apply_dedup_to_mapping(mapping, decisions)
        assert result["refA"]["count"] == 15
        assert result["refB"]["status"] == "merged_into"

    def test_no_groups(self):
        mapping = {"refA": {"name": "A"}}
        decisions = ResolvedDecisions()
        result = apply_dedup_to_mapping(mapping, decisions)
        assert result == mapping


# ── apply_merges_to_categories ───────────────────────────────────────


class TestApplyMergesToCategories:
    def test_taxonomy_merge(self):
        categories = {
            "Cat1": {
                "Sub1": [
                    ("refA", {"count": 100, "volumes": "5", "document_appearances": {"v1": ["d1"]}}),
                    ("refB", {"count": 50, "volumes": "3", "document_appearances": {"v2": ["d2"]}}),
                ]
            }
        }
        decisions = ResolvedDecisions(merge_map={"refA": "refB"})
        result = apply_merges_to_categories(categories, decisions)
        # refA should be removed, refB should have combined counts
        sub1 = result["Cat1"]["Sub1"]
        assert len(sub1) == 1
        assert sub1[0][0] == "refB"
        assert sub1[0][1]["count"] == 150

    def test_candidate_merge(self):
        categories = {
            "Cat1": {
                "Sub1": [("refTarget", {"count": 0, "volumes": "0"})]
            }
        }
        decisions = ResolvedDecisions(
            candidate_merges={
                "candidate_decisions_topics": {
                    "idx-001": {"action": "merged", "mergeTarget": "refTarget"}
                }
            },
            candidate_data={
                "candidate_decisions_topics": {
                    "idx-001": {"doc_count": 75, "volume_count": 2, "volume_docs": {}}
                }
            },
        )
        result = apply_merges_to_categories(categories, decisions)
        assert result["Cat1"]["Sub1"][0][1]["count"] == 75


# ── load_all_decisions (integration test with temp files) ────────────


class TestLoadAllDecisions:
    def test_loads_taxonomy_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create minimal taxonomy_review_state.json
            state = {
                "exclusions": {"excl-ref": {"name": "Excluded"}},
                "global_rejections": {"rej-ref": {"name": "Rejected"}},
                "lcsh_decisions": {"lcsh-ref": "accepted"},
                "merge_decisions": {"src": {"targetRef": "tgt"}},
            }
            with open(os.path.join(tmpdir, "taxonomy_review_state.json"), "w") as f:
                json.dump(state, f)
            os.makedirs(os.path.join(tmpdir, "config"), exist_ok=True)
            os.makedirs(os.path.join(tmpdir, "data"), exist_ok=True)

            decisions = load_all_decisions(tmpdir)
            assert "excl-ref" in decisions.exclusions
            assert "rej-ref" in decisions.global_rejections
            assert decisions.lcsh_decisions["lcsh-ref"] == "accepted"
            assert decisions.merge_map["src"] == "tgt"

    def test_loads_volume_rejections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # taxonomy_review_state (required)
            with open(os.path.join(tmpdir, "taxonomy_review_state.json"), "w") as f:
                json.dump({}, f)
            config_dir = os.path.join(tmpdir, "config")
            os.makedirs(config_dir, exist_ok=True)
            os.makedirs(os.path.join(tmpdir, "data"), exist_ok=True)

            # Per-volume rejections
            vol_data = {
                "rejections": [{"key": "d1:refA:42"}],
                "merge_decisions": [],
                "lcsh_decisions": [],
            }
            with open(os.path.join(config_dir, "annotation_rejections_vol1.json"), "w") as f:
                json.dump(vol_data, f)

            decisions = load_all_decisions(tmpdir, volume_id="vol1")
            assert is_rejected("d1", "refA", 42, decisions, "vol1")
            assert not is_rejected("d1", "refA", 99, decisions, "vol1")
