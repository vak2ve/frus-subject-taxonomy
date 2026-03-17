"""Tests for build_variant_groups.py."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from build_variant_groups import (
    build_variant_groups,
    load_dedup_groups,
    load_lcsh_uri_overlaps,
    load_overrides,
)


@pytest.fixture
def tmp_config(tmp_path, monkeypatch):
    """Set up temporary config files."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("build_variant_groups.DEDUP_FILE", str(tmp_path / "dedup_decisions.json"))
    monkeypatch.setattr("build_variant_groups.SEMANTIC_DEDUP_FILE", str(tmp_path / "semantic_dedup_decisions.json"))
    monkeypatch.setattr("build_variant_groups.LCSH_MAPPING_FILE", str(tmp_path / "lcsh_mapping.json"))
    monkeypatch.setattr("build_variant_groups.OVERRIDES_FILE", str(tmp_path / "variant_overrides.json"))
    monkeypatch.setattr("build_variant_groups.OUTPUT_FILE", str(tmp_path / "variant_groups.json"))
    return tmp_path


def test_load_dedup_groups_missing_file(tmp_config):
    """Missing dedup file should return empty list."""
    groups = load_dedup_groups(str(tmp_config / "nonexistent.json"))
    assert groups == []


def test_load_dedup_groups_valid(tmp_config):
    """Should load merge groups from dedup decisions."""
    data = {
        "merge": [
            {
                "primary_ref": "rec1",
                "primary_name": "Cold War",
                "all_refs": ["rec1", "rec2"],
                "all_names": ["Cold War", "cold war"],
            }
        ]
    }
    path = tmp_config / "dedup.json"
    path.write_text(json.dumps(data))

    groups = load_dedup_groups(str(path))
    assert len(groups) == 1
    assert groups[0]["primary_ref"] == "rec1"


def test_load_overrides_missing_file(tmp_config):
    """Missing overrides file should return empty sets."""
    splits, merges = load_overrides(str(tmp_config / "nonexistent.json"))
    assert splits == set()
    assert merges == []


def test_load_overrides_with_splits(tmp_config):
    """Should parse split overrides into frozenset pairs."""
    data = {
        "overrides": [
            {"action": "split", "refs": ["rec1", "rec2", "rec3"]},
        ]
    }
    path = tmp_config / "overrides.json"
    path.write_text(json.dumps(data))

    splits, merges = load_overrides(str(path))
    assert frozenset(["rec1", "rec2"]) in splits
    assert frozenset(["rec1", "rec3"]) in splits
    assert frozenset(["rec2", "rec3"]) in splits
    assert merges == []


def test_load_lcsh_uri_overlaps_groups_by_uri(tmp_config):
    """Should group refs sharing the same LCSH URI."""
    mapping = {
        "rec1": {"name": "Cold War", "lcsh_uri": "http://id.loc.gov/authorities/subjects/sh1234", "count": 10},
        "rec2": {"name": "cold war", "lcsh_uri": "http://id.loc.gov/authorities/subjects/sh1234", "count": 5},
        "rec3": {"name": "NATO", "lcsh_uri": "http://id.loc.gov/authorities/subjects/sh5678", "count": 3},
    }
    path = tmp_config / "lcsh.json"
    path.write_text(json.dumps(mapping))

    taxonomy_refs = {"rec1": "Cold War", "rec2": "cold war", "rec3": "NATO"}
    groups = load_lcsh_uri_overlaps(str(path), taxonomy_refs)

    assert len(groups) == 1  # Only sh1234 has 2+ members
    assert groups[0]["canonical_ref"] == "rec1"  # Highest count
    assert set(groups[0]["all_refs"]) == {"rec1", "rec2"}


def test_build_variant_groups_basic(tmp_config):
    """Should produce correct structure with ref_to_canonical mapping."""
    # Create dedup file
    dedup = {
        "merge": [
            {
                "primary_ref": "rec1",
                "primary_name": "Cold War",
                "all_refs": ["rec1", "rec2"],
                "all_names": ["Cold War", "cold war"],
            }
        ]
    }
    (tmp_config / "dedup_decisions.json").write_text(json.dumps(dedup))

    taxonomy_refs = {"rec1": "Cold War", "rec2": "cold war", "rec3": "NATO"}
    result, stats = build_variant_groups(taxonomy_refs)

    assert "groups" in result
    assert "ref_to_canonical" in result
    assert result["ref_to_canonical"].get("rec2") == "rec1"
    assert stats["dedup"] == 1


def test_build_variant_groups_empty(tmp_config):
    """Should handle empty inputs gracefully."""
    result, stats = build_variant_groups({})

    assert result["total_groups"] == 0
    assert result["ref_to_canonical"] == {}
