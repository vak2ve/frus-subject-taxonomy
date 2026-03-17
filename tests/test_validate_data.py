"""Tests for validate_data.py."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from validate_data import check_results_integrity, check_volume_sources, check_coverage, BASE_DIR


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    """Create a temporary data structure and monkeypatch BASE_DIR."""
    monkeypatch.setattr("validate_data.BASE_DIR", tmp_path)
    (tmp_path / "annotations").mkdir()
    (tmp_path / "data" / "documents").mkdir(parents=True)
    (tmp_path / "volumes").mkdir()
    return tmp_path


def make_string_match_results(data_dir, volume_id, docs=None, extra_keys=True):
    """Create a valid string_match_results JSON file."""
    if docs is None:
        docs = {
            "d1": {
                "title": "Test Document",
                "date": "January 1, 1970",
                "doc_type": "historical-document",
                "match_count": 2,
                "matches": [
                    {"term": "Cold War", "sentence": "Context about Cold War."},
                    {"term": "NATO", "sentence": "Context about NATO."},
                ],
            }
        }

    result = {
        "metadata": {
            "volume_id": volume_id,
            "total_matches": sum(d.get("match_count", len(d.get("matches", []))) for d in docs.values()),
        },
        "by_document": docs,
        "by_term": {},
    }

    vol_dir = data_dir / "data" / "documents" / volume_id
    vol_dir.mkdir(parents=True, exist_ok=True)
    with open(vol_dir / f"string_match_results_{volume_id}.json", "w") as f:
        json.dump(result, f)


def test_check_results_integrity_valid(data_dir):
    """Valid string_match_results should pass with no issues."""
    make_string_match_results(data_dir, "frus-test-v01")
    issues = check_results_integrity()
    error_issues = [i for i in issues if i[0].startswith("all_") or i[0] in ("bad_json", "missing_key")]
    assert len(error_issues) == 0


def test_check_results_integrity_bad_json(data_dir):
    """Malformed JSON should be caught."""
    vol_dir = data_dir / "data" / "documents" / "frus-bad"
    vol_dir.mkdir(parents=True)
    with open(vol_dir / "string_match_results_frus-bad.json", "w") as f:
        f.write("{not valid json")

    issues = check_results_integrity()
    assert any(i[0] == "bad_json" for i in issues)


def test_check_results_integrity_missing_keys(data_dir):
    """Missing required top-level keys should be flagged."""
    vol_dir = data_dir / "data" / "documents" / "frus-missing"
    vol_dir.mkdir(parents=True)
    with open(vol_dir / "string_match_results_frus-missing.json", "w") as f:
        json.dump({"metadata": {}}, f)

    issues = check_results_integrity()
    assert any(i[0] == "missing_key" for i in issues)


def test_check_results_integrity_all_empty_titles(data_dir):
    """All documents with empty titles should be flagged as error."""
    make_string_match_results(data_dir, "frus-empty", docs={
        "d1": {"title": "", "date": "Jan 1, 1970", "doc_type": "historical-document",
               "match_count": 1, "matches": [{"term": "test", "sentence": "ctx"}]},
        "d2": {"title": "", "date": "Jan 2, 1970", "doc_type": "historical-document",
               "match_count": 1, "matches": [{"term": "test2", "sentence": "ctx2"}]},
    })

    issues = check_results_integrity()
    assert any(i[0] == "all_empty_titles" for i in issues)


def test_check_volume_sources_ok(data_dir):
    """Volume with XML and split docs should pass."""
    vol_id = "frus-test-v01"
    # Create annotation file
    with open(data_dir / "annotations" / f"annotations_{vol_id}.xml", "w") as f:
        f.write("<annotations><volume_id>frus-test-v01</volume_id></annotations>")
    # Create volume XML
    with open(data_dir / "volumes" / f"{vol_id}.xml", "w") as f:
        f.write("<TEI/>")
    # Create split docs
    doc_dir = data_dir / "data" / "documents" / vol_id
    doc_dir.mkdir(parents=True)
    with open(doc_dir / "d1.xml", "w") as f:
        f.write("<TEI/>")

    issues = check_volume_sources()
    error_issues = [i for i in issues if i[0] in ("missing_source", "needs_split")]
    assert len(error_issues) == 0


def test_check_volume_sources_needs_split(data_dir):
    """Volume with XML but no split docs should flag needs_split."""
    vol_id = "frus-test-v02"
    # Create results so the vol is discovered
    make_string_match_results(data_dir, vol_id)
    # Create volume XML
    with open(data_dir / "volumes" / f"{vol_id}.xml", "w") as f:
        f.write("<TEI/>")
    # No split docs

    issues = check_volume_sources()
    assert any(i[0] == "needs_split" for i in issues)


def test_check_coverage_missing_results(data_dir):
    """Annotations without results should be flagged."""
    vol_id = "frus-test-v03"
    with open(data_dir / "annotations" / f"annotations_{vol_id}.xml", "w") as f:
        f.write("<annotations><volume_id>frus-test-v03</volume_id></annotations>")

    issues = check_coverage()
    assert any(i[0] == "no_results_for_annotations" for i in issues)
