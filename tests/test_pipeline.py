"""Tests for run_reviewed_pipeline.py and overall pipeline integration."""

import json
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from run_reviewed_pipeline import run_step


def test_run_step_success():
    """run_step should return True for successful commands."""
    result = run_step("Test echo", [sys.executable, "-c", "print('ok')"])
    assert result is True


def test_run_step_failure():
    """run_step should return False for failed commands."""
    result = run_step("Test fail", [sys.executable, "-c", "raise SystemExit(1)"])
    assert result is False


def test_run_step_nonexistent_command():
    """run_step should return False for nonexistent commands (not raise)."""
    # This tests the error handling added to run_step
    result = run_step("Bad command", ["__nonexistent_command__"])
    assert result is False


class TestValidateDataIntegration:
    """Integration tests that run validate_data.py against real repo data."""

    @pytest.fixture
    def repo_root(self):
        return Path(__file__).resolve().parent.parent

    def test_validate_data_runs(self, repo_root):
        """validate_data.py should run without crashing."""
        result = subprocess.run(
            [sys.executable, str(repo_root / "scripts" / "validate_data.py")],
            capture_output=True,
            text=True,
            cwd=str(repo_root),
        )
        # Should complete (may have warnings but shouldn't crash)
        assert result.returncode in (0, 1)
        assert "Validating annotation data" in result.stdout


class TestDataIntegrity:
    """Tests that verify the integrity of committed data files."""

    @pytest.fixture
    def repo_root(self):
        return Path(__file__).resolve().parent.parent

    def test_subject_taxonomy_lcsh_is_valid_xml(self, repo_root):
        """subject-taxonomy-lcsh.xml should be valid XML."""
        from lxml import etree
        taxonomy_path = repo_root / "subject-taxonomy-lcsh.xml"
        if not taxonomy_path.exists():
            pytest.skip("subject-taxonomy-lcsh.xml not present")
        tree = etree.parse(str(taxonomy_path))
        root = tree.getroot()
        assert root.tag == "taxonomy"
        # Should have at least some subjects
        subjects = root.findall(".//subject")
        assert len(subjects) > 0

    def test_lcsh_mapping_is_valid_json(self, repo_root):
        """config/lcsh_mapping.json should be valid JSON with expected structure."""
        mapping_path = repo_root / "config" / "lcsh_mapping.json"
        if not mapping_path.exists():
            pytest.skip("lcsh_mapping.json not present")
        with open(mapping_path) as f:
            data = json.load(f)
        assert isinstance(data, dict)
        assert len(data) > 0
        # Each entry should have at least a name
        sample = next(iter(data.values()))
        assert "name" in sample

    def test_dedup_decisions_structure(self, repo_root):
        """config/dedup_decisions.json should have expected structure."""
        path = repo_root / "config" / "dedup_decisions.json"
        if not path.exists():
            pytest.skip("dedup_decisions.json not present")
        with open(path) as f:
            data = json.load(f)
        assert "merge" in data
        assert isinstance(data["merge"], list)
        if data["merge"]:
            entry = data["merge"][0]
            assert "primary_ref" in entry
            assert "all_refs" in entry

    def test_variant_overrides_structure(self, repo_root):
        """config/variant_overrides.json should have expected structure."""
        path = repo_root / "config" / "variant_overrides.json"
        if not path.exists():
            pytest.skip("variant_overrides.json not present")
        with open(path) as f:
            data = json.load(f)
        assert "overrides" in data
        assert isinstance(data["overrides"], list)

    def test_string_match_results_valid(self, repo_root):
        """All string_match_results files should be valid JSON with required keys."""
        results_dir = repo_root / "data" / "documents"
        if not results_dir.exists():
            pytest.skip("No data/documents directory")
        results_files = list(results_dir.glob("*/string_match_results_*.json"))
        if not results_files:
            pytest.skip("No string_match_results files found")

        for path in results_files:
            with open(path) as f:
                data = json.load(f)
            assert "metadata" in data, f"Missing 'metadata' in {path}"
            assert "by_document" in data, f"Missing 'by_document' in {path}"
            assert "by_term" in data, f"Missing 'by_term' in {path}"
