"""Tests for annotate_documents.py — skip logic, CLI flags, and annotation correctness."""

import json
import os
import sys
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from lxml import etree

# Add scripts/ to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from annotate_documents import (
    compile_term_patterns,
    match_document,
    extract_sentence,
    extract_body_text,
    results_exist,
    has_documents,
    discover_volumes,
    annotate_volume,
    write_results,
    load_annotation_resources,
)


# ── Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def tmp_data_dir(tmp_path):
    """Create a temporary data/documents structure for testing."""
    docs_dir = tmp_path / "data" / "documents"
    docs_dir.mkdir(parents=True)
    return docs_dir


@pytest.fixture
def sample_volume(tmp_data_dir):
    """Create a minimal volume with one document for testing."""
    vol_id = "frus-test-v01"
    vol_dir = tmp_data_dir / vol_id
    vol_dir.mkdir()

    # Create a minimal TEI document
    doc_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0" xmlns:frus="http://history.state.gov/frus/ns/1.0">
    <teiHeader>
        <fileDesc>
            <titleStmt><title>Test Document</title></titleStmt>
            <sourceDesc>
                <bibl type="frus-volume-id">frus-test-v01</bibl>
                <bibl type="frus-document-id">d1</bibl>
            </sourceDesc>
        </fileDesc>
    </teiHeader>
    <text>
        <body>
            <div type="document" subtype="historical-document"
                 frus:doc-dateTime-min="1980-01-15T00:00:00-05:00"
                 frus:doc-dateTime-max="1980-01-15T23:59:59-05:00"
                 n="1" xml:id="d1">
                <head>Test Meeting on Arms Control</head>
                <p>The President discussed arms control and disarmament
                with the Soviet delegation. Human rights concerns were
                also raised during the bilateral negotiations.</p>
                <p>Economic sanctions against the regime were considered
                as part of foreign economic policy discussions.</p>
            </div>
        </body>
    </text>
</TEI>'''

    (vol_dir / "d1.xml").write_text(doc_xml)
    return vol_id, vol_dir


@pytest.fixture
def sample_volume_multi(tmp_data_dir):
    """Create a volume with multiple documents."""
    vol_id = "frus-test-v02"
    vol_dir = tmp_data_dir / vol_id
    vol_dir.mkdir()

    for i in range(1, 4):
        doc_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0" xmlns:frus="http://history.state.gov/frus/ns/1.0">
    <teiHeader>
        <fileDesc>
            <titleStmt><title>Document {i}</title></titleStmt>
            <sourceDesc>
                <bibl type="frus-volume-id">{vol_id}</bibl>
                <bibl type="frus-document-id">d{i}</bibl>
            </sourceDesc>
        </fileDesc>
    </teiHeader>
    <text>
        <body>
            <div type="document" subtype="historical-document"
                 frus:doc-dateTime-min="1980-0{i}-01T00:00:00-05:00"
                 n="{i}" xml:id="d{i}">
                <head>Document {i}</head>
                <p>This document discusses diplomacy and international relations
                in the context of the Cold War period.</p>
            </div>
        </body>
    </text>
</TEI>'''
        (vol_dir / f"d{i}.xml").write_text(doc_xml)

    return vol_id, vol_dir


# ── Core matching tests ───────────────────────────────────────

class TestCompileTermPatterns:
    """Tests for regex compilation."""

    def test_basic_compilation(self):
        terms = [
            {"term": "Human rights", "ref": "ref1", "type": "topic",
             "count": 10, "category": "HR", "subcategory": "General",
             "lcsh_uri": "", "lcsh_match": ""},
        ]
        pattern, lookup = compile_term_patterns(terms)
        assert "human rights" in lookup
        assert pattern.search("We discussed Human rights today")

    def test_longest_first(self):
        terms = [
            {"term": "National Security Council", "ref": "ref1", "type": "topic",
             "count": 10, "category": "C1", "subcategory": "S1",
             "lcsh_uri": "", "lcsh_match": ""},
            {"term": "National Security", "ref": "ref2", "type": "topic",
             "count": 5, "category": "C1", "subcategory": "S1",
             "lcsh_uri": "", "lcsh_match": ""},
        ]
        # Terms should already be sorted longest first
        terms.sort(key=lambda t: len(t["term"]), reverse=True)
        pattern, lookup = compile_term_patterns(terms)
        # The lookup should have both
        assert "national security council" in lookup
        assert "national security" in lookup

    def test_empty_terms(self):
        pattern, lookup = compile_term_patterns([])
        assert len(lookup) == 0

    def test_case_insensitive(self):
        terms = [
            {"term": "Terrorism", "ref": "ref1", "type": "topic",
             "count": 5, "category": "C1", "subcategory": "S1",
             "lcsh_uri": "", "lcsh_match": ""},
        ]
        pattern, lookup = compile_term_patterns(terms)
        assert pattern.search("TERRORISM is a concern")
        assert pattern.search("terrorism is a concern")
        assert pattern.search("Terrorism is a concern")


class TestMatchDocument:
    """Tests for the main matching logic."""

    def test_basic_match(self):
        terms = [
            {"term": "Arms control", "ref": "ref1", "type": "topic",
             "count": 10, "category": "AC", "subcategory": "General",
             "lcsh_uri": "", "lcsh_match": ""},
        ]
        terms.sort(key=lambda t: len(t["term"]), reverse=True)
        compiled = compile_term_patterns(terms)
        matches = match_document("We discussed arms control today.", compiled)
        assert len(matches) == 1
        assert matches[0]["term"] == "Arms control"
        assert matches[0]["ref"] == "ref1"

    def test_longest_match_wins(self):
        terms = [
            {"term": "National Security Council", "ref": "ref1", "type": "topic",
             "count": 10, "category": "C1", "subcategory": "S1",
             "lcsh_uri": "", "lcsh_match": ""},
            {"term": "National Security", "ref": "ref2", "type": "topic",
             "count": 5, "category": "C1", "subcategory": "S1",
             "lcsh_uri": "", "lcsh_match": ""},
        ]
        terms.sort(key=lambda t: len(t["term"]), reverse=True)
        compiled = compile_term_patterns(terms)
        matches = match_document(
            "The National Security Council met yesterday.", compiled
        )
        # Should match "National Security Council" not "National Security"
        assert len(matches) == 1
        assert matches[0]["ref"] == "ref1"

    def test_multiple_matches(self):
        terms = [
            {"term": "Arms control", "ref": "ref1", "type": "topic",
             "count": 10, "category": "C1", "subcategory": "S1",
             "lcsh_uri": "", "lcsh_match": ""},
            {"term": "Human rights", "ref": "ref2", "type": "topic",
             "count": 8, "category": "C2", "subcategory": "S2",
             "lcsh_uri": "", "lcsh_match": ""},
        ]
        terms.sort(key=lambda t: len(t["term"]), reverse=True)
        compiled = compile_term_patterns(terms)
        text = "Arms control and human rights were discussed."
        matches = match_document(text, compiled)
        assert len(matches) == 2
        refs = {m["ref"] for m in matches}
        assert refs == {"ref1", "ref2"}

    def test_empty_text(self):
        terms = [
            {"term": "Test", "ref": "ref1", "type": "topic",
             "count": 1, "category": "C1", "subcategory": "S1",
             "lcsh_uri": "", "lcsh_match": ""},
        ]
        compiled = compile_term_patterns(terms)
        matches = match_document("", compiled)
        assert matches == []

    def test_no_matches(self):
        terms = [
            {"term": "Quantum physics", "ref": "ref1", "type": "topic",
             "count": 1, "category": "C1", "subcategory": "S1",
             "lcsh_uri": "", "lcsh_match": ""},
        ]
        compiled = compile_term_patterns(terms)
        matches = match_document("This text has nothing relevant.", compiled)
        assert matches == []

    def test_word_boundary(self):
        """Terms should only match at word boundaries."""
        terms = [
            {"term": "Oil", "ref": "ref1", "type": "topic",
             "count": 10, "category": "C1", "subcategory": "S1",
             "lcsh_uri": "", "lcsh_match": ""},
        ]
        terms.sort(key=lambda t: len(t["term"]), reverse=True)
        compiled = compile_term_patterns(terms)
        # Should match standalone "oil"
        assert len(match_document("We need oil imports.", compiled)) == 1
        # Should NOT match "oil" inside "toil" or "oilskin"
        assert len(match_document("They toil daily.", compiled)) == 0


class TestExtractSentence:
    """Tests for sentence extraction."""

    def test_basic_sentence(self):
        text = "First sentence. The match is here. Third sentence."
        sentence = extract_sentence(text, 20, 25)  # "match"
        assert "match" in sentence

    def test_truncation(self):
        long_text = "A" * 500 + " match " + "B" * 500
        sentence = extract_sentence(long_text, 500, 505, max_chars=100)
        assert len(sentence) <= 110  # Allow for "..." markers


# ── Skip logic tests ─────────────────────────────────────────

class TestSkipLogic:
    """Tests for skip-if-exists and force logic."""

    def test_results_exist_true(self, tmp_data_dir):
        """results_exist returns True when results JSON exists."""
        vol_id = "frus-test-exists"
        vol_dir = tmp_data_dir / vol_id
        vol_dir.mkdir()
        (vol_dir / f"string_match_results_{vol_id}.json").write_text("{}")

        with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_data_dir)):
            assert results_exist(vol_id) is True

    def test_results_exist_false(self, tmp_data_dir):
        """results_exist returns False when no results JSON exists."""
        vol_id = "frus-test-noexist"
        vol_dir = tmp_data_dir / vol_id
        vol_dir.mkdir()

        with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_data_dir)):
            assert results_exist(vol_id) is False

    def test_results_exist_no_dir(self, tmp_data_dir):
        """results_exist returns False when volume directory doesn't exist."""
        with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_data_dir)):
            assert results_exist("frus-nonexistent") is False

    def test_has_documents_true(self, sample_volume, tmp_data_dir):
        """has_documents returns True when d*.xml files exist."""
        vol_id, _ = sample_volume
        with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_data_dir)):
            assert has_documents(vol_id) is True

    def test_has_documents_false_empty(self, tmp_data_dir):
        """has_documents returns False for empty directory."""
        vol_dir = tmp_data_dir / "frus-empty"
        vol_dir.mkdir()
        with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_data_dir)):
            assert has_documents("frus-empty") is False

    def test_has_documents_false_missing(self, tmp_data_dir):
        """has_documents returns False for nonexistent directory."""
        with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_data_dir)):
            assert has_documents("frus-missing") is False


class TestDiscoverVolumes:
    """Tests for volume discovery."""

    def test_discovers_volumes(self, tmp_data_dir, sample_volume, sample_volume_multi):
        """discover_volumes finds all volumes with split documents."""
        with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_data_dir)):
            volumes = discover_volumes()
        assert "frus-test-v01" in volumes
        assert "frus-test-v02" in volumes

    def test_skips_empty_dirs(self, tmp_data_dir):
        """discover_volumes skips directories without d*.xml files."""
        (tmp_data_dir / "empty-vol").mkdir()
        with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_data_dir)):
            volumes = discover_volumes()
        assert "empty-vol" not in volumes

    def test_empty_documents_dir(self, tmp_path):
        """discover_volumes returns empty list for nonexistent base dir."""
        with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_path / "nonexistent")):
            volumes = discover_volumes()
        assert volumes == []


# ── Integration tests with real repo data ─────────────────────

class TestAnnotateVolumeIntegration:
    """Integration tests that use the real taxonomy and config files."""

    @pytest.fixture
    def repo_root(self):
        return Path(__file__).resolve().parent.parent

    @pytest.fixture
    def resources(self, repo_root):
        """Load real annotation resources (slow, but tests the full chain)."""
        orig_dir = os.getcwd()
        os.chdir(str(repo_root / "scripts"))
        try:
            return load_annotation_resources(quiet=True)
        finally:
            os.chdir(orig_dir)

    def test_load_resources(self, resources):
        """Annotation resources should load successfully."""
        assert resources["compiled"] is not None
        assert len(resources["terms"]) > 100
        assert isinstance(resources["ref_to_canonical"], dict)
        assert isinstance(resources["canonical_info"], dict)

    def test_annotate_sample_document(self, resources, tmp_path):
        """Annotate a single synthetic document and verify results."""
        vol_id = "frus-integration-test"
        vol_dir = tmp_path / vol_id
        vol_dir.mkdir()

        doc_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0" xmlns:frus="http://history.state.gov/frus/ns/1.0">
    <teiHeader><fileDesc><titleStmt><title>Test</title></titleStmt>
    <sourceDesc><bibl type="frus-volume-id">frus-integration-test</bibl></sourceDesc>
    </fileDesc></teiHeader>
    <text><body>
        <div type="document" subtype="historical-document" n="1" xml:id="d1"
             frus:doc-dateTime-min="1980-01-01T00:00:00-05:00">
            <head>Test Document</head>
            <p>The discussion focused on human rights and arms control.</p>
        </div>
    </body></text>
</TEI>'''
        (vol_dir / "d1.xml").write_text(doc_xml)

        orig_dir = os.getcwd()
        os.chdir(str(Path(__file__).resolve().parent.parent / "scripts"))
        try:
            results = annotate_volume(vol_id, resources, docs_dir=str(vol_dir), quiet=True)
        finally:
            os.chdir(orig_dir)

        assert results is not None
        assert results["metadata"]["volume_id"] == vol_id
        assert results["metadata"]["total_documents"] == 1
        assert results["metadata"]["total_matches"] > 0
        assert "d1" in results["by_document"]

    def test_write_results_creates_file(self, resources, tmp_path):
        """write_results should create the JSON file in the right place."""
        vol_id = "frus-write-test"
        vol_dir = tmp_path / vol_id
        vol_dir.mkdir()

        doc_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0" xmlns:frus="http://history.state.gov/frus/ns/1.0">
    <teiHeader><fileDesc><titleStmt><title>Test</title></titleStmt>
    <sourceDesc><bibl type="frus-volume-id">frus-write-test</bibl></sourceDesc>
    </fileDesc></teiHeader>
    <text><body>
        <div type="document" subtype="historical-document" n="1" xml:id="d1"
             frus:doc-dateTime-min="1980-01-01T00:00:00-05:00">
            <head>Test</head>
            <p>Diplomacy and negotiations.</p>
        </div>
    </body></text>
</TEI>'''
        (vol_dir / "d1.xml").write_text(doc_xml)

        orig_dir = os.getcwd()
        os.chdir(str(Path(__file__).resolve().parent.parent / "scripts"))
        try:
            results = annotate_volume(vol_id, resources, docs_dir=str(vol_dir), quiet=True)
            with patch("annotate_documents.DOCUMENTS_DIR", str(tmp_path)):
                output_path = write_results(vol_id, results)
        finally:
            os.chdir(orig_dir)

        assert os.path.exists(output_path)
        with open(output_path) as f:
            saved = json.load(f)
        assert saved["metadata"]["volume_id"] == vol_id

    def test_annotate_empty_volume(self, resources, tmp_path):
        """Annotating a volume with no documents should return None."""
        vol_dir = tmp_path / "frus-empty-vol"
        vol_dir.mkdir()

        orig_dir = os.getcwd()
        os.chdir(str(Path(__file__).resolve().parent.parent / "scripts"))
        try:
            results = annotate_volume("frus-empty-vol", resources,
                                      docs_dir=str(vol_dir), quiet=True)
        finally:
            os.chdir(orig_dir)

        assert results is None


# ── CLI flag tests ────────────────────────────────────────────

class TestCLIFlags:
    """Tests for CLI argument parsing."""

    def test_parse_all(self):
        from annotate_documents import parse_args
        with patch("sys.argv", ["annotate_documents.py", "--all"]):
            args = parse_args()
        assert args.all is True
        assert args.force is False
        assert args.workers == 1

    def test_parse_force(self):
        from annotate_documents import parse_args
        with patch("sys.argv", ["annotate_documents.py", "--all", "--force"]):
            args = parse_args()
        assert args.all is True
        assert args.force is True

    def test_parse_workers(self):
        from annotate_documents import parse_args
        with patch("sys.argv", ["annotate_documents.py", "--all", "--workers", "4"]):
            args = parse_args()
        assert args.workers == 4

    def test_parse_volume(self):
        from annotate_documents import parse_args
        with patch("sys.argv", ["annotate_documents.py", "--volume", "frus1981-88v10"]):
            args = parse_args()
        assert args.volumes == ["frus1981-88v10"]

    def test_parse_multiple_volumes(self):
        from annotate_documents import parse_args
        with patch("sys.argv", ["annotate_documents.py",
                                "--volume", "frus1981-88v10",
                                "--volume", "frus1981-88v03"]):
            args = parse_args()
        assert args.volumes == ["frus1981-88v10", "frus1981-88v03"]

    def test_parse_dry_run(self):
        from annotate_documents import parse_args
        with patch("sys.argv", ["annotate_documents.py", "--all", "--dry-run"]):
            args = parse_args()
        assert args.dry_run is True

    def test_parse_legacy_positional(self):
        from annotate_documents import parse_args
        with patch("sys.argv", ["annotate_documents.py", "../data/documents/frus-test"]):
            args = parse_args()
        assert args.docs_dir == "../data/documents/frus-test"
        assert args.all is False
