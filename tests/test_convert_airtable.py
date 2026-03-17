"""Tests for convert_airtable_annotations.py."""

import json
import sys
from pathlib import Path

import pytest
from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from convert_airtable_annotations import (
    parse_airtable_xml,
    build_results,
    get_document_metadata,
    ensure_volume_split,
)


@pytest.fixture
def tmp_repo(tmp_path, monkeypatch):
    """Create a temporary repo structure."""
    monkeypatch.setattr("convert_airtable_annotations.BASE_DIR", tmp_path)
    (tmp_path / "annotations").mkdir()
    (tmp_path / "data" / "documents").mkdir(parents=True)
    (tmp_path / "volumes").mkdir()
    return tmp_path


def make_annotation_xml(tmp_path, volume_id, entries):
    """Create a minimal Airtable annotation XML."""
    lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<annotations>"]
    lines.append(f"  <volume_id>{volume_id}</volume_id>")
    for entry in entries:
        lines.append("  <entry>")
        lines.append(f"    <recordID>{entry['record_id']}</recordID>")
        lines.append(f"    <table_name>{entry.get('table', 'subjects')}</table_name>")
        lines.append(f"    <annotation_content>{entry['content']}</annotation_content>")
        lines.append(f"    <entity_name>{entry.get('entity', entry['content'])}</entity_name>")
        if entry.get("documents"):
            lines.append("    <documents>")
            for doc in entry["documents"]:
                lines.append(f"      <document><doc_number>{volume_id}#{doc}</doc_number></document>")
            lines.append("    </documents>")
        lines.append("  </entry>")
    lines.append("</annotations>")

    xml_path = tmp_path / "annotations" / f"annotations_{volume_id}.xml"
    xml_path.write_text("\n".join(lines))
    return xml_path


def make_split_doc(tmp_repo, volume_id, doc_id, title="Test Doc", date="1970-01-01"):
    """Create a minimal split document XML."""
    TEI_NS = "http://www.tei-c.org/ns/1.0"
    FRUS_NS = "http://history.state.gov/frus/ns/1.0"
    root = etree.Element(f"{{{TEI_NS}}}TEI", nsmap={None: TEI_NS, "frus": FRUS_NS})
    text = etree.SubElement(root, f"{{{TEI_NS}}}text")
    body = etree.SubElement(text, f"{{{TEI_NS}}}body")
    div = etree.SubElement(body, f"{{{TEI_NS}}}div", attrib={
        "type": "document",
        "subtype": "historical-document",
        "{http://www.w3.org/XML/1998/namespace}id": doc_id,
        f"{{{FRUS_NS}}}doc-dateTime-min": f"{date}T00:00:00-05:00",
    })
    head = etree.SubElement(div, f"{{{TEI_NS}}}head")
    head.text = title

    doc_dir = tmp_repo / "data" / "documents" / volume_id
    doc_dir.mkdir(parents=True, exist_ok=True)
    doc_path = doc_dir / f"{doc_id}.xml"
    etree.ElementTree(root).write(str(doc_path), xml_declaration=True, encoding="UTF-8")


def test_parse_airtable_xml(tmp_repo):
    """Should parse volume_id and entries from annotation XML."""
    xml_path = make_annotation_xml(tmp_repo, "frus-test", [
        {"record_id": "rec1", "content": "Cold War", "documents": ["d1", "d2"]},
        {"record_id": "rec2", "content": "NATO", "documents": ["d1"]},
    ])

    vol_id, entries = parse_airtable_xml(xml_path)

    assert vol_id == "frus-test"
    assert len(entries) == 2
    assert entries[0]["recordID"] == "rec1"
    assert entries[0]["documents"] == ["d1", "d2"]
    assert entries[1]["recordID"] == "rec2"


def test_parse_airtable_xml_invalid(tmp_path):
    """Invalid XML should return None and empty list."""
    bad_xml = tmp_path / "bad.xml"
    bad_xml.write_text("not xml at all {{{")

    vol_id, entries = parse_airtable_xml(bad_xml)

    assert vol_id is None
    assert entries == []


def test_build_results_structure(tmp_repo):
    """build_results should produce correct result structure."""
    entries = [
        {
            "recordID": "rec1",
            "table_name": "subjects",
            "annotation_content": "Cold War",
            "entity_name": "Cold War",
            "documents": ["d1", "d2"],
        },
    ]

    result = build_results("frus-test", entries)

    assert "metadata" in result
    assert "by_document" in result
    assert "by_term" in result
    assert result["metadata"]["volume_id"] == "frus-test"
    assert result["metadata"]["total_matches"] == 2
    assert result["metadata"]["unique_terms_matched"] == 1
    assert "d1" in result["by_document"]
    assert "d2" in result["by_document"]
    assert "rec1" in result["by_term"]


def test_build_results_by_document_fields(tmp_repo):
    """Each document entry should have required fields."""
    entries = [
        {
            "recordID": "rec1",
            "table_name": "subjects",
            "annotation_content": "Cold War",
            "entity_name": "Cold War",
            "documents": ["d1"],
        },
    ]

    result = build_results("frus-test", entries)
    doc = result["by_document"]["d1"]

    assert "title" in doc
    assert "date" in doc
    assert "doc_type" in doc
    assert "match_count" in doc
    assert "matches" in doc
    assert doc["match_count"] == 1


def test_get_document_metadata_with_split_doc(tmp_repo):
    """Should extract metadata from split document XML."""
    make_split_doc(tmp_repo, "frus-test", "d1", title="Test Title", date="1975-03-15")

    meta = get_document_metadata("frus-test", "d1")

    assert meta["title"] == "Test Title"
    assert "1975" in meta["date"]
    assert meta["doc_type"] == "historical-document"


def test_get_document_metadata_missing_doc(tmp_repo):
    """Missing doc should return empty strings."""
    meta = get_document_metadata("frus-test", "d999")

    assert meta["title"] == ""
    assert meta["date"] == ""
    assert meta["doc_type"] == ""


def test_ensure_volume_split_true(tmp_repo):
    """Should return True when split docs exist."""
    make_split_doc(tmp_repo, "frus-test", "d1")
    assert ensure_volume_split("frus-test") is True


def test_ensure_volume_split_false(tmp_repo):
    """Should return False when no split docs exist."""
    assert ensure_volume_split("frus-nonexistent") is False
