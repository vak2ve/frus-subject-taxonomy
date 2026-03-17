"""Tests for split_volume.py."""

import os
import sys
import tempfile
from pathlib import Path

import pytest
from lxml import etree

# Add scripts directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from split_volume import split_volume, TEI_NS, FRUS_NS


@pytest.fixture
def tmp_repo(tmp_path):
    """Create a temporary repo structure with a minimal volume XML."""
    (tmp_path / "volumes").mkdir()
    (tmp_path / "data" / "documents").mkdir(parents=True)
    return tmp_path


def make_volume_xml(volume_id, doc_ids, tmp_repo):
    """Create a minimal TEI volume XML with given document IDs."""
    root = etree.Element(
        f"{{{TEI_NS}}}TEI",
        nsmap={None: TEI_NS, "frus": FRUS_NS},
    )
    text = etree.SubElement(root, f"{{{TEI_NS}}}text")
    body = etree.SubElement(text, f"{{{TEI_NS}}}body")
    for doc_id in doc_ids:
        div = etree.SubElement(
            body,
            f"{{{TEI_NS}}}div",
            attrib={
                "type": "document",
                "subtype": "historical-document",
                "{http://www.w3.org/XML/1998/namespace}id": doc_id,
            },
        )
        head = etree.SubElement(div, f"{{{TEI_NS}}}head")
        head.text = f"Document {doc_id}"
        p = etree.SubElement(div, f"{{{TEI_NS}}}p")
        p.text = "Some content."

    vol_path = tmp_repo / "volumes" / f"{volume_id}.xml"
    etree.ElementTree(root).write(str(vol_path), xml_declaration=True, encoding="UTF-8")
    return vol_path


def test_split_volume_creates_files(tmp_repo, monkeypatch):
    """split_volume should create one XML file per document div."""
    monkeypatch.setattr("split_volume.BASE_DIR", tmp_repo)
    make_volume_xml("testvol", ["d1", "d2", "d3"], tmp_repo)

    count = split_volume("testvol")

    assert count == 3
    out_dir = tmp_repo / "data" / "documents" / "testvol"
    assert out_dir.exists()
    assert (out_dir / "d1.xml").exists()
    assert (out_dir / "d2.xml").exists()
    assert (out_dir / "d3.xml").exists()


def test_split_volume_output_is_valid_xml(tmp_repo, monkeypatch):
    """Each split document should be valid XML with a TEI root."""
    monkeypatch.setattr("split_volume.BASE_DIR", tmp_repo)
    make_volume_xml("testvol", ["d1"], tmp_repo)

    split_volume("testvol")

    doc_path = tmp_repo / "data" / "documents" / "testvol" / "d1.xml"
    tree = etree.parse(str(doc_path))
    root = tree.getroot()
    assert root.tag == "TEI" or root.tag == f"{{{TEI_NS}}}TEI"


def test_split_volume_missing_file(tmp_repo, monkeypatch):
    """split_volume should return 0 if the volume file doesn't exist."""
    monkeypatch.setattr("split_volume.BASE_DIR", tmp_repo)

    count = split_volume("nonexistent")

    assert count == 0


def test_split_volume_no_documents(tmp_repo, monkeypatch):
    """split_volume should return 0 if no document divs found."""
    monkeypatch.setattr("split_volume.BASE_DIR", tmp_repo)

    # Create a TEI file with no document divs
    root = etree.Element(f"{{{TEI_NS}}}TEI", nsmap={None: TEI_NS})
    text = etree.SubElement(root, f"{{{TEI_NS}}}text")
    body = etree.SubElement(text, f"{{{TEI_NS}}}body")
    p = etree.SubElement(body, f"{{{TEI_NS}}}p")
    p.text = "No documents here."

    vol_path = tmp_repo / "volumes" / "emptyvol.xml"
    etree.ElementTree(root).write(str(vol_path), xml_declaration=True, encoding="UTF-8")

    count = split_volume("emptyvol")

    assert count == 0


def test_split_volume_skips_divs_without_id(tmp_repo, monkeypatch):
    """Divs without xml:id should be skipped."""
    monkeypatch.setattr("split_volume.BASE_DIR", tmp_repo)

    root = etree.Element(f"{{{TEI_NS}}}TEI", nsmap={None: TEI_NS, "frus": FRUS_NS})
    text = etree.SubElement(root, f"{{{TEI_NS}}}text")
    body = etree.SubElement(text, f"{{{TEI_NS}}}body")

    # Div with ID
    div1 = etree.SubElement(body, f"{{{TEI_NS}}}div", attrib={
        "type": "document", "subtype": "historical-document",
        "{http://www.w3.org/XML/1998/namespace}id": "d1",
    })
    etree.SubElement(div1, f"{{{TEI_NS}}}p").text = "Content"

    # Div without ID
    div2 = etree.SubElement(body, f"{{{TEI_NS}}}div", attrib={
        "type": "document", "subtype": "historical-document",
    })
    etree.SubElement(div2, f"{{{TEI_NS}}}p").text = "No ID content"

    vol_path = tmp_repo / "volumes" / "mixedvol.xml"
    etree.ElementTree(root).write(str(vol_path), xml_declaration=True, encoding="UTF-8")

    split_volume("mixedvol")

    out_dir = tmp_repo / "data" / "documents" / "mixedvol"
    assert (out_dir / "d1.xml").exists()
    files = list(out_dir.glob("*.xml"))
    assert len(files) == 1
