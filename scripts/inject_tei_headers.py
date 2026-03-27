#!/usr/bin/env python3
"""
Inject pared-down TEI headers into per-document XML files.

Reads existing document XMLs and their corresponding string_match_results JSON
to produce enriched documents with a <teiHeader> containing:

  - fileDesc: volume ID, document ID, document number, subtype, title
  - profileDesc/textClass/keywords: subject taxonomy annotations (with slug IDs)
  - profileDesc/settingDesc: date range from frus:doc-dateTime-min/max

The original <text><body> content is preserved unchanged.

Usage:
    python3 scripts/inject_tei_headers.py                     # All volumes
    python3 scripts/inject_tei_headers.py frus1981-88v06      # Single volume
    python3 scripts/inject_tei_headers.py --dry-run frus1861  # Preview only
    python3 scripts/inject_tei_headers.py --force             # Replace existing headers
"""

import sys
import os
import re
import json
import argparse
from pathlib import Path
from lxml import etree
from datetime import datetime

TEI_NS = "http://www.tei-c.org/ns/1.0"
FRUS_NS = "http://history.state.gov/frus/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"
NSMAP = {"tei": TEI_NS, "frus": FRUS_NS}

BASE_DIR = Path(__file__).resolve().parent.parent
DOCS_DIR = BASE_DIR / "data" / "documents"
LABEL_MAP_PATH = BASE_DIR / "config" / "taxonomy_label_to_id.json"

# Label → canonical ID mapping from the authoritative taxonomy.xml
_label_to_id: dict = {}


def _load_label_map():
    """Load the canonical label→ID mapping from config."""
    global _label_to_id
    if _label_to_id:
        return
    if LABEL_MAP_PATH.exists():
        with open(LABEL_MAP_PATH) as f:
            _label_to_id = json.load(f)
    else:
        print(f"  WARNING: {LABEL_MAP_PATH} not found; falling back to slugify")


def slugify(label: str) -> str:
    """Convert a human-readable label to a kebab-case slug ID.

    Matches the ID convention used in the FRUS subject taxonomy:
      "Arms Control and Disarmament" → "arms-control-and-disarmament"
      "Nuclear Weapons" → "nuclear-weapons"
      "Buildings: Domestic" → "buildings-domestic"
      "U.S.-Soviet/Russian Relations" → "u-s-soviet-russian-relations"
    """
    s = label.lower()
    # Replace common separators with hyphens
    s = s.replace(":", "-").replace("/", "-").replace(".", "-")
    # Replace any non-alphanumeric (except hyphen) with hyphen
    s = re.sub(r"[^a-z0-9-]", "-", s)
    # Collapse multiple hyphens and strip leading/trailing
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def label_to_id(label: str) -> str:
    """Resolve a label to its canonical taxonomy ID, falling back to slugify."""
    _load_label_map()
    if label in _label_to_id:
        return _label_to_id[label]
    return slugify(label)


def load_annotations(volume_id: str) -> dict:
    """Load string match results for a volume. Returns by_document dict."""
    json_path = DOCS_DIR / volume_id / f"string_match_results_{volume_id}.json"
    if not json_path.exists():
        return {}
    with open(json_path) as f:
        data = json.load(f)
    return data.get("by_document", {})


def extract_title_from_head(div_el) -> str:
    """Extract document title from <head> element, stripping footnotes."""
    head = div_el.find(f"{{{TEI_NS}}}head")
    if head is None:
        return ""
    # Get text content but skip <note> children
    parts = []
    if head.text:
        parts.append(head.text)
    for child in head:
        tag = etree.QName(child.tag).localname if isinstance(child.tag, str) else ""
        if tag == "note":
            # Skip footnotes but include their tail
            if child.tail:
                parts.append(child.tail)
        else:
            parts.append(etree.tostring(child, method="text", encoding="unicode"))
            if child.tail:
                parts.append(child.tail)
    return "".join(parts).strip()


def build_tei_header(volume_id: str, doc_id: str, div_el, matches: list) -> etree._Element:
    """Build a <teiHeader> element with metadata and annotations."""
    header = etree.Element(f"{{{TEI_NS}}}teiHeader")

    # --- fileDesc ---
    file_desc = etree.SubElement(header, f"{{{TEI_NS}}}fileDesc")

    # titleStmt
    title_stmt = etree.SubElement(file_desc, f"{{{TEI_NS}}}titleStmt")
    title_el = etree.SubElement(title_stmt, f"{{{TEI_NS}}}title")
    title_el.text = extract_title_from_head(div_el)

    # sourceDesc with document identification
    source_desc = etree.SubElement(file_desc, f"{{{TEI_NS}}}sourceDesc")

    doc_number = div_el.get("n", "")
    doc_subtype = div_el.get("subtype", "")

    bibl_vol = etree.SubElement(source_desc, f"{{{TEI_NS}}}bibl")
    bibl_vol.set("type", "frus-volume-id")
    bibl_vol.text = volume_id

    bibl_doc = etree.SubElement(source_desc, f"{{{TEI_NS}}}bibl")
    bibl_doc.set("type", "frus-document-id")
    bibl_doc.text = doc_id

    if doc_number:
        bibl_num = etree.SubElement(source_desc, f"{{{TEI_NS}}}bibl")
        bibl_num.set("type", "frus-document-number")
        bibl_num.text = doc_number

    bibl_sub = etree.SubElement(source_desc, f"{{{TEI_NS}}}bibl")
    bibl_sub.set("type", "frus-div-subtype")
    bibl_sub.text = doc_subtype

    # --- profileDesc ---
    profile_desc = etree.SubElement(header, f"{{{TEI_NS}}}profileDesc")

    # settingDesc with date
    date_min = div_el.get(f"{{{FRUS_NS}}}doc-dateTime-min", "")
    date_max = div_el.get(f"{{{FRUS_NS}}}doc-dateTime-max", "")
    if date_min or date_max:
        setting_desc = etree.SubElement(profile_desc, f"{{{TEI_NS}}}settingDesc")
        setting = etree.SubElement(setting_desc, f"{{{TEI_NS}}}setting")
        date_el = etree.SubElement(setting, f"{{{TEI_NS}}}date")
        if date_min:
            date_el.set("notBefore", date_min)
        if date_max:
            date_el.set("notAfter", date_max)

    # textClass/keywords with subject annotations
    if matches:
        text_class = etree.SubElement(profile_desc, f"{{{TEI_NS}}}textClass")
        keywords = etree.SubElement(text_class, f"{{{TEI_NS}}}keywords")
        keywords.set("scheme", "frus-subject-taxonomy")

        # Deduplicate by canonical_ref (multiple position hits → one term entry)
        seen_refs = set()
        for match in matches:
            cref = match.get("canonical_ref", match.get("ref", ""))
            if cref in seen_refs:
                continue
            seen_refs.add(cref)

            term_el = etree.SubElement(keywords, f"{{{TEI_NS}}}term")
            term_el.set("ref", cref)
            term_el.set("type", match.get("type", ""))

            # Use canonical taxonomy IDs for category/subcategory
            category = match.get("category", "")
            subcategory = match.get("subcategory", "")
            if category:
                term_el.set("category", label_to_id(category))
            if subcategory:
                term_el.set("subcategory", label_to_id(subcategory))

            if match.get("lcsh_uri"):
                term_el.set("lcsh-uri", match["lcsh_uri"])
            if match.get("lcsh_match"):
                term_el.set("lcsh-match", match["lcsh_match"])
            term_el.text = match.get("term", "")

    return header


def inject_header_into_document(doc_path: Path, volume_id: str, doc_id: str,
                                 annotations: dict, dry_run: bool = False,
                                 force: bool = False) -> bool:
    """
    Inject a TEI header into an existing document XML file.
    Returns True if the file was modified.
    """
    tree = etree.parse(str(doc_path))
    root = tree.getroot()

    # Check if teiHeader already exists
    existing_header = root.find(f"{{{TEI_NS}}}teiHeader")
    if existing_header is not None:
        if force:
            root.remove(existing_header)
        else:
            return False

    # Find the <div> with document metadata
    div_el = root.find(f".//{{{TEI_NS}}}div[@type='document']")
    if div_el is None:
        div_el = root.find(f".//{{{TEI_NS}}}div")
    if div_el is None:
        return False

    # Get matches for this document
    doc_data = annotations.get(doc_id, {})
    matches = doc_data.get("matches", [])

    # Build the header
    header = build_tei_header(volume_id, doc_id, div_el, matches)

    if dry_run:
        print(etree.tostring(header, pretty_print=True, encoding="unicode"))
        return False

    # Insert header as first child of <TEI> root
    root.insert(0, header)

    # Write back with proper indentation
    etree.indent(root, space="    ", level=0)
    tree.write(
        str(doc_path),
        xml_declaration=True,
        encoding="UTF-8",
        pretty_print=True,
    )
    return True


def process_volume(volume_id: str, dry_run: bool = False, limit: int = 0,
                   force: bool = False) -> tuple:
    """
    Process all documents in a volume. Returns (modified, skipped, errors).
    """
    vol_dir = DOCS_DIR / volume_id
    if not vol_dir.is_dir():
        print(f"  ERROR: Volume directory not found: {vol_dir}")
        return (0, 0, 1)

    annotations = load_annotations(volume_id)

    doc_files = sorted(
        [f for f in vol_dir.iterdir() if f.suffix == ".xml" and f.stem.startswith("d")],
        key=lambda p: (len(p.stem), p.stem),
    )
    if limit:
        doc_files = doc_files[:limit]

    modified = 0
    skipped = 0
    errors = 0

    for doc_path in doc_files:
        doc_id = doc_path.stem
        try:
            if inject_header_into_document(doc_path, volume_id, doc_id, annotations,
                                            dry_run, force):
                modified += 1
            else:
                skipped += 1
        except Exception as e:
            print(f"  ERROR: {volume_id}/{doc_id}: {e}")
            errors += 1

    return (modified, skipped, errors)


def main():
    parser = argparse.ArgumentParser(description="Inject TEI headers into document XMLs")
    parser.add_argument("volumes", nargs="*", help="Volume IDs (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Preview header for first doc only")
    parser.add_argument("--limit", type=int, default=0, help="Max docs per volume (0=all)")
    parser.add_argument("--force", action="store_true",
                        help="Replace existing headers (default: skip files with headers)")
    args = parser.parse_args()

    if args.volumes:
        volume_ids = args.volumes
    else:
        volume_ids = sorted(
            d for d in os.listdir(DOCS_DIR)
            if (DOCS_DIR / d).is_dir() and d.startswith("frus")
        )

    print(f"Processing {len(volume_ids)} volume(s)...")
    if args.dry_run:
        print("(DRY RUN — no files will be modified)\n")
    if args.force:
        print("(FORCE — replacing existing headers)\n")

    total_modified = 0
    total_skipped = 0
    total_errors = 0
    start = datetime.now()

    for i, vol_id in enumerate(volume_ids, 1):
        if not args.dry_run and len(volume_ids) > 1:
            print(f"  [{i}/{len(volume_ids)}] {vol_id}...", end=" ", flush=True)

        modified, skipped, errors = process_volume(vol_id, args.dry_run, args.limit,
                                                    args.force)
        total_modified += modified
        total_skipped += skipped
        total_errors += errors

        if not args.dry_run and len(volume_ids) > 1:
            print(f"{modified} modified, {skipped} skipped, {errors} errors")

        if args.dry_run:
            break  # Only preview one volume in dry-run

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\nDone in {elapsed:.1f}s. {total_modified} modified, {total_skipped} skipped, {total_errors} errors.")


if __name__ == "__main__":
    main()
