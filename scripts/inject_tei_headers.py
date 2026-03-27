#!/usr/bin/env python3
"""
inject_tei_headers.py — Inject TEI headers into per-document XML files.

For each document XML in data/documents/{vol}/d*.xml, this script:
1. Reads the corresponding string_match_results_{vol}.json for annotations
2. Extracts metadata from the document XML (title, dates, doc number, subtype)
3. Builds a <teiHeader> with <fileDesc>, <settingDesc>, and <textClass>
4. Injects the header as the first child of the root element

Annotations become <term> elements under <keywords scheme="frus-subject-taxonomy">,
with @ref, @type, @category, and @subcategory attributes derived from the taxonomy.

Usage:
    python3 scripts/inject_tei_headers.py                    # skip files that already have teiHeader
    python3 scripts/inject_tei_headers.py --force             # overwrite existing teiHeaders
    python3 scripts/inject_tei_headers.py --vol frus1969-76v01  # process single volume
    python3 scripts/inject_tei_headers.py --dry-run           # preview without writing
"""

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path

from lxml import etree

# Resolve paths relative to repo root (one level up from scripts/)
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = REPO_ROOT / "data" / "documents"
CONFIG_DIR = REPO_ROOT / "config"

# Namespace map for FRUS TEI documents
NS = {"tei": "http://www.tei-c.org/ns/1.0", "frus": "http://history.state.gov/frus/ns/1.0"}


def slugify(text):
    """Convert a label to a kebab-case slug for use as an XML ID."""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text


def load_taxonomy_id_map():
    """
    Load the taxonomy label -> ID mapping from config/taxonomy_label_to_id.json.
    Falls back to generating from config/taxonomy.xml if the JSON doesn't exist.
    Returns dict mapping lowercased label -> canonical ID string.
    """
    json_path = CONFIG_DIR / "taxonomy_label_to_id.json"
    if json_path.exists():
        with open(json_path) as f:
            raw = json.load(f)
        return {k.lower(): v for k, v in raw.items()}

    # Fallback: generate from taxonomy.xml
    xml_path = CONFIG_DIR / "taxonomy.xml"
    if not xml_path.exists():
        print(f"WARNING: Neither {json_path} nor {xml_path} found. Using slugify fallback for all IDs.")
        return {}

    print(f"Generating taxonomy ID map from {xml_path}...")
    mapping = {}
    tree = etree.parse(str(xml_path))
    root = tree.getroot()

    categories = root.findall(".//category") or root.findall(".//{http://www.tei-c.org/ns/1.0}category")
    for cat in categories:
        xml_id = cat.get("{http://www.w3.org/XML/1998/namespace}id", cat.get("xml:id", cat.get("id", "")))
        label_el = (
            cat.find("catDesc")
            or cat.find("{http://www.tei-c.org/ns/1.0}catDesc")
            or cat.find("gloss")
            or cat.find("{http://www.tei-c.org/ns/1.0}gloss")
        )
        if label_el is not None and xml_id:
            label_text = (label_el.text or "").strip()
            if label_text:
                mapping[label_text.lower()] = xml_id

    with open(json_path, "w") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)
    print(f"  Saved {len(mapping)} entries to {json_path}")

    return mapping


def extract_doc_metadata(tree):
    """
    Extract metadata from a FRUS document XML tree.
    Returns dict with: title, doc_number, subtype, dates (list of date dicts).
    """
    root = tree.getroot()
    meta = {"title": "", "doc_number": "", "subtype": "", "dates": []}

    # Title: try <title> or <head> in various namespace forms
    for xpath in [
        ".//tei:head[@type='title']",
        ".//tei:head",
        ".//head[@type='title']",
        ".//head",
    ]:
        try:
            el = root.find(xpath, NS)
        except Exception:
            el = root.find(xpath)
        if el is not None:
            meta["title"] = "".join(el.itertext()).strip()
            break

    # Document number and subtype from div attributes
    for div in root.iter():
        if div.tag.endswith("div") or div.tag == "div":
            n = div.get("n", "")
            if n:
                meta["doc_number"] = n
                break
            subtype = div.get("subtype", "")
            if subtype:
                meta["subtype"] = subtype

    # Dates: look for <date> elements with temporal attributes
    for date_el in root.iter():
        tag = date_el.tag.split("}")[-1] if "}" in str(date_el.tag) else str(date_el.tag)
        if tag != "date":
            continue
        d = {}
        for attr in ["when", "notBefore", "notAfter", "from", "to", "when-iso"]:
            val = date_el.get(attr, "")
            if val:
                key = attr.replace("-", "_")
                d[key] = val
        if d:
            meta["dates"].append(d)

    return meta


def build_tei_header(vol_id, doc_id, doc_number, metadata, annotations, taxonomy_map):
    """
    Build a <teiHeader> element with fileDesc, settingDesc, and textClass.
    """
    header = etree.Element("teiHeader")

    # === fileDesc ===
    file_desc = etree.SubElement(header, "fileDesc")

    title_stmt = etree.SubElement(file_desc, "titleStmt")
    title_el = etree.SubElement(title_stmt, "title")
    title_el.text = metadata.get("title", "")

    source_desc = etree.SubElement(file_desc, "sourceDesc")

    def add_bibl(parent, bibl_type, text):
        bibl = etree.SubElement(parent, "bibl")
        bibl.set("type", bibl_type)
        bibl.text = text

    add_bibl(source_desc, "frus-volume-id", vol_id)
    add_bibl(source_desc, "frus-document-id", doc_id)
    if doc_number:
        add_bibl(source_desc, "frus-document-number", doc_number)
    subtype = metadata.get("subtype", "")
    if subtype:
        add_bibl(source_desc, "frus-div-subtype", subtype)

    # === profileDesc ===
    profile_desc = etree.SubElement(header, "profileDesc")

    # settingDesc with dates
    dates = metadata.get("dates", [])
    if dates:
        setting_desc = etree.SubElement(profile_desc, "settingDesc")
        setting = etree.SubElement(setting_desc, "setting")
        date_info = dates[0]
        date_el = etree.SubElement(setting, "date")
        for key, val in date_info.items():
            attr_name = key.replace("_", "-")
            if attr_name == "when-iso":
                attr_name = "when"
            date_el.set(attr_name, val)

    # textClass with annotations
    if annotations:
        text_class = etree.SubElement(profile_desc, "textClass")
        keywords = etree.SubElement(text_class, "keywords")
        keywords.set("scheme", "frus-subject-taxonomy")

        seen = set()
        for ann in annotations:
            matched = ann.get("matched_text", ann.get("term", ""))
            category = ann.get("category", "")
            subcategory = ann.get("subcategory", "")
            ann_type = ann.get("type", "topic")

            # Resolve the ref ID from taxonomy map
            ref = ""
            for lookup_key in [subcategory, matched, category]:
                if lookup_key:
                    ref = taxonomy_map.get(lookup_key.lower(), "")
                    if ref:
                        break
            if not ref:
                ref = ann.get("id", "") or slugify(matched or subcategory or category)

            cat_id = taxonomy_map.get(category.lower(), slugify(category)) if category else ""
            subcat_id = taxonomy_map.get(subcategory.lower(), slugify(subcategory)) if subcategory else ""

            dedup_key = (ref, matched)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            term_el = etree.SubElement(keywords, "term")
            term_el.set("ref", ref)
            term_el.set("type", ann_type)
            if cat_id:
                term_el.set("category", cat_id)
            if subcat_id:
                term_el.set("subcategory", subcat_id)
            term_el.text = matched

    return header


def inject_header_into_doc(doc_path, header, force=False):
    """
    Parse a document XML, inject the teiHeader as first child of root.
    If force=True, replace existing teiHeader. Otherwise skip if present.
    Returns True if the file was modified, False if skipped.
    """
    parser = etree.XMLParser(remove_blank_text=False)
    tree = etree.parse(str(doc_path), parser)
    root = tree.getroot()

    existing = root.find("teiHeader")
    if existing is None:
        existing = root.find("{http://www.tei-c.org/ns/1.0}teiHeader")

    if existing is not None:
        if not force:
            return False
        root.remove(existing)

    root.insert(0, header)

    tree.write(
        str(doc_path),
        xml_declaration=True,
        encoding="UTF-8",
        pretty_print=True,
    )
    return True


def process_volume(vol_id, taxonomy_map, force=False, dry_run=False):
    """Process all documents in a single volume."""
    vol_dir = DATA_DIR / vol_id
    if not vol_dir.is_dir():
        print(f"  WARNING: Volume directory not found: {vol_dir}")
        return 0, 0, 0

    # Load string match results
    results_path = vol_dir / f"string_match_results_{vol_id}.json"
    annotations_by_doc = {}
    if results_path.exists():
        with open(results_path) as f:
            results = json.load(f)
        if isinstance(results, dict):
            annotations_by_doc = results
        elif isinstance(results, list):
            for entry in results:
                doc_id = entry.get("document_id", entry.get("doc_id", ""))
                if doc_id:
                    annotations_by_doc.setdefault(doc_id, []).append(entry)

    processed = 0
    skipped = 0
    errors = 0

    doc_files = sorted(vol_dir.glob("d*.xml"))
    for doc_path in doc_files:
        doc_id = doc_path.stem
        doc_number = doc_id.lstrip("d") if doc_id.startswith("d") else ""

        try:
            parser = etree.XMLParser(remove_blank_text=False)
            tree = etree.parse(str(doc_path), parser)
            metadata = extract_doc_metadata(tree)

            if not metadata["doc_number"] and doc_number:
                metadata["doc_number"] = doc_number
            if not metadata["subtype"]:
                metadata["subtype"] = "historical-document"

            doc_annotations = annotations_by_doc.get(doc_id, [])
            if isinstance(doc_annotations, dict):
                doc_annotations = doc_annotations.get("annotations", doc_annotations.get("matches", []))

            header = build_tei_header(
                vol_id, doc_id, doc_number, metadata, doc_annotations, taxonomy_map
            )

            if dry_run:
                if doc_annotations:
                    print(f"    [DRY RUN] {doc_id}: would inject header with {len(doc_annotations)} annotation(s)")
                processed += 1
                continue

            modified = inject_header_into_doc(doc_path, header, force=force)
            if modified:
                processed += 1
            else:
                skipped += 1

        except Exception as e:
            errors += 1
            print(f"    ERROR processing {doc_path.name}: {e}")

    return processed, skipped, errors


def main():
    parser = argparse.ArgumentParser(description="Inject TEI headers into FRUS document XMLs")
    parser.add_argument("--vol", help="Process a single volume (e.g. frus1969-76v01)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing teiHeaders")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing files")
    args = parser.parse_args()

    print("=== FRUS TEI Header Injection ===")
    print(f"Data directory: {DATA_DIR}")
    print(f"Force overwrite: {args.force}")
    print(f"Dry run: {args.dry_run}")
    print()

    taxonomy_map = load_taxonomy_id_map()
    print(f"Loaded {len(taxonomy_map)} taxonomy label->ID mappings")
    print()

    if args.vol:
        volumes = [args.vol]
    else:
        if not DATA_DIR.exists():
            print(f"ERROR: Data directory not found: {DATA_DIR}")
            print("Run the split pipeline first: make split")
            sys.exit(1)
        volumes = sorted([d.name for d in DATA_DIR.iterdir() if d.is_dir()])

    total_processed = 0
    total_skipped = 0
    total_errors = 0
    start_time = time.time()

    for i, vol_id in enumerate(volumes, 1):
        print(f"[{i}/{len(volumes)}] Processing {vol_id}...")
        p, s, e = process_volume(vol_id, taxonomy_map, force=args.force, dry_run=args.dry_run)
        total_processed += p
        total_skipped += s
        total_errors += e
        if p or e:
            print(f"  -> {p} processed, {s} skipped, {e} errors")

    elapsed = time.time() - start_time
    print()
    print(f"=== Complete ===")
    print(f"Volumes: {len(volumes)}")
    print(f"Documents processed: {total_processed}")
    print(f"Documents skipped (existing header): {total_skipped}")
    print(f"Errors: {total_errors}")
    print(f"Time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
