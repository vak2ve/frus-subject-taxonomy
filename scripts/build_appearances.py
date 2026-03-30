#!/usr/bin/env python3
"""
build_appearances.py — Build document_appearances.json and doc_metadata.json.

Unified replacement for extract_doc_appearances.py and merge_annotations_to_appearances.py.
Reads from both sources:
  1. Annotated TEI XML volumes (*-annotated.xml) — extracts <rs> element references
  2. String match results JSON (string_match_results_*.json) — extracts by_term data

The two sources are complementary: annotated XML covers volumes that have been through
the full pipeline, while string match results cover all annotated volumes including
those not yet merged back to volume files.

Output:
  - document_appearances.json: {ref: {volume_id: [doc_ids]}}
  - doc_metadata.json: {documents: {vol/doc: {t, d}}, volumes: {vol: title}}

Usage:
    python3 build_appearances.py           # both sources
    python3 build_appearances.py --xml     # annotated XML only
    python3 build_appearances.py --json    # string match results only
"""

import argparse
import glob
import json
import os
import re
import sys
from collections import defaultdict

from lxml import etree

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"
NSMAP = {"tei": TEI_NS, "xml": XML_NS}

OUTPUT_APPEARANCES = os.path.join(ROOT_DIR, "document_appearances.json")
OUTPUT_METADATA = os.path.join(ROOT_DIR, "doc_metadata.json")


def doc_id_sort_key(doc_id):
    """Sort doc IDs like d1, d2, d10 in natural numeric order."""
    match = re.match(r"d(\d+)(.*)", doc_id)
    if match:
        return (int(match.group(1)), match.group(2))
    return (float("inf"), doc_id)


# ── Source 1: Annotated TEI XML ──────────────────────────────────────


def process_annotated_xml(appearances, doc_meta):
    """Extract appearances from annotated TEI XML volumes."""
    pattern = os.path.join(ROOT_DIR, "volumes", "*-annotated.xml")
    files = sorted(glob.glob(pattern))
    print(f"\nSource: annotated XML — {len(files)} volumes")

    if not files:
        print("  No annotated volumes found, skipping.")
        return

    total_annotations = 0
    for filepath in files:
        basename = os.path.basename(filepath)
        try:
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(filepath, parser)
            root = tree.getroot()

            volume_id = root.get(f"{{{XML_NS}}}id")
            if not volume_id:
                volume_id = basename.replace("-annotated.xml", "")

            doc_divs = tree.xpath('//tei:div[@type="document"]', namespaces=NSMAP)
            vol_annotations = 0

            for div in doc_divs:
                doc_id = div.get(f"{{{XML_NS}}}id")
                if not doc_id:
                    continue
                rs_elements = div.xpath(
                    './/tei:rs[@type="topic" or @type="compound-subject"]',
                    namespaces=NSMAP,
                )
                for rs in rs_elements:
                    corresp = rs.get("corresp")
                    if corresp:
                        if corresp not in appearances:
                            appearances[corresp] = {}
                        existing = set(appearances[corresp].get(volume_id, []))
                        existing.add(doc_id)
                        appearances[corresp][volume_id] = sorted(existing, key=doc_id_sort_key)
                        vol_annotations += 1

            total_annotations += vol_annotations
            print(f"  {basename}: {len(doc_divs)} docs, {vol_annotations} annotations")
        except Exception as e:
            print(f"  ERROR {basename}: {e}")

    print(f"  Total annotations from XML: {total_annotations}")


# ── Source 2: String match results JSON ──────────────────────────────


def process_string_match_results(appearances, doc_meta):
    """Merge appearances from string_match_results JSON files."""
    pattern = os.path.join(ROOT_DIR, "data", "documents", "*", "string_match_results_*.json")
    files = sorted(glob.glob(pattern))
    print(f"\nSource: string match results — {len(files)} volumes")

    if not files:
        print("  No results files found, skipping.")
        return

    total_new = 0
    total_merged = 0

    for results_file in files:
        with open(results_file) as f:
            results = json.load(f)

        vol_id = results["metadata"]["volume_id"]
        new_entries = 0
        merged_entries = 0

        # Merge by_term data into appearances
        for ref, term_data in results.get("by_term", {}).items():
            if ref not in appearances:
                appearances[ref] = {}

            doc_ids = sorted(term_data.get("documents", {}).keys())
            if not doc_ids:
                continue

            if vol_id not in appearances[ref]:
                appearances[ref][vol_id] = doc_ids
                new_entries += len(doc_ids)
            else:
                existing = set(appearances[ref][vol_id])
                new = set(doc_ids)
                combined = sorted(existing | new)
                added = len(combined) - len(existing)
                appearances[ref][vol_id] = combined
                merged_entries += added

        # Update doc metadata
        if vol_id not in doc_meta["volumes"]:
            doc_meta["volumes"][vol_id] = f"Foreign Relations of the United States, {vol_id}"

        for doc_id, doc_info in results.get("by_document", {}).items():
            doc_key = f"{vol_id}/{doc_id}"
            if doc_key not in doc_meta["documents"]:
                doc_meta["documents"][doc_key] = {
                    "t": doc_info.get("title", doc_id),
                    "d": doc_info.get("date", ""),
                }

        total_new += new_entries
        total_merged += merged_entries
        if new_entries or merged_entries:
            print(f"  {vol_id}: +{new_entries} new, +{merged_entries} merged")

    print(f"  Total: +{total_new} new, +{total_merged} merged")


# ── Main ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Build document appearances and metadata")
    parser.add_argument("--xml", action="store_true", help="Only process annotated XML volumes")
    parser.add_argument("--json", action="store_true", help="Only process string match results JSON")
    args = parser.parse_args()

    # Default: both sources
    do_xml = not args.json or args.xml
    do_json = not args.xml or args.json

    # Load existing data
    print("Loading existing data...")
    appearances = {}
    if os.path.exists(OUTPUT_APPEARANCES):
        with open(OUTPUT_APPEARANCES) as f:
            appearances = json.load(f)

    doc_meta = {"documents": {}, "volumes": {}}
    if os.path.exists(OUTPUT_METADATA):
        with open(OUTPUT_METADATA) as f:
            doc_meta = json.load(f)

    print(f"  Existing: {len(appearances)} subjects, {len(doc_meta.get('documents', {}))} documents")

    if do_xml:
        process_annotated_xml(appearances, doc_meta)

    if do_json:
        process_string_match_results(appearances, doc_meta)

    # Sort and write
    sorted_appearances = dict(sorted(appearances.items()))

    all_vols = set()
    for vols in sorted_appearances.values():
        all_vols.update(vols.keys())
    total_refs = sum(len(docs) for vols in sorted_appearances.values() for docs in vols.values())

    print(f"\nFinal: {len(sorted_appearances)} subjects, {len(all_vols)} volumes, {total_refs:,} doc refs")
    print(f"Metadata: {len(doc_meta['documents'])} documents, {len(doc_meta['volumes'])} volumes")

    with open(OUTPUT_APPEARANCES, "w") as f:
        json.dump(sorted_appearances, f, separators=(",", ":"))
    print(f"\nWrote {OUTPUT_APPEARANCES} ({os.path.getsize(OUTPUT_APPEARANCES) / 1024:.0f} KB)")

    with open(OUTPUT_METADATA, "w") as f:
        json.dump(doc_meta, f, separators=(",", ":"))
    print(f"Wrote {OUTPUT_METADATA} ({os.path.getsize(OUTPUT_METADATA) / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
