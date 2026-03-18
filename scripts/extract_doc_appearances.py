#!/usr/bin/env python3
"""
Extract document-level appearance data for subject annotations from
annotated FRUS TEI XML volumes.

For each annotated volume, finds all rs[@type="topic"] and
rs[@type="compound-subject"] elements within document divs, and builds
a mapping of airtable_rec_id -> {volume: [doc_ids]}.

Output: document_appearances.json
"""

import glob
import json
import os
import re
import sys
from collections import defaultdict

from lxml import etree

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"

NSMAP = {"tei": TEI_NS, "xml": XML_NS}

def doc_id_sort_key(doc_id):
    """Sort doc IDs like d1, d2, d10 in natural numeric order."""
    match = re.match(r"d(\d+)(.*)", doc_id)
    if match:
        return (int(match.group(1)), match.group(2))
    return (float("inf"), doc_id)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
INPUT_PATTERN = os.path.join(ROOT_DIR, "volumes", "*-annotated.xml")
OUTPUT_FILE = os.path.join(ROOT_DIR, "document_appearances.json")


def extract_volume_id(tree):
    """Extract the volume ID from the root TEI element's xml:id attribute."""
    root = tree.getroot()
    return root.get(f"{{{XML_NS}}}id")


def process_volume(filepath):
    """
    Parse one annotated volume and return a dict:
      { rec_id: set_of_doc_ids }
    """
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(filepath, parser)

    volume_id = extract_volume_id(tree)
    if not volume_id:
        basename = os.path.basename(filepath)
        volume_id = basename.replace("-annotated.xml", "")
        print(f"  Warning: no xml:id on root TEI element; using filename-derived ID: {volume_id}")

    # Find all document divs
    doc_divs = tree.xpath(
        '//tei:div[@type="document"]',
        namespaces=NSMAP,
    )

    rec_to_docs = defaultdict(set)

    for div in doc_divs:
        doc_id = div.get(f"{{{XML_NS}}}id")
        if not doc_id:
            continue

        # Find rs elements with type="topic" or type="compound-subject"
        rs_elements = div.xpath(
            './/tei:rs[@type="topic" or @type="compound-subject"]',
            namespaces=NSMAP,
        )

        for rs in rs_elements:
            corresp = rs.get("corresp")
            if corresp:
                rec_to_docs[corresp].add(doc_id)

    return volume_id, rec_to_docs


def main():
    files = sorted(glob.glob(INPUT_PATTERN))
    print(f"Found {len(files)} annotated volume(s).\n")

    if not files:
        print("WARNING: No annotated volumes found. Output will be empty.")

    # Master mapping: rec_id -> { volume_id: [doc_ids] }
    master = defaultdict(dict)

    total_docs = 0
    total_annotations = 0
    errors = 0

    for filepath in files:
        basename = os.path.basename(filepath)
        print(f"Processing: {basename}")

        try:
            volume_id, rec_to_docs = process_volume(filepath)

            # Count documents in this volume
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(filepath, parser)
            doc_count = len(tree.xpath('//tei:div[@type="document"]', namespaces=NSMAP))
            total_docs += doc_count

            vol_annotations = 0
            for rec_id, doc_ids in rec_to_docs.items():
                master[rec_id][volume_id] = sorted(doc_ids, key=doc_id_sort_key)
                vol_annotations += len(doc_ids)

            total_annotations += vol_annotations
            unique_subjects = len(rec_to_docs)
            print(f"  -> {doc_count} documents, {unique_subjects} unique subjects, {vol_annotations} doc-subject pairs")
        except Exception as e:
            print(f"  ERROR processing {basename}: {e}")
            errors += 1

    # Sort master by rec_id for stable output
    sorted_master = dict(sorted(master.items()))

    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted_master, f, indent=2, ensure_ascii=False)
    except OSError as e:
        print(f"ERROR: Failed to write {OUTPUT_FILE}: {e}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Volumes processed:        {len(files)}")
    print(f"  Total documents:          {total_docs}")
    print(f"  Unique subject rec IDs:   {len(sorted_master)}")
    print(f"  Total doc-subject pairs:  {total_annotations}")
    print(f"  Output written to:        {OUTPUT_FILE}")
    if errors > 0:
        print(f"  Errors:                   {errors}")
        sys.exit(1)


if __name__ == "__main__":
    main()
