#!/usr/bin/env python3
"""
export_json.py — Export taxonomy and document-subject mappings as JSON.

Parses the built subject-taxonomy-lcsh.xml and generates:
  1. exports/taxonomy.json — taxonomy structure with categories, subjects, LCSH
  2. exports/document_subjects.json — subject-to-document mapping by volume

These JSON files are consumed by the frus-otd social media toolkit via
GitHub raw URLs.

Usage:
    python3 scripts/export_json.py              # generate both files
    python3 scripts/export_json.py --compact    # minimal whitespace for both
    python3 scripts/export_json.py --taxonomy-only
    python3 scripts/export_json.py --mapping-only
"""

import argparse
import json
import os
import sys
from datetime import date

from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

TAXONOMY_XML = "../subject-taxonomy-lcsh.xml"
EXPORTS_DIR = "../exports"
TAXONOMY_JSON = os.path.join(EXPORTS_DIR, "taxonomy.json")
DOCUMENT_SUBJECTS_JSON = os.path.join(EXPORTS_DIR, "document_subjects.json")


def parse_subject(subject_el):
    """Parse a <subject> element into a dict."""
    subj = {
        "ref": subject_el.get("ref"),
        "name": subject_el.findtext("name", ""),
        "count": int(subject_el.get("count", 0)),
        "volumes": int(subject_el.get("volumes", 0)),
    }

    lcsh_uri = subject_el.get("lcsh-uri")
    if lcsh_uri:
        subj["lcshUri"] = lcsh_uri
        subj["lcshMatch"] = subject_el.get("lcsh-match", "")

    variants = []
    for variant in subject_el.iter("variant"):
        text = variant.text
        if text and text.strip():
            variants.append(text.strip())
    if variants:
        subj["variants"] = variants

    return subj


def parse_document_appearances(subject_el):
    """Parse <documents> from a <subject> into a {volume_id: "d1, d2"} dict."""
    appearances = {}
    for vol_el in subject_el.iter("volume"):
        vol_id = vol_el.get("id")
        doc_text = vol_el.text
        if vol_id and doc_text and doc_text.strip():
            appearances[vol_id] = doc_text.strip()
    return appearances


def build_taxonomy_json(root):
    """Build the taxonomy structure dict from the parsed XML root."""
    generated = root.get("generated", date.today().isoformat())
    total_subjects = 0
    subject_index = []
    categories = []

    for cat_el in root.iterchildren("category"):
        cat = {
            "label": cat_el.get("label", ""),
            "totalAnnotations": int(cat_el.get("total-annotations", 0)),
            "subcategories": [],
        }

        for subcat_el in cat_el.iterchildren("subcategory"):
            subcat = {
                "label": subcat_el.get("label", ""),
                "subjects": [],
            }

            for subject_el in subcat_el.iterchildren("subject"):
                subj = parse_subject(subject_el)
                subcat["subjects"].append(subj)
                subject_index.append(subj["ref"])
                total_subjects += 1

            cat["subcategories"].append(subcat)

        categories.append(cat)

    return {
        "schemaVersion": 1,
        "generated": generated,
        "totalSubjects": total_subjects,
        "categories": categories,
        "subjectIndex": subject_index,
    }


def build_document_subjects_json(root):
    """Build the subject-to-document mapping dict from the parsed XML root."""
    generated = root.get("generated", date.today().isoformat())
    subjects = {}
    total_references = 0

    for cat_el in root.iterchildren("category"):
        for subcat_el in cat_el.iterchildren("subcategory"):
            for subject_el in subcat_el.iterchildren("subject"):
                ref = subject_el.get("ref")
                appearances = parse_document_appearances(subject_el)
                if appearances:
                    subjects[ref] = appearances
                    for doc_text in appearances.values():
                        total_references += len(doc_text.split(", "))

    return {
        "schemaVersion": 1,
        "generated": generated,
        "totalReferences": total_references,
        "subjects": subjects,
    }


def main():
    parser = argparse.ArgumentParser(description="Export taxonomy as JSON for frus-otd")
    parser.add_argument("--compact", action="store_true", help="Minimal whitespace")
    parser.add_argument("--taxonomy-only", action="store_true", help="Only export taxonomy.json")
    parser.add_argument("--mapping-only", action="store_true", help="Only export document_subjects.json")
    args = parser.parse_args()

    if not os.path.exists(TAXONOMY_XML):
        print(f"Error: {TAXONOMY_XML} not found. Run 'make mockup' or build_taxonomy_lcsh.py first.")
        sys.exit(1)

    os.makedirs(EXPORTS_DIR, exist_ok=True)

    print("Parsing taxonomy XML...")
    tree = etree.parse(TAXONOMY_XML)
    root = tree.getroot()

    export_taxonomy = not args.mapping_only
    export_mapping = not args.taxonomy_only

    if export_taxonomy:
        print("Building taxonomy structure...")
        taxonomy = build_taxonomy_json(root)
        json_kwargs = {"separators": (",", ":")} if args.compact else {"indent": 2}
        with open(TAXONOMY_JSON, "w") as f:
            json.dump(taxonomy, f, ensure_ascii=False, **json_kwargs)
        size_kb = os.path.getsize(TAXONOMY_JSON) / 1024
        print(f"  {TAXONOMY_JSON}: {taxonomy['totalSubjects']} subjects, "
              f"{len(taxonomy['categories'])} categories, {size_kb:.0f} KB")

    if export_mapping:
        print("Building document-subject mapping...")
        mapping = build_document_subjects_json(root)
        json_kwargs = {"separators": (",", ":")} if args.compact else {"indent": 2}
        with open(DOCUMENT_SUBJECTS_JSON, "w") as f:
            json.dump(mapping, f, ensure_ascii=False, **json_kwargs)
        size_mb = os.path.getsize(DOCUMENT_SUBJECTS_JSON) / (1024 * 1024)
        print(f"  {DOCUMENT_SUBJECTS_JSON}: {len(mapping['subjects'])} subjects, "
              f"{mapping['totalReferences']:,} references, {size_mb:.1f} MB")

    print("Done.")


if __name__ == "__main__":
    main()
