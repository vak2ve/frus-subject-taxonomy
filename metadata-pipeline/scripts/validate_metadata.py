#!/usr/bin/env python3
"""
validate_metadata.py — Validate metadata pipeline data integrity.

Checks:
  1. Volume manifest covers all document directories
  2. Taxonomy XML is well-formed and consistent
  3. Document appearances are valid
  4. Review state JSON is valid
  5. Mockup data files exist
  6. Comparison with string-match pipeline (if available)

Usage:
    python3 validate_metadata.py
    python3 validate_metadata.py --compare   # also compare with string-match results
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

from lxml import etree

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PIPELINE_DIR.parent
DATA_DIR = PIPELINE_DIR / "data"
TAXONOMY_PATH = PIPELINE_DIR / "subject-taxonomy-metadata.xml"
DOC_APPEARANCES_PATH = DATA_DIR / "document_appearances.json"
STATE_PATH = PIPELINE_DIR / "metadata_review_state.json"
MANIFEST_PATH = DATA_DIR / "volume_manifest.json"


DOC_DATA_DIR = REPO_ROOT / "data" / "documents"


def check_manifest():
    """Validate volume manifest against actual document directories."""
    print("Checking manifest...")
    if not MANIFEST_PATH.exists():
        print("  WARN: No manifest found. Run 'make scan' first.")
        return False

    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)

    manifest_vols = {e["volume_id"] for e in manifest}

    # Check that all document directories are in manifest
    actual_dirs = set()
    if DOC_DATA_DIR.exists():
        for d in DOC_DATA_DIR.iterdir():
            if d.is_dir() and any(d.glob("d*.xml")):
                actual_dirs.add(d.name)

    missing_from_manifest = actual_dirs - manifest_vols
    extra_in_manifest = manifest_vols - actual_dirs

    errors = 0
    if missing_from_manifest:
        print(f"  WARN: {len(missing_from_manifest)} dirs not in manifest (first 5: {sorted(missing_from_manifest)[:5]})")
    if extra_in_manifest:
        print(f"  ERROR: {len(extra_in_manifest)} manifest entries with no dir")
        errors += len(extra_in_manifest)

    # Check required keys in manifest entries
    required_keys = {"volume_id", "total_documents", "total_annotations"}
    for entry in manifest:
        missing = required_keys - set(entry.keys())
        if missing:
            print(f"  ERROR: {entry.get('volume_id', '?')} missing keys: {missing}")
            errors += 1

    # Stats
    total_docs = sum(e.get("total_documents", 0) for e in manifest)
    total_ann = sum(e.get("total_annotations", 0) for e in manifest)
    annotated = sum(e.get("documents_with_annotations", 0) for e in manifest)
    coverage = round(annotated / total_docs * 100, 1) if total_docs else 0

    print(f"  {len(manifest)} volumes, {total_docs:,} docs, {annotated:,} annotated ({coverage}%)")
    print(f"  {total_ann:,} total annotations")
    if errors:
        print(f"  {errors} errors")
    return errors == 0


def check_data_files():
    """Validate that all expected data files exist and are non-empty."""
    print("Checking data files...")
    errors = 0

    required_files = [
        (DOC_APPEARANCES_PATH, "document_appearances.json"),
        (DATA_DIR / "doc_metadata.json", "doc_metadata.json"),
        (DATA_DIR / "mockup_sidebar_data.json", "mockup_sidebar_data.json"),
        (DATA_DIR / "mockup_subject_data.json", "mockup_subject_data.json"),
    ]

    for path, label in required_files:
        if not path.exists():
            print(f"  ERROR: Missing {label}")
            errors += 1
        elif path.stat().st_size == 0:
            print(f"  ERROR: Empty {label}")
            errors += 1
        else:
            size_mb = path.stat().st_size / 1024 / 1024
            print(f"  OK: {label} ({size_mb:.1f} MB)")

    # Check mockup per-category files
    mockup_dir = DATA_DIR / "mockup"
    if mockup_dir.exists():
        cat_files = list(mockup_dir.glob("*.json"))
        print(f"  OK: {len(cat_files)} per-category mockup files")
    else:
        print(f"  WARN: No mockup directory")

    return errors == 0


def check_taxonomy():
    """Validate taxonomy XML."""
    print("Checking taxonomy XML...")
    if not TAXONOMY_PATH.exists():
        print("  WARN: No taxonomy found. Run build_metadata_taxonomy.py first.")
        return False

    try:
        tree = etree.parse(str(TAXONOMY_PATH))
        root = tree.getroot()
    except Exception as e:
        print(f"  ERROR: Invalid XML: {e}")
        return False

    cats = root.findall("category")
    total_subjects = 0
    refs = set()
    dup_refs = set()

    for cat in cats:
        for sub in cat.findall("subcategory"):
            for subj in sub.findall("subject"):
                ref = subj.get("ref", "")
                if ref in refs:
                    dup_refs.add(ref)
                refs.add(ref)
                total_subjects += 1

    claimed = int(root.get("total-subjects", 0))
    print(f"  {len(cats)} categories, {total_subjects} subjects (claimed: {claimed})")
    if dup_refs:
        print(f"  WARN: {len(dup_refs)} duplicate refs found")
    if total_subjects != claimed:
        print(f"  WARN: Count mismatch — found {total_subjects}, claimed {claimed}")

    return len(dup_refs) == 0


def check_doc_appearances():
    """Validate document appearances."""
    print("Checking document appearances...")
    if not DOC_APPEARANCES_PATH.exists():
        print("  WARN: No document appearances found.")
        return True

    with open(DOC_APPEARANCES_PATH) as f:
        apps = json.load(f)

    total_refs = len(apps)
    total_docs = sum(
        len(docs)
        for vols in apps.values()
        for docs in vols.values()
    )
    print(f"  {total_refs} subjects, {total_docs} total document references")
    return True


def check_review_state():
    """Validate review state JSON."""
    print("Checking review state...")
    if not STATE_PATH.exists():
        print("  No review state yet (normal for fresh pipeline)")
        return True

    try:
        with open(STATE_PATH) as f:
            state = json.load(f)
        print(f"  Exclusions: {len(state.get('exclusions', {}))}")
        print(f"  Merges: {len(state.get('merge_decisions', {}))}")
        print(f"  Overrides: {len(state.get('category_overrides', {}))}")
        print(f"  Last saved: {state.get('saved', 'never')}")
        return True
    except Exception as e:
        print(f"  ERROR: Invalid state JSON: {e}")
        return False


def compare_with_string_match():
    """Compare metadata pipeline taxonomy with string-match pipeline taxonomy."""
    print("\nComparing with string-match pipeline...")
    sm_taxonomy = REPO_ROOT / "subject-taxonomy-lcsh.xml"

    if not sm_taxonomy.exists():
        print("  String-match taxonomy not found, skipping comparison")
        return

    # Compare taxonomy subjects
    try:
        meta_tree = etree.parse(str(TAXONOMY_PATH))
        sm_tree = etree.parse(str(sm_taxonomy))
    except Exception as e:
        print(f"  ERROR: Could not parse taxonomy: {e}")
        return

    meta_refs = set()
    for subj in meta_tree.getroot().iter("subject"):
        meta_refs.add(subj.get("ref", ""))

    sm_refs = set()
    for subj in sm_tree.getroot().iter("subject"):
        sm_refs.add(subj.get("ref", ""))

    both = meta_refs & sm_refs
    meta_only = meta_refs - sm_refs
    sm_only = sm_refs - meta_refs

    print(f"  Metadata pipeline: {len(meta_refs)} subjects")
    print(f"  String-match pipeline: {len(sm_refs)} subjects")
    print(f"  In both: {len(both)}")
    print(f"  Metadata-only: {len(meta_only)}")
    print(f"  String-match-only: {len(sm_only)}")


def main():
    parser = argparse.ArgumentParser(description="Validate metadata pipeline data")
    parser.add_argument("--compare", action="store_true",
                        help="Compare with string-match pipeline results")
    args = parser.parse_args()

    print("=== Metadata Pipeline Validation ===\n")

    results = [
        check_manifest(),
        check_data_files(),
        check_taxonomy(),
        check_doc_appearances(),
        check_review_state(),
    ]

    if args.compare:
        compare_with_string_match()

    print()
    if all(results):
        print("All checks passed.")
    else:
        print("Some checks failed or had warnings. See above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
