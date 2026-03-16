#!/usr/bin/env python3
"""
Merge extracted annotation data from string_match_results_*.json files
into document_appearances.json and doc_metadata.json.

This enriches the mockup's baseline data with annotation references from
all 14 volumes so the mockup shows the full picture without manual import.

Usage:
    python3 merge_annotations_to_appearances.py
"""

import glob
import json
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
os.chdir(ROOT_DIR)

DOC_APPEARANCES_FILE = "document_appearances.json"
DOC_METADATA_FILE = "doc_metadata.json"


def main():
    # Load existing data
    print("Loading existing data...")
    if os.path.exists(DOC_APPEARANCES_FILE):
        with open(DOC_APPEARANCES_FILE) as f:
            appearances = json.load(f)
    else:
        appearances = {}

    if os.path.exists(DOC_METADATA_FILE):
        with open(DOC_METADATA_FILE) as f:
            doc_meta = json.load(f)
    else:
        doc_meta = {"documents": {}, "volumes": {}}

    existing_vols = set()
    for vols in appearances.values():
        existing_vols.update(vols.keys())
    print(f"  Existing appearances: {len(appearances)} subjects, {len(existing_vols)} volumes")
    print(f"  Existing doc metadata: {len(doc_meta.get('documents', {}))} documents, {len(doc_meta.get('volumes', {}))} volumes")

    # Process each results file
    results_files = sorted(glob.glob("data/documents/*/string_match_results_*.json"))
    print(f"\nProcessing {len(results_files)} results files...")

    total_new_entries = 0
    total_merged_entries = 0

    for results_file in results_files:
        with open(results_file) as f:
            results = json.load(f)

        vol_id = results["metadata"]["volume_id"]
        source = results["metadata"].get("source", "string_match")
        is_new_vol = vol_id not in existing_vols
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
                # Union of existing and new doc IDs
                existing = set(appearances[ref][vol_id])
                new = set(doc_ids)
                combined = sorted(existing | new)
                added = len(combined) - len(existing)
                appearances[ref][vol_id] = combined
                merged_entries += added

        # Update doc_metadata with document titles/dates
        if "documents" not in doc_meta:
            doc_meta["documents"] = {}
        if "volumes" not in doc_meta:
            doc_meta["volumes"] = {}

        # Add volume title
        if vol_id not in doc_meta["volumes"]:
            doc_meta["volumes"][vol_id] = f"Foreign Relations of the United States, {vol_id}"

        # Add document metadata
        for doc_id, doc_info in results.get("by_document", {}).items():
            doc_key = f"{vol_id}/{doc_id}"
            if doc_key not in doc_meta["documents"]:
                doc_meta["documents"][doc_key] = {
                    "t": doc_info.get("title", doc_id),
                    "d": doc_info.get("date", ""),
                }

        status = "NEW" if is_new_vol else "merged"
        print(f"  {vol_id} ({source}): +{new_entries} new, +{merged_entries} merged doc refs [{status}]")
        total_new_entries += new_entries
        total_merged_entries += merged_entries

    # Write updated files
    print(f"\nTotals: +{total_new_entries} new entries, +{total_merged_entries} merged entries")

    all_vols = set()
    for vols in appearances.values():
        all_vols.update(vols.keys())
    total_doc_refs = sum(len(docs) for vols in appearances.values() for docs in vols.values())

    print(f"\nUpdated appearances: {len(appearances)} subjects, {len(all_vols)} volumes, {total_doc_refs:,} total doc refs")
    print(f"Updated doc metadata: {len(doc_meta['documents'])} documents, {len(doc_meta['volumes'])} volumes")
    print(f"Volumes: {sorted(all_vols)}")

    with open(DOC_APPEARANCES_FILE, "w") as f:
        json.dump(appearances, f, separators=(",", ":"))
    print(f"\nWrote {DOC_APPEARANCES_FILE} ({os.path.getsize(DOC_APPEARANCES_FILE) / 1024:.0f} KB)")

    with open(DOC_METADATA_FILE, "w") as f:
        json.dump(doc_meta, f, separators=(",", ":"))
    print(f"Wrote {DOC_METADATA_FILE} ({os.path.getsize(DOC_METADATA_FILE) / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
