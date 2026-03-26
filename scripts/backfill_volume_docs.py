#!/usr/bin/env python3
"""Backfill volume_docs into index_candidates.json and promoted_candidates.json.

Reads per-volume index entries for each candidate that has a volumes list
but no volume_docs, and reconstructs the volume->doc_id mapping by re-parsing
the relevant volume indexes.

Usage:
    python3 scripts/backfill_volume_docs.py
"""
import json
import os
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

from discover_index_terms import parse_index, normalize_for_matching

VOLUMES_DIR = os.environ.get(
    "FRUS_CORPUS_DIR",
    os.path.expanduser("~/mnt/Subject taxonomy/volumes"),
)


def backfill_candidates(candidates_file):
    """Add volume_docs to candidates missing it."""
    with open(candidates_file) as f:
        data = json.load(f)

    if isinstance(data, dict) and "candidates" in data:
        candidates = data["candidates"]
        is_wrapped = True
    else:
        candidates = data
        is_wrapped = False

    # Find candidates needing backfill
    needs_backfill = {}
    for c in candidates:
        name = c.get("term") or c.get("name", "")
        vols = c.get("volumes", [])
        if vols and not c.get("volume_docs"):
            key = normalize_for_matching(name)
            if key:
                needs_backfill[key] = {"candidate": c, "name": name, "volumes": vols}

    if not needs_backfill:
        print(f"  {candidates_file}: no candidates need backfill")
        return 0

    print(f"  {candidates_file}: {len(needs_backfill)} candidates need volume_docs backfill")

    # Collect all volumes we need to parse
    all_volumes = set()
    for info in needs_backfill.values():
        all_volumes.update(info["volumes"])

    print(f"  Parsing {len(all_volumes)} volumes...")

    # Parse each volume's index and collect per-heading doc_refs
    volume_docs_map = {}  # heading_key -> {volume -> sorted list of doc_ids}
    parsed = 0
    for vol_id in sorted(all_volumes):
        vol_path = os.path.join(VOLUMES_DIR, f"{vol_id}.xml")
        if not os.path.exists(vol_path):
            continue
        try:
            entries = parse_index(vol_path)
            for entry in entries:
                key = normalize_for_matching(entry["heading"])
                if not key:
                    continue
                # Match against needed candidates — also try prefix match
                # to handle index headings with cross-ref suffixes like "(see also"
                matched_key = None
                if key in needs_backfill:
                    matched_key = key
                else:
                    for candidate_key in needs_backfill:
                        if key.startswith(candidate_key + " "):
                            matched_key = candidate_key
                            break
                if matched_key:
                    if matched_key not in volume_docs_map:
                        volume_docs_map[matched_key] = {}
                    if entry["doc_refs"]:
                        existing = set(volume_docs_map[matched_key].get(vol_id, []))
                        existing.update(entry["doc_refs"])
                        volume_docs_map[matched_key][vol_id] = sorted(existing)
            parsed += 1
        except Exception as e:
            print(f"    Warning: failed to parse {vol_id}: {e}")

    print(f"  Parsed {parsed}/{len(all_volumes)} volumes")

    # Apply backfill
    filled = 0
    for key, info in needs_backfill.items():
        if key in volume_docs_map and volume_docs_map[key]:
            vd = volume_docs_map[key]
            info["candidate"]["volume_docs"] = vd
            # Update doc_count to accurate per-volume sum
            new_count = sum(len(docs) for docs in vd.values())
            if new_count > 0:
                info["candidate"]["doc_count"] = new_count
            filled += 1

    print(f"  Backfilled volume_docs for {filled}/{len(needs_backfill)} candidates")

    # Write back
    with open(candidates_file, "w") as f:
        json.dump(data, f, indent=2)

    return filled


def main():
    print("Backfilling volume_docs...")
    n1 = backfill_candidates("../data/index_candidates.json")
    n2 = backfill_candidates("../config/promoted_candidates.json")
    print(f"\nDone: {n1 + n2} total candidates updated")


if __name__ == "__main__":
    main()
