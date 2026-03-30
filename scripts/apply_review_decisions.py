#!/usr/bin/env python3
"""
Apply browser-exported review decisions to pipeline source files.

Reads exported JSON from:
  - string-match-review.html  -> annotation_rejections_<volume>.json
  - taxonomy-review.html      -> lcsh_decisions.json

Updates:
  - variant_overrides.json  (adds merge decisions)
  - lcsh_mapping.json       (marks rejected LCSH mappings as "lcsh_rejected")

Usage:
    python3 apply_review_decisions.py --volume frus1969-76v19p2
    python3 apply_review_decisions.py --volume frus1969-76v19p2 --dry-run
    python3 apply_review_decisions.py   # taxonomy LCSH decisions only (no volume)
"""

import argparse
import json
import os
import sys
from datetime import datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

VARIANT_OVERRIDES_FILE = "../config/variant_overrides.json"
LCSH_MAPPING_FILE = "../config/lcsh_mapping.json"
LCSH_DECISIONS_FILE = "../lcsh_decisions.json"


def annotation_rejections_file(volume_id):
    return f"../config/annotation_rejections_{volume_id}.json"



# ── Load exported decisions ──────────────────────────────────────────

def load_annotation_rejections(volume_id):
    """Load annotation_rejections_<volume>.json.

    Returns the parsed JSON dict, or None if not found.
    """
    path = annotation_rejections_file(volume_id)
    if not os.path.exists(path):
        print(f"  No annotation rejections file found: {path}")
        return None

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Validate
    if data.get("volume_id") != volume_id:
        print(f"  WARNING: volume_id mismatch in {path}: "
              f"expected '{volume_id}', got '{data.get('volume_id')}'")

    merge_count = len(data.get("merge_decisions", []))
    lcsh_count = len(data.get("lcsh_decisions", []))
    reject_count = len(data.get("rejections", []))
    snippet_count = len(data.get("variant_overrides_snippet", []))

    print(f"  Loaded {path}:")
    print(f"    {reject_count} annotation rejections")
    print(f"    {lcsh_count} LCSH decisions")
    print(f"    {merge_count} merge decisions")
    print(f"    {snippet_count} variant override snippets")

    return data


def load_lcsh_decisions():
    """Load LCSH decisions from taxonomy_review_state.json.

    Returns dict with "decisions" key in the format expected by update_lcsh_mapping:
    {"decisions": [{"ref": ref, "decision": "accepted"|"rejected"}, ...]}
    """
    state_file = os.path.join(os.path.dirname(LCSH_DECISIONS_FILE), "taxonomy_review_state.json")
    if not os.path.exists(state_file):
        print(f"  No taxonomy_review_state.json found")
        return None

    with open(state_file, "r", encoding="utf-8") as f:
        state = json.load(f)

    lcsh = state.get("lcsh_decisions", {})
    if not lcsh:
        print(f"  No LCSH decisions in taxonomy_review_state.json")
        return None

    # Convert dict format to list format for backwards compatibility
    decisions = [{"ref": ref, "decision": decision} for ref, decision in lcsh.items()]
    accepted = sum(1 for d in decisions if d["decision"] == "accepted")
    rejected = sum(1 for d in decisions if d["decision"] == "rejected")

    print(f"  Loaded LCSH decisions from taxonomy_review_state.json:")
    print(f"    {len(decisions)} total decisions ({accepted} accepted, {rejected} rejected)")

    return {"decisions": decisions}


# ── Update variant_overrides.json ────────────────────────────────────

def update_variant_overrides(merge_snippets, dry_run=False):
    """Append merge overrides to variant_overrides.json.

    Dedup-checks before adding: skips if same canonical_ref + variant_refs
    pair already exists.

    Returns: (added_count, skipped_count)
    """
    if not merge_snippets:
        return 0, 0

    with open(VARIANT_OVERRIDES_FILE, "r", encoding="utf-8") as f:
        overrides_data = json.load(f)

    existing = overrides_data.get("overrides", [])

    # Build set of existing merge pairs for dedup
    existing_merges = set()
    for entry in existing:
        if entry.get("action") == "merge":
            canonical = entry.get("canonical_ref", "")
            for vref in entry.get("variant_refs", []):
                existing_merges.add((canonical, vref))

    added = 0
    skipped = 0

    for snippet in merge_snippets:
        canonical = snippet.get("canonical_ref", "")
        variant_refs = snippet.get("variant_refs", [])

        # Check if all variant refs are already merged into this canonical
        all_exist = all(
            (canonical, vref) in existing_merges
            for vref in variant_refs
        )
        if all_exist:
            skipped += 1
            continue

        existing.append({
            "action": "merge",
            "canonical_ref": canonical,
            "variant_refs": variant_refs,
            "reason": snippet.get("reason", "From annotation review"),
        })
        for vref in variant_refs:
            existing_merges.add((canonical, vref))
        added += 1

    if added > 0 and not dry_run:
        overrides_data["overrides"] = existing
        overrides_data["updated"] = datetime.now().strftime("%Y-%m-%d")
        with open(VARIANT_OVERRIDES_FILE, "w", encoding="utf-8") as f:
            json.dump(overrides_data, f, indent=2, ensure_ascii=False)
            f.write("\n")

    return added, skipped


# ── Update lcsh_mapping.json ─────────────────────────────────────────

def update_lcsh_mapping(annotation_lcsh, taxonomy_lcsh, dry_run=False):
    """Update match_quality in lcsh_mapping.json for rejected LCSH.

    Taxonomy-review decisions take precedence over annotation-review
    LCSH decisions. For rejected entries, match_quality is set to
    "lcsh_rejected" (which build_taxonomy_lcsh.py naturally ignores
    when attaching LCSH URIs, since it only checks "exact"/"good_close").

    Returns: dict with counts
    """
    # Merge decisions: annotation first, taxonomy overwrites
    merged = {}
    for d in (annotation_lcsh or []):
        ref = d.get("ref")
        if ref:
            merged[ref] = d.get("decision", "")

    for d in (taxonomy_lcsh or []):
        ref = d.get("ref")
        if ref:
            merged[ref] = d.get("decision", "")

    if not merged:
        return {"rejected": 0, "accepted": 0, "already_rejected": 0, "not_found": 0}

    with open(LCSH_MAPPING_FILE, "r", encoding="utf-8") as f:
        mapping = json.load(f)

    counts = {"rejected": 0, "accepted": 0, "already_rejected": 0, "not_found": 0}

    for ref, decision in merged.items():
        if ref not in mapping:
            counts["not_found"] += 1
            continue

        entry = mapping[ref]

        if decision == "rejected":
            current_quality = entry.get("match_quality", "")
            if current_quality == "lcsh_rejected":
                counts["already_rejected"] += 1
            else:
                entry["match_quality"] = "lcsh_rejected"
                # Preserve original quality for reference
                if "original_match_quality" not in entry:
                    entry["original_match_quality"] = current_quality
                counts["rejected"] += 1
        elif decision == "accepted":
            # Restore original quality if it was previously rejected
            if entry.get("match_quality") == "lcsh_rejected":
                original = entry.pop("original_match_quality", "exact")
                entry["match_quality"] = original
            counts["accepted"] += 1

    if counts["rejected"] > 0 and not dry_run:
        with open(LCSH_MAPPING_FILE, "w", encoding="utf-8") as f:
            json.dump(mapping, f, indent=2, ensure_ascii=False)
            f.write("\n")

    return counts


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Apply browser-exported review decisions to pipeline source files."
    )
    parser.add_argument(
        "--volume", "-v",
        help="Volume ID (e.g., frus1969-76v19p2). If provided, loads annotation rejections for this volume.",
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Report what would change without modifying files.",
    )
    args = parser.parse_args()

    if args.dry_run:
        print("=== DRY RUN — no files will be modified ===\n")

    print("Loading exported review decisions...")
    annotation_data = None
    taxonomy_data = None

    if args.volume:
        annotation_data = load_annotation_rejections(args.volume)

    taxonomy_data = load_lcsh_decisions()

    if annotation_data is None and taxonomy_data is None:
        print("\nNo decision files found. Nothing to do.")
        print("Expected files:")
        if args.volume:
            print(f"  - {annotation_rejections_file(args.volume)}")
        print(f"  - {LCSH_DECISIONS_FILE}")
        sys.exit(1)

    # ── Apply merge decisions ────────────────────────────────────────
    merge_snippets = []
    if annotation_data:
        merge_snippets = annotation_data.get("variant_overrides_snippet", [])

    if merge_snippets:
        print(f"\nApplying {len(merge_snippets)} merge decision(s) to {VARIANT_OVERRIDES_FILE}...")
        added, skipped = update_variant_overrides(merge_snippets, dry_run=args.dry_run)
        print(f"  Result: {added} added, {skipped} already existed (skipped)")
    else:
        print("\nNo merge decisions to apply.")

    # ── Apply LCSH decisions ─────────────────────────────────────────
    annotation_lcsh = annotation_data.get("lcsh_decisions", []) if annotation_data else []
    taxonomy_lcsh = taxonomy_data.get("decisions", []) if taxonomy_data else []

    total_lcsh = len(set(
        d.get("ref") for d in (annotation_lcsh + taxonomy_lcsh) if d.get("ref")
    ))

    if total_lcsh > 0:
        print(f"\nApplying {total_lcsh} LCSH decision(s) to {LCSH_MAPPING_FILE}...")
        if annotation_lcsh and taxonomy_lcsh:
            print("  (taxonomy-review decisions take precedence)")
        counts = update_lcsh_mapping(annotation_lcsh, taxonomy_lcsh, dry_run=args.dry_run)
        print(f"  Result: {counts['rejected']} newly rejected, "
              f"{counts['accepted']} accepted/restored, "
              f"{counts['already_rejected']} already rejected, "
              f"{counts['not_found']} refs not found")
    else:
        print("\nNo LCSH decisions to apply.")

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    if args.dry_run:
        print("DRY RUN complete — no files were modified.")
    else:
        print("Review decisions applied successfully.")
        if merge_snippets:
            print(f"  Updated: {VARIANT_OVERRIDES_FILE}")
        if total_lcsh > 0 and any(
            d.get("decision") == "rejected"
            for d in (annotation_lcsh + taxonomy_lcsh)
        ):
            print(f"  Updated: {LCSH_MAPPING_FILE}")

    # ── Annotation rejection count (informational) ───────────────────
    if annotation_data:
        reject_count = len(annotation_data.get("rejections", []))
        if reject_count > 0:
            print(f"\n  Note: {reject_count} annotation rejections will be applied")
            print(f"  when running apply_curated_annotations.py {args.volume}")

    print(f"\nNext steps:")
    print(f"  python3 build_variant_groups.py")
    if args.volume:
        print(f"  python3 apply_curated_annotations.py {args.volume}")
        print(f"  python3 merge_annotations.py {args.volume}")
    print(f"  python3 extract_doc_appearances.py")
    print(f"  python3 build_taxonomy_lcsh.py build")
    print(f"  python3 generate_mockup_data.py")
    print(f"  python3 build_mockup_html.py")


if __name__ == "__main__":
    main()
