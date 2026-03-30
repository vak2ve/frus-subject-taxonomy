#!/usr/bin/env python3
"""
export_to_tei_headers.py — Export reviewed annotation decisions into TEI headers.

For each reviewed volume, this script:
1. Reads string_match_results_{vol}.json (all raw matches)
2. Reads annotation_rejections_{vol}.json (per-document rejections, merges)
3. Reads taxonomy_review_state.json (global exclusions, LCSH decisions, merges)
4. Filters to only accepted annotations, applies merges and exclusions
5. Writes the surviving subjects into each document's <textClass>/<keywords>

This makes the TEI documents themselves the source of truth for subject
assignments, rather than the centralized subject-taxonomy-lcsh.xml.

Usage:
    python3 scripts/export_to_tei_headers.py --vol frus1934v01   # single volume
    python3 scripts/export_to_tei_headers.py --all               # all reviewed volumes
    python3 scripts/export_to_tei_headers.py --dry-run            # preview without writing
    python3 scripts/export_to_tei_headers.py --force              # overwrite existing headers
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

from lxml import etree

# Resolve paths relative to repo root (one level up from scripts/)
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = REPO_ROOT / "data" / "documents"
CONFIG_DIR = REPO_ROOT / "config"

# Files
TAXONOMY_STATE_FILE = REPO_ROOT / "taxonomy_review_state.json"
LCSH_MAPPING_FILE = CONFIG_DIR / "lcsh_mapping.json"


def load_taxonomy_state():
    """Load taxonomy_review_state.json for global decisions."""
    if not TAXONOMY_STATE_FILE.exists():
        return {}
    with open(TAXONOMY_STATE_FILE) as f:
        return json.load(f)


def load_lcsh_mapping():
    """Load config/lcsh_mapping.json keyed by ref ID."""
    if not LCSH_MAPPING_FILE.exists():
        return {}
    with open(LCSH_MAPPING_FILE) as f:
        return json.load(f)


def load_string_match_results(vol_id):
    """Load string_match_results_{vol}.json."""
    path = DATA_DIR / vol_id / f"string_match_results_{vol_id}.json"
    if not path.exists():
        return None
    with open(path) as f:
        return json.load(f)


def load_annotation_rejections(vol_id):
    """Load annotation_rejections_{vol}.json from config/ directory.

    Returns dict with keys: rejections (set of keys), merges (list), lcsh_decisions (dict).
    """
    path = CONFIG_DIR / f"annotation_rejections_{vol_id}.json"
    if not path.exists():
        # Also check data/documents path
        path = DATA_DIR / vol_id / f"annotation_rejections_{vol_id}.json"
        if not path.exists():
            return {"rejections": set(), "merges": [], "lcsh_decisions": {}}

    with open(path) as f:
        data = json.load(f)

    rejections = set()
    for r in data.get("rejections", []):
        key = r.get("key", "")
        if key:
            rejections.add(key)

    merges = data.get("merge_decisions", [])
    lcsh = {}
    for d in data.get("lcsh_decisions", []):
        ref = d.get("ref", "")
        if ref:
            lcsh[ref] = d.get("decision", "")

    return {"rejections": rejections, "merges": merges, "lcsh_decisions": lcsh}


def build_merge_map(vol_merges, global_merges):
    """Build a ref -> target_ref mapping from merge decisions.

    Volume-level merges come from annotation_rejections.
    Global merges come from taxonomy_review_state.
    """
    merge_map = {}

    # Global merges (from taxonomy review)
    for source_ref, decision in global_merges.items():
        target_ref = decision.get("targetRef", "")
        if target_ref:
            merge_map[source_ref] = target_ref

    # Volume-level merges (override global if conflicting)
    for m in vol_merges:
        source_ref = m.get("source_ref", "")
        target_ref = m.get("target_ref", "")
        if source_ref and target_ref:
            merge_map[source_ref] = target_ref

    return merge_map


def build_exclusion_set(taxonomy_state):
    """Build set of excluded refs/slugs from taxonomy state."""
    excluded = set()

    # Exclusions (subjects excluded from taxonomy entirely)
    for key in taxonomy_state.get("exclusions", {}):
        excluded.add(key)

    # Global rejections
    for key in taxonomy_state.get("global_rejections", {}):
        excluded.add(key)

    return excluded


def resolve_ref_through_merges(ref, merge_map):
    """Follow merge chain to final target ref."""
    seen = set()
    current = ref
    while current in merge_map and current not in seen:
        seen.add(current)
        current = merge_map[current]
    return current


def filter_document_subjects(doc_id, doc_data, rejections, merge_map, exclusions,
                              global_lcsh_decisions, vol_lcsh_decisions, lcsh_mapping):
    """Filter and deduplicate subjects for a single document.

    Returns list of dicts with keys:
        subject  — canonical/target term name (the taxonomy heading)
        term     — the original matched text from the document
        ref, type, category, subcategory, lcsh_uri, lcsh_match
    """
    subjects = []
    seen_refs = set()

    for match in doc_data.get("matches", []):
        ref = match.get("canonical_ref", match.get("ref", ""))
        position = match.get("position", 0)

        # Check per-document rejection
        rejection_key = f"{doc_id}:{ref}:{position}"
        if rejection_key in rejections:
            continue

        # Resolve through merges
        final_ref = resolve_ref_through_merges(ref, merge_map)
        was_merged = final_ref != ref

        # Check exclusions (by ref and by slug-style keys)
        if final_ref in exclusions or ref in exclusions:
            continue

        # Skip if already seen this ref for this document
        if final_ref in seen_refs:
            continue
        seen_refs.add(final_ref)

        # Get LCSH info from mapping
        lcsh_info = lcsh_mapping.get(final_ref, {})
        lcsh_uri = match.get("lcsh_uri", "") or lcsh_info.get("lcsh_uri", "")
        lcsh_match = match.get("lcsh_match", "") or lcsh_info.get("match_quality", "")

        # Apply LCSH decisions (global takes precedence, then volume-level)
        if final_ref in global_lcsh_decisions:
            decision = global_lcsh_decisions[final_ref]
            if decision == "rejected":
                lcsh_match = "lcsh_rejected"
            elif decision == "accepted":
                lcsh_match = lcsh_info.get("match_quality", lcsh_match)
        elif final_ref in vol_lcsh_decisions:
            decision = vol_lcsh_decisions[final_ref]
            if decision == "rejected":
                lcsh_match = "lcsh_rejected"

        # Determine canonical subject name and original matched text
        matched_text = match.get("term", match.get("matched_text", ""))
        if final_ref in lcsh_mapping:
            subject_name = lcsh_mapping[final_ref].get("name", matched_text)
        else:
            subject_name = matched_text

        category = match.get("category", "")
        subcategory = match.get("subcategory", "")

        subjects.append({
            "subject": subject_name,
            "term": matched_text,
            "ref": final_ref,
            "type": match.get("type", "topic"),
            "category": category,
            "subcategory": subcategory,
            "lcsh_uri": lcsh_uri,
            "lcsh_match": lcsh_match,
        })

    return subjects


def slugify(text):
    """Convert a label to a kebab-case slug."""
    import re
    import unicodedata
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text


def build_keywords_element(subjects):
    """Build a <keywords scheme='frus-subject-taxonomy'> element from filtered subjects.

    Each <term> has:
      @ref          — taxonomy reference ID
      @type         — topic, person, organization, etc.
      @category     — top-level category slug
      @subcategory  — subcategory slug
      @subject      — canonical taxonomy heading name
      @lcsh-uri     — Library of Congress URI (if mapped)
      @lcsh-match   — LCSH match quality
      text content  — the matched string from the document
    """
    keywords = etree.Element("keywords")
    keywords.set("scheme", "frus-subject-taxonomy")

    for subj in subjects:
        term_el = etree.SubElement(keywords, "term")
        term_el.set("ref", subj["ref"])
        term_el.set("type", subj["type"])

        if subj["category"]:
            term_el.set("category", slugify(subj["category"]))
        if subj["subcategory"]:
            term_el.set("subcategory", slugify(subj["subcategory"]))
        if subj["subject"]:
            term_el.set("subject", slugify(subj["subject"]))
        if subj["lcsh_uri"]:
            term_el.set("lcsh-uri", subj["lcsh_uri"])
        if subj["lcsh_match"]:
            term_el.set("lcsh-match", subj["lcsh_match"])

        term_el.text = subj["term"]

    return keywords


def update_document_header(doc_path, subjects, force=False):
    """Update (or create) the <textClass>/<keywords> section in a document's TEI header.

    If the document has no teiHeader, one is created with just the textClass.
    If it has a teiHeader, the textClass/keywords is replaced.

    Returns: True if modified, False if skipped.
    """
    parser = etree.XMLParser(remove_blank_text=False)
    tree = etree.parse(str(doc_path), parser)
    root = tree.getroot()

    # Find or create teiHeader
    header = root.find("teiHeader")
    if header is None:
        header = root.find("{http://www.tei-c.org/ns/1.0}teiHeader")

    if header is None:
        if not force:
            return False
        # Create minimal header
        header = etree.Element("teiHeader")
        root.insert(0, header)

    # Find or create profileDesc
    profile_desc = header.find("profileDesc")
    if profile_desc is None:
        profile_desc = header.find("{http://www.tei-c.org/ns/1.0}profileDesc")
    if profile_desc is None:
        profile_desc = etree.SubElement(header, "profileDesc")

    # Remove existing textClass
    for ns_prefix in ["", "{http://www.tei-c.org/ns/1.0}"]:
        existing = profile_desc.find(f"{ns_prefix}textClass")
        if existing is not None:
            profile_desc.remove(existing)

    # Build and insert new textClass if there are subjects
    if subjects:
        text_class = etree.SubElement(profile_desc, "textClass")
        keywords = build_keywords_element(subjects)
        text_class.append(keywords)

    # Reindent the teiHeader so it's nicely formatted, without
    # disturbing whitespace in the document body.
    # Serialize and reparse the header with remove_blank_text=True
    # so etree.indent() can work correctly.
    header_str = etree.tostring(header, encoding="unicode")
    clean_parser = etree.XMLParser(remove_blank_text=True)
    clean_header = etree.fromstring(header_str, clean_parser)
    etree.indent(clean_header, space="    ", level=1)
    parent = header.getparent()
    idx = list(parent).index(header)
    parent.remove(header)
    parent.insert(idx, clean_header)

    tree.write(
        str(doc_path),
        xml_declaration=True,
        encoding="UTF-8",
        pretty_print=True,
    )
    return True


def find_annotated_volumes():
    """Find all volumes that have string_match_results (i.e., have been annotated).

    This includes both reviewed volumes (with annotation_rejections) and
    unreviewed volumes where all matches are accepted by default.
    """
    annotated = set()

    if DATA_DIR.exists():
        for vol_dir in DATA_DIR.iterdir():
            if vol_dir.is_dir():
                results_file = vol_dir / f"string_match_results_{vol_dir.name}.json"
                if results_file.exists():
                    annotated.add(vol_dir.name)

    return sorted(annotated)


def process_volume(vol_id, taxonomy_state, lcsh_mapping, merge_map, exclusions,
                   force=False, dry_run=False):
    """Process all documents in a single volume, exporting decisions to TEI headers."""
    vol_dir = DATA_DIR / vol_id
    if not vol_dir.is_dir():
        print(f"  WARNING: Volume directory not found: {vol_dir}")
        return 0, 0, 0

    # Load volume-specific data
    results = load_string_match_results(vol_id)
    if results is None:
        print(f"  WARNING: No string match results for {vol_id}")
        return 0, 0, 0

    vol_decisions = load_annotation_rejections(vol_id)
    rejections = vol_decisions["rejections"]
    vol_merges = vol_decisions["merges"]
    vol_lcsh = vol_decisions["lcsh_decisions"]

    # Combine volume merges with global merge map
    vol_merge_map = dict(merge_map)  # copy global
    for m in vol_merges:
        source_ref = m.get("source_ref", "")
        target_ref = m.get("target_ref", "")
        if source_ref and target_ref:
            vol_merge_map[source_ref] = target_ref

    global_lcsh = taxonomy_state.get("lcsh_decisions", {})

    by_document = results.get("by_document", {})

    doc_files = sorted(vol_dir.glob("d*.xml"))
    processed = 0
    skipped = 0
    errors = 0
    total_subjects = 0

    for doc_path in doc_files:
        doc_id = doc_path.stem

        try:
            doc_data = by_document.get(doc_id, {})
            subjects = filter_document_subjects(
                doc_id, doc_data, rejections, vol_merge_map, exclusions,
                global_lcsh, vol_lcsh, lcsh_mapping,
            )

            if dry_run:
                if subjects:
                    print(f"    [DRY RUN] {doc_id}: {len(subjects)} subjects")
                    total_subjects += len(subjects)
                processed += 1
                continue

            modified = update_document_header(doc_path, subjects, force=force)
            if modified:
                processed += 1
                total_subjects += len(subjects)
            else:
                skipped += 1

        except Exception as e:
            errors += 1
            print(f"    ERROR {doc_id}: {e}")

    return processed, skipped, errors, total_subjects


def main():
    parser = argparse.ArgumentParser(
        description="Export reviewed annotation decisions into TEI document headers"
    )
    parser.add_argument("--vol", help="Process a single volume (e.g., frus1969-76v01)")
    parser.add_argument("--all", action="store_true", help="Process all reviewed volumes")
    parser.add_argument("--force", action="store_true",
                        help="Create headers even for documents without existing teiHeader")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing files")
    args = parser.parse_args()

    if not args.vol and not args.all:
        print("ERROR: Specify --vol <volume_id> or --all")
        sys.exit(1)

    print("=== Export Decisions to TEI Headers ===")
    print(f"Force: {args.force}")
    print(f"Dry run: {args.dry_run}")
    print()

    # Load global state
    print("Loading global state...")
    taxonomy_state = load_taxonomy_state()
    lcsh_map = load_lcsh_mapping()

    global_merges = taxonomy_state.get("merge_decisions", {})
    merge_map = build_merge_map([], global_merges)
    exclusions = build_exclusion_set(taxonomy_state)

    print(f"  LCSH mapping: {len(lcsh_map)} entries")
    print(f"  Global merges: {len(merge_map)} entries")
    print(f"  Exclusions: {len(exclusions)} entries")
    print(f"  Global LCSH decisions: {len(taxonomy_state.get('lcsh_decisions', {}))} entries")
    print()

    # Determine volumes to process
    if args.vol:
        volumes = [args.vol]
    else:
        volumes = find_annotated_volumes()
        if not volumes:
            print("No annotated volumes found (no string_match_results files).")
            sys.exit(0)
        print(f"Found {len(volumes)} annotated volumes")
        print()

    total_processed = 0
    total_skipped = 0
    total_errors = 0
    total_subjects = 0
    start_time = time.time()

    for i, vol_id in enumerate(volumes, 1):
        print(f"[{i}/{len(volumes)}] {vol_id}...")
        p, s, e, subj = process_volume(
            vol_id, taxonomy_state, lcsh_map, merge_map, exclusions,
            force=args.force, dry_run=args.dry_run,
        )
        total_processed += p
        total_skipped += s
        total_errors += e
        total_subjects += subj
        if p or e:
            print(f"  -> {p} docs updated, {subj} subjects, {s} skipped, {e} errors")

    elapsed = time.time() - start_time
    print()
    print("=== Complete ===")
    print(f"Volumes: {len(volumes)}")
    print(f"Documents updated: {total_processed}")
    print(f"Total subjects exported: {total_subjects}")
    print(f"Documents skipped: {total_skipped}")
    print(f"Errors: {total_errors}")
    print(f"Time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
