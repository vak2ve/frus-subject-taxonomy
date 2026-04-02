#!/usr/bin/env python3
"""
export_to_tei_headers.py — Export reviewed annotation decisions into TEI headers.

For each reviewed volume, this script:
1. Reads string_match_results_{vol}.json (all raw matches)
2. Loads all review decisions via resolve_decisions module
3. Filters to only accepted annotations, applies merges and exclusions
4. Writes the surviving subjects into each document's <textClass>/<keywords>

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
import re
import sys
import time
import unicodedata
from pathlib import Path

from lxml import etree

from resolve_decisions import (
    load_all_decisions,
    resolve_merge_chain,
    is_excluded,
    is_rejected,
    get_lcsh_decision,
)

# Resolve paths relative to repo root (one level up from scripts/)
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = REPO_ROOT / "data" / "documents"
CONFIG_DIR = REPO_ROOT / "config"

LCSH_MAPPING_FILE = CONFIG_DIR / "lcsh_mapping.json"


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


def filter_document_subjects(doc_id, doc_data, decisions, volume_id, lcsh_mapping):
    """Filter and deduplicate subjects for a single document.

    Returns list of dicts with keys:
        subject  — canonical/target term name (the taxonomy heading)
        term     — the original matched text from the document
        ref, type, category, subcategory, lcsh_uri, lcsh_match
    """
    subjects = []
    seen_refs = set()

    # Build volume-specific merge map (global + per-volume merges)
    vol_merge_map = dict(decisions.merge_map)
    for m in decisions.vol_merges.get(volume_id, []):
        source_ref = m.get("source_ref", "")
        target_ref = m.get("target_ref", "")
        if source_ref and target_ref:
            vol_merge_map[source_ref] = target_ref

    for match in doc_data.get("matches", []):
        ref = match.get("canonical_ref", match.get("ref", ""))
        position = match.get("position", 0)

        # Check per-document rejection
        if is_rejected(doc_id, ref, position, decisions, volume_id):
            continue

        # Resolve through merges
        final_ref = resolve_merge_chain(ref, vol_merge_map)

        # Check exclusions
        if is_excluded(final_ref, decisions) or is_excluded(ref, decisions):
            continue

        # Skip if already seen this ref for this document
        if final_ref in seen_refs:
            continue
        seen_refs.add(final_ref)

        # Get LCSH info from mapping
        lcsh_info = lcsh_mapping.get(final_ref, {})
        lcsh_uri = match.get("lcsh_uri", "") or lcsh_info.get("lcsh_uri", "")
        lcsh_match = match.get("lcsh_match", "") or lcsh_info.get("match_quality", "")

        # Apply LCSH decisions
        lcsh_decision = get_lcsh_decision(final_ref, decisions, volume_id)
        if lcsh_decision == "rejected":
            lcsh_match = "lcsh_rejected"
        elif lcsh_decision == "accepted":
            lcsh_match = lcsh_info.get("match_quality", lcsh_match)

        # Determine canonical subject name and original matched text
        matched_text = match.get("term", match.get("matched_text", ""))
        if final_ref in lcsh_mapping:
            subject_name = lcsh_mapping[final_ref].get("name", matched_text)
        elif ref != final_ref and ref in lcsh_mapping:
            # Merged: use target name from the source's mapping if target not mapped
            subject_name = lcsh_mapping[ref].get("name", matched_text)
        else:
            subject_name = matched_text

        # Use the merge target's category if available, otherwise the match's
        if final_ref in lcsh_mapping:
            category = lcsh_mapping[final_ref].get("category", match.get("category", ""))
            subcategory = lcsh_mapping[final_ref].get("subcategory", match.get("subcategory", ""))
        else:
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


def _build_term_line(subj, indent="                    "):
    """Build a single <term .../> XML line for a subject."""
    attrs = [
        f'ref="{subj["ref"]}"',
        f'type="{subj["type"]}"',
    ]
    if subj["category"]:
        attrs.append(f'category="{slugify(subj["category"])}"')
    if subj["subcategory"]:
        attrs.append(f'subcategory="{slugify(subj["subcategory"])}"')
    if subj["subject"]:
        attrs.append(f'subject="{slugify(subj["subject"])}"')
    if subj["lcsh_uri"]:
        attrs.append(f'lcsh-uri="{subj["lcsh_uri"]}"')
    if subj["lcsh_match"]:
        attrs.append(f'lcsh-match="{subj["lcsh_match"]}"')
    # Escape XML special chars in text content
    text = subj["term"].replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'{indent}<term {" ".join(attrs)}>{text}</term>'


def update_document_header(doc_path, subjects, force=False):
    """Update (or create) the <textClass>/<keywords> section in a document's TEI header.

    Appends new annotations to existing headers using string-level insertion
    to preserve the original XML formatting exactly.

    Returns: True if modified, False if skipped.
    """
    content = doc_path.read_text(encoding="utf-8") if isinstance(doc_path, Path) else Path(doc_path).read_text(encoding="utf-8")

    # Collect existing refs to avoid duplicates
    existing_refs = set()
    for m in re.finditer(r'<term\s[^>]*ref="([^"]*)"', content):
        existing_refs.add(m.group(1))

    # Filter to only new subjects
    new_subjects = [s for s in subjects if s["ref"] not in existing_refs]

    if not new_subjects:
        # Check if there's a teiHeader at all — if not and force is set, we need to create one
        if "<teiHeader" not in content and not force:
            return False
        if "<teiHeader" not in content and force and subjects:
            # Need to create a full header from scratch — use lxml for this case
            return _create_header_from_scratch(doc_path, content, subjects)
        return False  # nothing new to add

    # If there's an existing <keywords scheme="frus-subject-taxonomy"> block, insert before </keywords>
    # Match the closing tag with its leading whitespace to preserve indentation
    kw_close_pattern = re.compile(r'(\n([ \t]*)</keywords>)')
    kw_match = kw_close_pattern.search(content)
    if kw_match and 'scheme="frus-subject-taxonomy"' in content:
        closing_ws = kw_match.group(2)  # whitespace before </keywords>
        term_ws = closing_ws + "    "   # one level deeper for <term>
        new_lines = "\n".join(_build_term_line(s, indent=term_ws) for s in new_subjects)
        content = content.replace(
            kw_match.group(1),
            "\n" + new_lines + kw_match.group(1),
            1,
        )
    elif "<textClass>" in content or "<textClass " in content:
        # Has textClass but no frus-subject-taxonomy keywords — add the block
        new_lines = "\n".join(_build_term_line(s) for s in new_subjects)
        block = (
            '                <keywords scheme="frus-subject-taxonomy">\n'
            + new_lines + "\n"
            + "                </keywords>"
        )
        content = content.replace(
            "</textClass>",
            block + "\n            </textClass>",
            1,
        )
    elif "</profileDesc>" in content:
        # Has profileDesc but no textClass — add both
        new_lines = "\n".join(_build_term_line(s) for s in new_subjects)
        block = (
            "            <textClass>\n"
            '                <keywords scheme="frus-subject-taxonomy">\n'
            + new_lines + "\n"
            + "                </keywords>\n"
            + "            </textClass>"
        )
        content = content.replace(
            "</profileDesc>",
            block + "\n        </profileDesc>",
            1,
        )
    elif "</teiHeader>" in content:
        # Has teiHeader but no profileDesc — add all three
        new_lines = "\n".join(_build_term_line(s) for s in new_subjects)
        block = (
            "        <profileDesc>\n"
            "            <textClass>\n"
            '                <keywords scheme="frus-subject-taxonomy">\n'
            + new_lines + "\n"
            + "                </keywords>\n"
            + "            </textClass>\n"
            + "        </profileDesc>"
        )
        content = content.replace(
            "</teiHeader>",
            block + "\n    </teiHeader>",
            1,
        )
    elif force and subjects:
        return _create_header_from_scratch(doc_path, content, subjects)
    else:
        return False

    Path(doc_path).write_text(content, encoding="utf-8")
    return True


def _create_header_from_scratch(doc_path, content, subjects):
    """Create a minimal teiHeader with subjects for documents that have none."""
    new_lines = "\n".join(_build_term_line(s) for s in subjects)
    header = (
        "    <teiHeader>\n"
        "        <profileDesc>\n"
        "            <textClass>\n"
        '                <keywords scheme="frus-subject-taxonomy">\n'
        + new_lines + "\n"
        + "                </keywords>\n"
        + "            </textClass>\n"
        + "        </profileDesc>\n"
        + "    </teiHeader>"
    )
    # Insert after the root element opening tag
    content = re.sub(
        r'(<TEI[^>]*>)',
        r'\1\n' + header,
        content,
        count=1,
    )
    Path(doc_path).write_text(content, encoding="utf-8")
    return True


def find_annotated_volumes():
    """Find all volumes that have string_match_results (i.e., have been annotated)."""
    annotated = set()
    if DATA_DIR.exists():
        for vol_dir in DATA_DIR.iterdir():
            if vol_dir.is_dir():
                results_file = vol_dir / f"string_match_results_{vol_dir.name}.json"
                if results_file.exists():
                    annotated.add(vol_dir.name)
    return sorted(annotated)


def process_volume(vol_id, decisions, lcsh_mapping, force=False, dry_run=False):
    """Process all documents in a single volume, exporting decisions to TEI headers."""
    vol_dir = DATA_DIR / vol_id
    if not vol_dir.is_dir():
        print(f"  WARNING: Volume directory not found: {vol_dir}")
        return 0, 0, 0, 0

    results = load_string_match_results(vol_id)
    if results is None:
        print(f"  WARNING: No string match results for {vol_id}")
        return 0, 0, 0, 0

    # Load per-volume decisions if not already loaded
    if vol_id not in decisions.vol_rejections:
        vol_decisions = load_all_decisions(REPO_ROOT, volume_id=vol_id)
        decisions.vol_rejections[vol_id] = vol_decisions.vol_rejections.get(vol_id, set())
        decisions.vol_merges[vol_id] = vol_decisions.vol_merges.get(vol_id, [])
        decisions.vol_lcsh_decisions[vol_id] = vol_decisions.vol_lcsh_decisions.get(vol_id, {})

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
                doc_id, doc_data, decisions, vol_id, lcsh_mapping,
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

    # Load global decisions
    print("Loading decisions...")
    decisions = load_all_decisions(REPO_ROOT)
    lcsh_map = load_lcsh_mapping()

    print(f"  LCSH mapping: {len(lcsh_map)} entries")
    print(f"  Global merges: {len(decisions.merge_map)} entries")
    print(f"  Exclusions: {len(decisions.exclusions)} entries")
    print(f"  Global LCSH decisions: {len(decisions.lcsh_decisions)} entries")
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
            vol_id, decisions, lcsh_map,
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
