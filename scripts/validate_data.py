#!/usr/bin/env python3
"""
Validate the integrity of annotation data across all volumes.

Checks for:
  - Missing volume XML files (can't split or extract metadata)
  - Missing split documents (need to run split_volume.py)
  - Empty metadata in string_match_results (title, date, doc_type)
  - Annotation result JSON files that are malformed or missing
  - Volumes with annotation XMLs but no string_match_results
  - Volumes with string_match_results but no annotation source

Usage:
    python3 scripts/validate_data.py          # Check everything
    python3 scripts/validate_data.py --fix    # Check and offer fixes
"""

import glob
import json
import os
import sys
from pathlib import Path

os.chdir(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = Path("..").resolve()

# ANSI colors
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
BOLD = "\033[1m"
RESET = "\033[0m"


def warn(msg):
    print(f"  {YELLOW}WARNING:{RESET} {msg}")


def error(msg):
    print(f"  {RED}ERROR:{RESET} {msg}")


def ok(msg):
    print(f"  {GREEN}OK:{RESET} {msg}")


def check_volume_sources():
    """Check that all expected volume XML files exist."""
    print(f"\n{BOLD}1. Volume source files{RESET}")
    issues = []

    # Volumes referenced by annotation XMLs
    annotation_vols = set()
    for f in sorted((BASE_DIR / "annotations").glob("annotations_*.xml")):
        vol_id = f.stem.replace("annotations_", "")
        annotation_vols.add(vol_id)

    # Volumes referenced by string_match_results
    result_vols = set()
    for f in sorted((BASE_DIR / "data" / "documents").glob("*/string_match_results_*.json")):
        vol_id = f.parent.name
        result_vols.add(vol_id)

    all_vols = sorted(annotation_vols | result_vols)

    for vol_id in all_vols:
        vol_xml = BASE_DIR / "volumes" / f"{vol_id}.xml"
        split_dir = BASE_DIR / "data" / "documents" / vol_id
        has_split_docs = split_dir.exists() and any(split_dir.glob("d*.xml"))

        if not vol_xml.exists() and not has_split_docs:
            error(f"{vol_id}: No volume XML and no split documents")
            issues.append(("missing_source", vol_id))
        elif not vol_xml.exists():
            warn(f"{vol_id}: No volume XML (split docs exist, but can't regenerate)")
            issues.append(("missing_volume_xml", vol_id))
        elif not has_split_docs:
            warn(f"{vol_id}: Volume XML exists but not split yet")
            issues.append(("needs_split", vol_id))
        else:
            ok(f"{vol_id}: Volume XML and split documents present")

    return issues


def check_results_integrity():
    """Check string_match_results JSON files for completeness."""
    print(f"\n{BOLD}2. String match results integrity{RESET}")
    issues = []

    result_files = sorted(
        (BASE_DIR / "data" / "documents").glob("*/string_match_results_*.json")
    )

    if not result_files:
        error("No string_match_results files found")
        return [("no_results",)]

    for f in result_files:
        vol_id = f.parent.name

        # Try to parse
        try:
            with open(f) as fh:
                data = json.load(fh)
        except json.JSONDecodeError as e:
            error(f"{vol_id}: Malformed JSON — {e}")
            issues.append(("bad_json", vol_id, str(e)))
            continue

        # Check required keys
        for key in ("metadata", "by_document", "by_term"):
            if key not in data:
                error(f"{vol_id}: Missing top-level key '{key}'")
                issues.append(("missing_key", vol_id, key))

        if "by_document" not in data:
            continue

        docs = data["by_document"]
        total = len(docs)
        empty_title = sum(1 for d in docs.values() if not d.get("title"))
        empty_date = sum(1 for d in docs.values() if not d.get("date"))
        empty_context = sum(
            1
            for d in docs.values()
            for m in d.get("matches", [])
            if not m.get("sentence")
        )
        total_matches = sum(d.get("match_count", 0) for d in docs.values())

        if empty_title == total and total > 0:
            error(
                f"{vol_id}: ALL {total} documents have empty titles "
                f"(missing volume XML or split docs)"
            )
            issues.append(("all_empty_titles", vol_id, total))
        elif empty_title > 0:
            warn(f"{vol_id}: {empty_title}/{total} documents have empty titles")
            issues.append(("some_empty_titles", vol_id, empty_title, total))

        if empty_date == total and total > 0:
            error(f"{vol_id}: ALL {total} documents have empty dates")
            issues.append(("all_empty_dates", vol_id, total))
        elif empty_date > 0:
            warn(f"{vol_id}: {empty_date}/{total} documents have empty dates")
            issues.append(("some_empty_dates", vol_id, empty_date, total))

        if empty_context == total_matches and total_matches > 0:
            warn(
                f"{vol_id}: No sentence context for any match "
                f"(converted from Airtable annotations)"
            )
            issues.append(("no_context", vol_id, total_matches))
        elif empty_context > 0:
            warn(
                f"{vol_id}: {empty_context}/{total_matches} matches "
                f"have no sentence context"
            )

        if not issues or issues[-1][1] != vol_id:
            ok(f"{vol_id}: {total} docs, {total_matches} matches — all good")

    return issues


def check_coverage():
    """Check that all annotation sources have corresponding results."""
    print(f"\n{BOLD}3. Coverage check{RESET}")
    issues = []

    annotation_vols = set()
    for f in sorted((BASE_DIR / "annotations").glob("annotations_*.xml")):
        vol_id = f.stem.replace("annotations_", "")
        annotation_vols.add(vol_id)

    result_vols = set()
    for f in sorted(
        (BASE_DIR / "data" / "documents").glob("*/string_match_results_*.json")
    ):
        vol_id = f.parent.name
        result_vols.add(vol_id)

    # Annotation XMLs without results
    missing_results = annotation_vols - result_vols
    for vol_id in sorted(missing_results):
        warn(
            f"{vol_id}: Has Airtable annotations but no string_match_results "
            f"(run convert_airtable_annotations.py)"
        )
        issues.append(("no_results_for_annotations", vol_id))

    # Results without any source (neither annotation XML nor split docs with context)
    for vol_id in sorted(result_vols):
        has_annotations = vol_id in annotation_vols
        split_dir = BASE_DIR / "data" / "documents" / vol_id
        has_split_docs = split_dir.exists() and any(split_dir.glob("d*.xml"))
        if has_annotations or has_split_docs:
            ok(f"{vol_id}: Has annotation source")
        else:
            warn(f"{vol_id}: Results exist but no annotation source found")
            issues.append(("orphan_results", vol_id))

    return issues


def print_summary(all_issues):
    """Print a summary with suggested fixes."""
    errors = [i for i in all_issues if i[0].startswith("all_") or i[0] in ("bad_json", "missing_source", "missing_key", "no_results")]
    warnings = [i for i in all_issues if i not in errors]

    print(f"\n{'=' * 60}")
    if not all_issues:
        print(f"{GREEN}{BOLD}All checks passed!{RESET}")
        return

    print(f"{BOLD}Summary: {len(errors)} error(s), {len(warnings)} warning(s){RESET}")

    # Group suggested fixes
    needs_split = [i[1] for i in all_issues if i[0] == "needs_split"]
    needs_source = [i[1] for i in all_issues if i[0] in ("missing_source", "missing_volume_xml")]
    needs_convert = [i[1] for i in all_issues if i[0] == "no_results_for_annotations"]

    if needs_split:
        print(f"\n{BOLD}Fix: Split these volumes:{RESET}")
        print(f"  python3 scripts/split_volume.py {' '.join(needs_split)}")

    if needs_source:
        print(f"\n{BOLD}Fix: Download missing volume XML files:{RESET}")
        for vol_id in needs_source:
            print(f"  {vol_id} — download to volumes/{vol_id}.xml, then split and re-run converter")

    if needs_convert:
        print(f"\n{BOLD}Fix: Convert Airtable annotations:{RESET}")
        print(f"  python3 scripts/convert_airtable_annotations.py {' '.join(needs_convert)}")

    no_context = [i[1] for i in all_issues if i[0] == "no_context"]
    if no_context:
        print(f"\n{BOLD}Note: These volumes have no sentence context (Airtable-sourced):{RESET}")
        for vol_id in no_context:
            print(f"  {vol_id}")


def main():
    print(f"{BOLD}Validating annotation data...{RESET}")

    all_issues = []
    all_issues.extend(check_volume_sources())
    all_issues.extend(check_results_integrity())
    all_issues.extend(check_coverage())

    print_summary(all_issues)

    return 1 if any(
        i[0].startswith("all_") or i[0] in ("bad_json", "missing_source", "missing_key")
        for i in all_issues
    ) else 0


if __name__ == "__main__":
    sys.exit(main())
