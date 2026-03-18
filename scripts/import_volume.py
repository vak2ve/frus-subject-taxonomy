#!/usr/bin/env python3
"""
Import new volumes into the annotation review pipeline.

Scans volumes/ for any volume XMLs that have not yet been split and
annotated, then processes them and rebuilds the review HTML.

Supports batch import by series (e.g., --series 1981-88) and
specific volume IDs as arguments.

  1. Detect unprocessed volumes in volumes/
  2. Split each new volume into per-document files
  3. Run string-match annotation on each new volume
  4. Rebuild string-match-review.html with all volumes

Usage:
    python3 import_volume.py                    # Auto-detect new volumes
    python3 import_volume.py frus1981-88v12     # Process a specific volume
    python3 import_volume.py --series 1981-88   # Import all unprocessed in a series
    python3 import_volume.py --series all       # Import all unprocessed volumes
    python3 import_volume.py --list-series      # List available series and counts
"""

import glob
import os
import re
import subprocess
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

ROOT_DIR = os.path.abspath("..")

# Pattern to extract series from volume IDs like frus1981-88v41, frus1969-76v19p2
SERIES_RE = re.compile(r"^frus(\d{4}-\d{2,4})")


def log(msg):
    """Print with immediate flush so SSE streaming sees output in real time."""
    print(msg, flush=True)


def extract_series(vol_id):
    """Extract the series identifier from a volume ID.

    e.g., 'frus1981-88v41' -> '1981-88'
          'frus1969-76v19p2' -> '1969-76'
    """
    m = SERIES_RE.match(vol_id)
    return m.group(1) if m else "other"


def find_all_volumes():
    """Find all volume XML files in volumes/."""
    volume_files = sorted(glob.glob(os.path.join(ROOT_DIR, "volumes", "*.xml")))
    volumes = []
    for vf in volume_files:
        vol_id = os.path.splitext(os.path.basename(vf))[0]
        volumes.append(vol_id)
    return volumes


def find_unprocessed_volumes(series_filter=None):
    """Find volumes in volumes/ that don't yet have string_match_results.

    If series_filter is provided, only return volumes from that series.
    If series_filter is 'all', return all unprocessed volumes.
    """
    volume_files = sorted(glob.glob(os.path.join(ROOT_DIR, "volumes", "*.xml")))
    unprocessed = []
    for vf in volume_files:
        vol_id = os.path.splitext(os.path.basename(vf))[0]

        # Filter by series if requested
        if series_filter and series_filter != "all":
            if extract_series(vol_id) != series_filter:
                continue

        results_file = os.path.join(
            ROOT_DIR, "data", "documents", vol_id,
            f"string_match_results_{vol_id}.json"
        )
        if not os.path.exists(results_file):
            unprocessed.append(vol_id)
    return unprocessed


def list_series():
    """List available series with counts of total and unprocessed volumes."""
    all_vols = find_all_volumes()
    series_info = {}

    for vol_id in all_vols:
        series = extract_series(vol_id)
        if series not in series_info:
            series_info[series] = {"total": 0, "unprocessed": 0, "volumes": []}
        series_info[series]["total"] += 1
        series_info[series]["volumes"].append(vol_id)

        results_file = os.path.join(
            ROOT_DIR, "data", "documents", vol_id,
            f"string_match_results_{vol_id}.json"
        )
        if not os.path.exists(results_file):
            series_info[series]["unprocessed"] += 1

    return series_info


def process_volume(vol_id, index=None, total=None):
    """Split and annotate a single volume."""
    prefix = f"[{index}/{total}] " if index and total else ""
    volume_file = os.path.join(ROOT_DIR, "volumes", f"{vol_id}.xml")
    if not os.path.exists(volume_file):
        log(f"  {prefix}ERROR: Volume file not found: volumes/{vol_id}.xml")
        return False

    docs_dir = os.path.join(ROOT_DIR, "data", "documents", vol_id)

    # Step 1: Split volume into documents (skip if already split)
    has_docs = os.path.isdir(docs_dir) and glob.glob(os.path.join(docs_dir, "d*.xml"))
    if has_docs:
        doc_count = len(glob.glob(os.path.join(docs_dir, "d*.xml")))
        log(f"  {prefix}Already split: {doc_count} documents in data/documents/{vol_id}/")
    else:
        log(f"  {prefix}Splitting volume into documents...")
        result = subprocess.run(
            [sys.executable, "-u", "split_volume.py", vol_id],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=sys.stdout,
            stderr=subprocess.STDOUT,
        )
        if result.returncode != 0:
            log(f"  {prefix}ERROR: split_volume.py failed (exit code {result.returncode})")
            return False

    # Step 2: Run string-match annotation
    log(f"  {prefix}Running string-match annotation...")
    result = subprocess.run(
        [sys.executable, "-u", "annotate_documents.py",
         os.path.join("..", "data", "documents", vol_id)],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        log(f"  {prefix}ERROR: annotate_documents.py failed (exit code {result.returncode})")
        return False

    return True


def main():
    log("=" * 60)
    log("Import Volume")
    log("=" * 60)

    # Handle --list-series
    if "--list-series" in sys.argv:
        series_info = list_series()
        log("\nAvailable series in volumes/:")
        log("")
        total_all = 0
        unprocessed_all = 0
        for series in sorted(series_info.keys()):
            info = series_info[series]
            total_all += info["total"]
            unprocessed_all += info["unprocessed"]
            status = f"{info['unprocessed']} new" if info["unprocessed"] > 0 else "all imported"
            log(f"  {series:12s}  {info['total']:4d} volumes  ({status})")
        log(f"  {'TOTAL':12s}  {total_all:4d} volumes  ({unprocessed_all} new)")
        return

    # Determine series filter
    series_filter = None
    explicit_volumes = []

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--series":
            if i + 1 < len(args):
                series_filter = args[i + 1]
                i += 2
            else:
                log("ERROR: --series requires a value (e.g., --series 1981-88 or --series all)")
                sys.exit(1)
        elif args[i] == "--list-series":
            i += 1  # handled above
        else:
            explicit_volumes.append(args[i])
            i += 1

    # Determine which volumes to process
    if explicit_volumes:
        volumes = explicit_volumes
        log(f"\nProcessing {len(volumes)} specified volume(s):")
        for v in volumes:
            log(f"  - {v}")
    elif series_filter:
        volumes = find_unprocessed_volumes(series_filter)
        series_label = f"series {series_filter}" if series_filter != "all" else "all series"
        if not volumes:
            log(f"\nNo new volumes found in {series_label}.")
            log("All matching volumes already have annotation results.")

            # Still rebuild the review HTML
            log("\nRebuilding string-match-review.html...")
            result = subprocess.run(
                [sys.executable, "-u", "build_annotation_review.py"],
                cwd=os.path.dirname(os.path.abspath(__file__)),
                stdout=sys.stdout,
                stderr=subprocess.STDOUT,
            )
            if result.returncode != 0:
                log("ERROR: build_annotation_review.py failed")
                sys.exit(result.returncode)
            log("  ✓ Review tool rebuilt")
            log("")
            log("=" * 60)
            log(f"Import complete (no new volumes in {series_label})")
            log("=" * 60)
            return

        log(f"\nFound {len(volumes)} new volume(s) in {series_label}:")
        # Group by series for display
        by_series = {}
        for v in volumes:
            s = extract_series(v)
            by_series.setdefault(s, []).append(v)
        for s in sorted(by_series.keys()):
            log(f"\n  Series {s} ({len(by_series[s])} volumes):")
            for v in by_series[s]:
                log(f"    - {v}")
    else:
        # Auto-detect unprocessed volumes
        volumes = find_unprocessed_volumes()
        if not volumes:
            log("\nNo new volumes found.")
            log("All volumes in volumes/ already have annotation results.")
            log("\nTo re-annotate an existing volume, pass its ID:")
            log("  python3 import_volume.py frus1981-88v12")
            log("")
            log("=" * 60)

            # Still rebuild the review HTML in case data changed
            log("\nRebuilding string-match-review.html...")
            result = subprocess.run(
                [sys.executable, "-u", "build_annotation_review.py"],
                cwd=os.path.dirname(os.path.abspath(__file__)),
                stdout=sys.stdout,
                stderr=subprocess.STDOUT,
            )
            if result.returncode != 0:
                log("ERROR: build_annotation_review.py failed")
                sys.exit(result.returncode)
            log("  ✓ Review tool rebuilt")
            log("")
            log("=" * 60)
            log("Import complete (no new volumes)")
            log("=" * 60)
            return

        log(f"\nFound {len(volumes)} new volume(s) to process:")
        for v in volumes:
            log(f"  - {v}")

    # Process each volume
    failed = []
    succeeded = []
    for i, vol_id in enumerate(volumes):
        log("")
        log("─" * 60)
        log(f"Volume {i + 1}/{len(volumes)}: {vol_id}")
        log("─" * 60)

        if process_volume(vol_id, i + 1, len(volumes)):
            log(f"  ✓ {vol_id} complete")
            succeeded.append(vol_id)
        else:
            log(f"  ✗ {vol_id} failed")
            failed.append(vol_id)

    # Rebuild review HTML
    log("")
    log("─" * 60)
    log("Rebuilding string-match-review.html")
    log("─" * 60)
    result = subprocess.run(
        [sys.executable, "-u", "build_annotation_review.py"],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        log("ERROR: build_annotation_review.py failed")
        sys.exit(result.returncode)
    log("  ✓ Review tool rebuilt")

    # Summary
    log("")
    log("=" * 60)
    log("Import complete!")
    if succeeded:
        log(f"  Succeeded: {len(succeeded)} volume(s)")
        for v in succeeded:
            log(f"    ✓ {v}")
    if failed:
        log(f"  Failed: {len(failed)} volume(s)")
        for v in failed:
            log(f"    ✗ {v}")
    log(f"  Open: http://localhost:9090/string-match-review.html")
    log("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
