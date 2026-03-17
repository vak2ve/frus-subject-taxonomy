#!/usr/bin/env python3
"""
Import a new volume into the annotation review pipeline.

Scans volumes/ for any volume XMLs that have not yet been split and
annotated, then processes them and rebuilds the review HTML:

  1. Detect unprocessed volumes in volumes/
  2. Split each new volume into per-document files
  3. Run string-match annotation on each new volume
  4. Rebuild string-match-review.html with all volumes

Can also process a specific volume by passing its ID as an argument.

Usage:
    python3 import_volume.py                    # Auto-detect new volumes
    python3 import_volume.py frus1981-88v12     # Process a specific volume
"""

import glob
import os
import subprocess
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

ROOT_DIR = os.path.abspath("..")


def log(msg):
    """Print with immediate flush so SSE streaming sees output in real time."""
    print(msg, flush=True)


def find_unprocessed_volumes():
    """Find volumes in volumes/ that don't yet have string_match_results."""
    volume_files = sorted(glob.glob(os.path.join(ROOT_DIR, "volumes", "*.xml")))
    unprocessed = []
    for vf in volume_files:
        vol_id = os.path.splitext(os.path.basename(vf))[0]
        results_file = os.path.join(
            ROOT_DIR, "data", "documents", vol_id,
            f"string_match_results_{vol_id}.json"
        )
        if not os.path.exists(results_file):
            unprocessed.append(vol_id)
    return unprocessed


def process_volume(vol_id):
    """Split and annotate a single volume."""
    volume_file = os.path.join(ROOT_DIR, "volumes", f"{vol_id}.xml")
    if not os.path.exists(volume_file):
        log(f"  ERROR: Volume file not found: volumes/{vol_id}.xml")
        return False

    docs_dir = os.path.join(ROOT_DIR, "data", "documents", vol_id)

    # Step 1: Split volume into documents (skip if already split)
    has_docs = os.path.isdir(docs_dir) and glob.glob(os.path.join(docs_dir, "d*.xml"))
    if has_docs:
        doc_count = len(glob.glob(os.path.join(docs_dir, "d*.xml")))
        log(f"  Already split: {doc_count} documents in data/documents/{vol_id}/")
    else:
        log(f"  Splitting volume into documents...")
        result = subprocess.run(
            [sys.executable, "-u", "split_volume.py", vol_id],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=sys.stdout,
            stderr=subprocess.STDOUT,
        )
        if result.returncode != 0:
            log(f"  ERROR: split_volume.py failed (exit code {result.returncode})")
            return False

    # Step 2: Run string-match annotation
    log(f"  Running string-match annotation...")
    result = subprocess.run(
        [sys.executable, "-u", "annotate_documents.py",
         os.path.join("..", "data", "documents", vol_id)],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )
    if result.returncode != 0:
        log(f"  ERROR: annotate_documents.py failed (exit code {result.returncode})")
        return False

    return True


def main():
    log("=" * 60)
    log("Import Volume")
    log("=" * 60)

    # Determine which volumes to process
    if len(sys.argv) > 1:
        # Specific volume(s) requested
        volumes = sys.argv[1:]
        log(f"\nProcessing {len(volumes)} specified volume(s):")
        for v in volumes:
            log(f"  - {v}")
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

        if process_volume(vol_id):
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
        log(f"  Succeeded: {', '.join(succeeded)}")
    if failed:
        log(f"  Failed: {', '.join(failed)}")
    log(f"  Open: http://localhost:9090/string-match-review.html")
    log("=" * 60)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
