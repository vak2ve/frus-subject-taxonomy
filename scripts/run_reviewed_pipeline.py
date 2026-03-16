#!/usr/bin/env python3
"""
Run the full post-review pipeline for a volume.

Executes all steps in sequence:
  1. Apply review decisions (merge overrides + LCSH rejections)
  2. Rebuild variant groups
  3. Apply curated annotations to TEI documents
  4. Merge annotations into volume file
  5. Extract document appearances
  6. Rebuild taxonomy XML
  7. Generate mockup data
  8. Build mockup HTML

Stops on any failure.

Usage:
    python3 run_reviewed_pipeline.py <volume-id>
    python3 run_reviewed_pipeline.py frus1969-76v19p2
"""

import os
import subprocess
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def run_step(label, cmd):
    """Run a pipeline step, return True on success."""
    print(f"\n{'=' * 60}")
    print(f"  Step: {label}")
    print(f"  Command: {' '.join(cmd)}")
    print(f"{'=' * 60}\n")

    start = time.time()
    result = subprocess.run(cmd, cwd=SCRIPT_DIR)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n  FAILED ({elapsed:.1f}s)")
        return False

    print(f"\n  Done ({elapsed:.1f}s)")
    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 run_reviewed_pipeline.py <volume-id>")
        print("Example: python3 run_reviewed_pipeline.py frus1969-76v19p2")
        sys.exit(1)

    volume_id = sys.argv[1]

    print(f"{'=' * 60}")
    print(f"  Post-Review Pipeline for {volume_id}")
    print(f"{'=' * 60}")

    steps = [
        (
            "Apply review decisions",
            ["python3", "apply_review_decisions.py", "--volume", volume_id],
        ),
        (
            "Rebuild variant groups",
            ["python3", "build_variant_groups.py"],
        ),
        (
            "Apply curated annotations",
            ["python3", "apply_curated_annotations.py", volume_id],
        ),
        (
            "Merge annotations into volume",
            ["python3", "merge_annotations.py", volume_id],
        ),
        (
            "Extract document appearances",
            ["python3", "extract_doc_appearances.py"],
        ),
        (
            "Rebuild taxonomy XML",
            ["python3", "build_taxonomy_lcsh.py", "build"],
        ),
        (
            "Generate mockup data",
            ["python3", "generate_mockup_data.py"],
        ),
        (
            "Build mockup HTML",
            ["python3", "build_mockup_html.py"],
        ),
    ]

    total_start = time.time()
    completed = 0

    for label, cmd in steps:
        if not run_step(label, cmd):
            print(f"\n{'=' * 60}")
            print(f"  PIPELINE FAILED at step: {label}")
            print(f"  Completed {completed}/{len(steps)} steps")
            print(f"{'=' * 60}")
            sys.exit(1)
        completed += 1

    total_elapsed = time.time() - total_start

    print(f"\n{'=' * 60}")
    print(f"  PIPELINE COMPLETE")
    print(f"  All {len(steps)} steps succeeded ({total_elapsed:.1f}s total)")
    print(f"{'=' * 60}")
    print(f"\nOutputs:")
    print(f"  Taxonomy:   subject-taxonomy-lcsh.xml")
    print(f"  Annotated:  {volume_id}-annotated.xml")
    print(f"  Mockup:     hsg-subjects-mockup.html")
    print(f"\nServe locally:")
    print(f"  make serve")
    print(f"  Open http://localhost:9090/hsg-subjects-mockup.html")


if __name__ == "__main__":
    main()
