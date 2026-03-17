#!/usr/bin/env python3
"""
Rebuild the taxonomy and taxonomy-review.html in one pass.

Runs the necessary pipeline steps to incorporate annotation review
decisions into the taxonomy, then regenerates the review HTML:

  1. build_variant_groups.py      — rebuild variant groupings
  2. extract_doc_appearances.py   — recalculate document appearances
  3. build_taxonomy_lcsh.py build — rebuild taxonomy XML (skip LCSH fetch)
  4. build_taxonomy_review.py     — regenerate the HTML review tool

Usage:
    python3 rebuild_taxonomy_review.py
"""

import os
import subprocess
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

STEPS = [
    ("Rebuilding variant groups", [sys.executable, "-u", "build_variant_groups.py"]),
    ("Extracting document appearances", [sys.executable, "-u", "extract_doc_appearances.py"]),
    ("Rebuilding taxonomy XML", [sys.executable, "-u", "build_taxonomy_lcsh.py", "build"]),
    ("Generating taxonomy-review.html", [sys.executable, "-u", "build_taxonomy_review.py"]),
]


def log(msg):
    """Print with immediate flush so SSE streaming sees output in real time."""
    print(msg, flush=True)


def main():
    log("=" * 60)
    log("Rebuilding taxonomy review (full chain)")
    log("=" * 60)

    for label, cmd in STEPS:
        log("")
        log("─" * 60)
        log(f"Step: {label}")
        log(f"  Running: {' '.join(cmd)}")
        log("─" * 60)

        # Pipe child stdout/stderr through so _stream_subprocess captures it
        result = subprocess.run(
            cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            stdout=sys.stdout,
            stderr=subprocess.STDOUT,
        )

        if result.returncode != 0:
            log(f"\nERROR: {label} failed (exit code {result.returncode})")
            sys.exit(result.returncode)

        log(f"  ✓ {label} complete")

    log("")
    log("=" * 60)
    log("Taxonomy review rebuild complete!")
    log("  Open: http://localhost:9090/taxonomy-review.html")
    log("=" * 60)


if __name__ == "__main__":
    main()
