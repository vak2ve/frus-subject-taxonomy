#!/usr/bin/env python3
"""
Rebuild hsg-subjects-mockup.html in one pass.

Incorporates all review decisions (LCSH, category overrides, merges,
annotations) into the taxonomy, then regenerates the mockup HTML:

  1. build_variant_groups.py              — apply merge/dedup decisions
  2. merge_annotations_to_appearances.py  — merge string-match annotations into doc appearances
  3. build_taxonomy_lcsh.py build         — rebuild taxonomy XML (skip LCSH fetch)
  4. generate_mockup_data.py              — build sidebar & subject JSON from updated taxonomy
  5. build_mockup_html.py                 — embed JSON into self-contained HTML

Usage:
    python3 rebuild_mockup.py
"""

import os
import subprocess
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

STEPS = [
    ("Rebuilding variant groups", [sys.executable, "-u", "build_variant_groups.py"]),
    ("Merging annotations to appearances", [sys.executable, "-u", "merge_annotations_to_appearances.py"]),
    ("Rebuilding taxonomy XML", [sys.executable, "-u", "build_taxonomy_lcsh.py", "build"]),
    ("Generating mockup data", [sys.executable, "-u", "generate_mockup_data.py"]),
    ("Building hsg-subjects-mockup.html", [sys.executable, "-u", "build_mockup_html.py"]),
]


def log(msg):
    """Print with immediate flush so SSE streaming sees output in real time."""
    print(msg, flush=True)


def main():
    log("=" * 60)
    log("Rebuilding HSG subjects mockup (full chain)")
    log("=" * 60)

    for label, cmd in STEPS:
        log("")
        log("─" * 60)
        log(f"Step: {label}")
        log(f"  Running: {' '.join(cmd)}")
        log("─" * 60)

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
    log("HSG subjects mockup rebuild complete!")
    log("  Open: http://localhost:9090/hsg-subjects-mockup.html")
    log("=" * 60)


if __name__ == "__main__":
    main()
