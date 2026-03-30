#!/usr/bin/env python3
"""
rebuild.py — Unified build orchestrator for the FRUS subject taxonomy pipeline.

Replaces the three separate orchestrators:
  - run_reviewed_pipeline.py (per-volume post-review)
  - rebuild_taxonomy_review.py (taxonomy rebuild)
  - rebuild_mockup.py (mockup rebuild)

Usage:
    python3 scripts/rebuild.py --volume frus1969-76v19p2   # per-volume pipeline
    python3 scripts/rebuild.py --taxonomy                   # rebuild taxonomy review
    python3 scripts/rebuild.py --mockup                     # rebuild mockup
    python3 scripts/rebuild.py --all                        # everything
    python3 scripts/rebuild.py --steps build-variant-groups,build-taxonomy-xml  # ad hoc
"""

import argparse
import os
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Step definitions ─────────────────────────────────────────────────

STEPS = {
    "apply-review-decisions": {
        "label": "Apply review decisions",
        "cmd": lambda vol: [sys.executable, "-u", "apply_review_decisions.py", "--volume", vol],
        "requires_volume": True,
    },
    "build-variant-groups": {
        "label": "Build variant groups",
        "cmd": lambda vol: [sys.executable, "-u", "build_variant_groups.py"],
    },
    "apply-curated-annotations": {
        "label": "Apply curated annotations",
        "cmd": lambda vol: [sys.executable, "-u", "apply_curated_annotations.py", vol],
        "requires_volume": True,
    },
    "merge-annotations": {
        "label": "Merge annotations into volume",
        "cmd": lambda vol: [sys.executable, "-u", "merge_annotations.py", vol],
        "requires_volume": True,
    },
    "build-appearances": {
        "label": "Build document appearances",
        "cmd": lambda vol: [sys.executable, "-u", "build_appearances.py"],
    },
    "build-taxonomy-xml": {
        "label": "Build taxonomy XML",
        "cmd": lambda vol: [sys.executable, "-u", "build_taxonomy_lcsh.py", "build"],
    },
    "build-taxonomy-review": {
        "label": "Build taxonomy review HTML",
        "cmd": lambda vol: [sys.executable, "-u", "build_taxonomy_review.py"],
    },
    "generate-mockup-data": {
        "label": "Generate mockup data",
        "cmd": lambda vol: [sys.executable, "-u", "generate_mockup_data.py"],
    },
    "build-mockup-html": {
        "label": "Build mockup HTML",
        "cmd": lambda vol: [sys.executable, "-u", "build_mockup_html.py"],
    },
}

STEP_ORDER = [
    "apply-review-decisions",
    "build-variant-groups",
    "apply-curated-annotations",
    "merge-annotations",
    "build-appearances",
    "build-taxonomy-xml",
    "build-taxonomy-review",
    "generate-mockup-data",
    "build-mockup-html",
]

PROFILES = {
    "volume": [
        "apply-review-decisions",
        "build-variant-groups",
        "apply-curated-annotations",
        "merge-annotations",
        "build-appearances",
        "build-taxonomy-xml",
        "generate-mockup-data",
        "build-mockup-html",
    ],
    "taxonomy": [
        "build-variant-groups",
        "build-appearances",
        "build-taxonomy-xml",
        "build-taxonomy-review",
    ],
    "mockup": [
        "build-variant-groups",
        "build-appearances",
        "build-taxonomy-xml",
        "generate-mockup-data",
        "build-mockup-html",
    ],
    "all": STEP_ORDER,
}


def log(msg):
    """Print with flush for SSE streaming."""
    print(msg, flush=True)


def run_step(step_name, volume_id=None):
    """Run a single pipeline step."""
    step = STEPS[step_name]

    if step.get("requires_volume") and not volume_id:
        log(f"  SKIP {step_name} (requires --volume)")
        return True

    cmd = step["cmd"](volume_id or "")
    label = step["label"]

    log(f"\n{'─' * 60}")
    log(f"Step: {label}")
    log(f"  Running: {' '.join(cmd)}")
    log(f"{'─' * 60}")

    result = subprocess.run(
        cmd,
        cwd=SCRIPT_DIR,
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    )

    if result.returncode != 0:
        log(f"\nERROR: {label} failed (exit code {result.returncode})")
        return False

    log(f"  ✓ {label} complete")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Unified build orchestrator for FRUS subject taxonomy"
    )
    parser.add_argument("--volume", "--vol", help="Run per-volume pipeline (e.g., frus1969-76v19p2)")
    parser.add_argument("--taxonomy", action="store_true", help="Rebuild taxonomy review")
    parser.add_argument("--mockup", action="store_true", help="Rebuild mockup")
    parser.add_argument("--all", action="store_true", help="Run all steps")
    parser.add_argument("--steps", help="Comma-separated list of specific steps to run")
    args = parser.parse_args()

    # Determine which steps to run
    if args.steps:
        steps = [s.strip() for s in args.steps.split(",")]
        for s in steps:
            if s not in STEPS:
                log(f"ERROR: Unknown step '{s}'. Available: {', '.join(STEP_ORDER)}")
                sys.exit(1)
        profile_name = "custom"
    elif args.volume:
        steps = PROFILES["volume"]
        profile_name = f"volume ({args.volume})"
    elif args.taxonomy:
        steps = PROFILES["taxonomy"]
        profile_name = "taxonomy"
    elif args.mockup:
        steps = PROFILES["mockup"]
        profile_name = "mockup"
    elif args.all:
        steps = PROFILES["all"]
        profile_name = "all"
    else:
        log("ERROR: Specify --volume, --taxonomy, --mockup, --all, or --steps")
        sys.exit(1)

    log("=" * 60)
    log(f"Rebuild: {profile_name}")
    log(f"Steps: {', '.join(steps)}")
    if args.volume:
        log(f"Volume: {args.volume}")
    log("=" * 60)

    for step_name in steps:
        if not run_step(step_name, args.volume):
            log(f"\nPipeline failed at: {step_name}")
            sys.exit(1)

    log(f"\n{'=' * 60}")
    log(f"Rebuild complete: {profile_name}")
    log("=" * 60)


if __name__ == "__main__":
    main()
