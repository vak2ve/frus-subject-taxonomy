#!/usr/bin/env python3
"""
generate_metadata_mockup_data.py — Generate mockup data from metadata pipeline.

Reads the metadata taxonomy XML and document appearances, applies review decisions
(exclusions, merges), and produces sidebar/subject JSON files for the HSG mockup.

Usage:
    python3 generate_metadata_mockup_data.py
"""

import json
import os
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

from lxml import etree

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PIPELINE_DIR.parent
TAXONOMY_PATH = PIPELINE_DIR / "subject-taxonomy-metadata.xml"
DOC_APPEARANCES_PATH = PIPELINE_DIR / "data" / "document_appearances.json"
STATE_PATH = PIPELINE_DIR / "metadata_review_state.json"
DATA_DIR = PIPELINE_DIR / "data"
MOCKUP_DIR = DATA_DIR / "mockup"

# Doc metadata — prefer our own compact version, fall back to main pipeline's
DOC_METADATA_PATH = DATA_DIR / "doc_metadata.json"
DOC_METADATA_FALLBACK = REPO_ROOT / "doc_metadata.json"


def slugify(name):
    """Convert name to URL-safe slug."""
    text = unicodedata.normalize("NFKD", name)
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


def natural_sort_key(s):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", s)]


def load_review_state():
    """Load review state decisions."""
    if not STATE_PATH.exists():
        return {}, {}, {}
    with open(STATE_PATH) as f:
        state = json.load(f)
    return (
        state.get("exclusions", {}),
        state.get("merge_decisions", {}),
        state.get("category_overrides", {}),
    )


def load_doc_metadata():
    """Load document metadata (titles, dates)."""
    for path in [DOC_METADATA_PATH, DOC_METADATA_FALLBACK]:
        if path.exists():
            with open(path) as f:
                return json.load(f)
    return {"documents": {}, "volumes": {}}


def main():
    print("=== Generating Metadata Mockup Data ===")

    if not TAXONOMY_PATH.exists():
        print(f"ERROR: {TAXONOMY_PATH} not found. Run build_metadata_taxonomy.py first.")
        return

    # Load taxonomy
    tree = etree.parse(str(TAXONOMY_PATH))
    root = tree.getroot()

    # Load supporting data
    doc_apps = {}
    if DOC_APPEARANCES_PATH.exists():
        with open(DOC_APPEARANCES_PATH) as f:
            doc_apps = json.load(f)

    doc_meta = load_doc_metadata()
    exclusions, merges, cat_overrides = load_review_state()

    print(f"  Exclusions: {len(exclusions)}, Merges: {len(merges)}, Overrides: {len(cat_overrides)}")

    # Build merged ref mapping
    merged_into = {}  # source_ref -> target_ref
    for src_ref, merge_info in merges.items():
        merged_into[src_ref] = merge_info.get("targetRef", "")

    # Parse taxonomy into categories
    categories = {}  # cat_label -> {sub_label -> [{ref, name, count, ...}]}
    ref_to_name = {}

    for cat in root.findall("category"):
        cat_label = cat.get("label", "")
        for sub in cat.findall("subcategory"):
            sub_label = sub.get("label", "")
            for subj in sub.findall("subject"):
                ref = subj.get("ref", "")
                name_el = subj.find("name")
                name = name_el.text.strip() if name_el is not None and name_el.text else ""
                ref_to_name[ref] = name

                # Skip excluded
                if exclusions.get(ref):
                    continue
                # Skip merged sources
                if ref in merged_into:
                    continue

                count = int(subj.get("count", 0))
                lcsh_uri = subj.get("lcsh-uri", "")

                categories.setdefault(cat_label, {}).setdefault(sub_label, []).append({
                    "ref": ref,
                    "name": name,
                    "count": count,
                    "lcsh_uri": lcsh_uri,
                })

    # Build sidebar data
    sidebar_data = {}
    for cat_label, subcats in sorted(categories.items()):
        sidebar_entries = []
        for sub_label, subjects in sorted(subcats.items()):
            total_docs = sum(s["count"] for s in subjects)
            sidebar_entries.append({
                "id": slugify(sub_label),
                "name": sub_label,
                "docCount": total_docs,
                "subjects": sorted(
                    [{"ref": s["ref"], "name": s["name"], "count": s["count"]} for s in subjects],
                    key=lambda x: -x["count"]
                ),
            })
        sidebar_data[cat_label] = sorted(sidebar_entries, key=lambda x: -x["docCount"])

    # Build subject detail data
    # Pre-index doc_meta documents for faster lookup
    doc_docs = doc_meta.get("documents", {})
    vol_titles = doc_meta.get("volumes", {})

    # Write outputs
    MOCKUP_DIR.mkdir(parents=True, exist_ok=True)

    sidebar_path = DATA_DIR / "mockup_sidebar_data.json"
    with open(sidebar_path, "w") as f:
        json.dump(sidebar_data, f, indent=2, ensure_ascii=False)
    print(f"  Sidebar data: {sidebar_path} ({len(sidebar_data)} categories)")

    # Build subject data per-category to keep memory bounded
    # Also accumulate a lightweight index for the combined file
    subject_index = {}  # ref -> {name, count, vol_count} (lightweight)
    total_subjects = 0

    for cat_label, subcats in categories.items():
        slug = slugify(cat_label)
        cat_subjects = {}

        for sub_label, subjects in subcats.items():
            for s in subjects:
                ref = s["ref"]
                apps = doc_apps.get(ref, {})

                # Also merge in doc appearances from merged subjects
                for src_ref, tgt_ref in merged_into.items():
                    if tgt_ref == ref and src_ref in doc_apps:
                        for vol, docs in doc_apps[src_ref].items():
                            existing = set(apps.get(vol, []))
                            existing.update(docs)
                            apps[vol] = sorted(existing, key=natural_sort_key)

                volumes = {}
                for vol_id, doc_ids in sorted(apps.items()):
                    vol_url = f"https://history.state.gov/historicaldocuments/{vol_id}"
                    vol_title = vol_titles.get(vol_id, vol_id)
                    docs = []
                    for did in doc_ids:
                        key = f"{vol_id}/{did}"
                        dm = doc_docs.get(key, {})
                        docs.append({
                            "id": did,
                            "title": dm.get("t", did),
                            "date": dm.get("d", ""),
                            "url": f"{vol_url}/{did}",
                        })
                    volumes[vol_id] = {
                        "title": vol_title,
                        "url": vol_url,
                        "docs": docs,
                    }

                entry = {
                    "name": s["name"],
                    "count": s["count"],
                    "volumes": volumes,
                }
                if s.get("lcsh_uri"):
                    entry["lcsh"] = s["name"]

                # Note merged subjects
                merged_names = []
                for src_ref, tgt_ref in merged_into.items():
                    if tgt_ref == ref:
                        merged_names.append(ref_to_name.get(src_ref, src_ref))
                if merged_names:
                    entry["merged_names"] = merged_names

                cat_subjects[ref] = entry
                subject_index[ref] = {
                    "name": s["name"],
                    "count": s["count"],
                    "vol_count": len(volumes),
                    "category": cat_label,
                }
                total_subjects += 1

        # Write per-category file immediately (don't accumulate)
        cat_path = MOCKUP_DIR / f"{slug}.json"
        with open(cat_path, "w") as f:
            json.dump(cat_subjects, f, indent=2, ensure_ascii=False)
        print(f"    {cat_label}: {len(cat_subjects)} subjects → {slug}.json")

    # Write lightweight subject index instead of full subject data
    # (the mockup HTML loads per-category files for details)
    subject_path = DATA_DIR / "mockup_subject_data.json"
    with open(subject_path, "w") as f:
        json.dump(subject_index, f, indent=2, ensure_ascii=False)
    print(f"  Subject index: {subject_path} ({total_subjects} subjects)")

    print(f"  Per-category files: {MOCKUP_DIR}/ ({len(categories)} files)")
    print(f"\nDone. {total_subjects} subjects across {len(categories)} categories.")


if __name__ == "__main__":
    main()
