#!/usr/bin/env python3
"""
DEPRECATED: Superseded by scan_tei_headers.py which builds the taxonomy XML
directly from TEI headers in a single pass. Kept for reference only.

build_metadata_taxonomy.py — Build taxonomy XML from TEI header metadata.

Reads metadata_results_{vol}.json files produced by extract_header_metadata.py
and builds a subject-taxonomy-metadata.xml in the same schema as the main
subject-taxonomy-lcsh.xml, but sourced entirely from TEI header metadata.

Categories and subcategories come directly from the TEI header @category and
@subcategory attributes rather than keyword matching.

Usage:
    python3 build_metadata_taxonomy.py
    python3 build_metadata_taxonomy.py --apply-overrides   # apply config/ overrides
"""

import argparse
import json
import os
import re
from collections import defaultdict
from datetime import date
from pathlib import Path

from lxml import etree

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PIPELINE_DIR.parent
DATA_DIR = PIPELINE_DIR / "data"
CONFIG_DIR = REPO_ROOT / "config"
OUTPUT_TAXONOMY = PIPELINE_DIR / "subject-taxonomy-metadata.xml"
DOC_APPEARANCES_FILE = PIPELINE_DIR / "data" / "document_appearances.json"


def natural_sort_key(s):
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", s)]


def format_category_label(slug):
    """Convert kebab-case slug to Title Case label."""
    if not slug:
        return "Uncategorized"
    return slug.replace("-", " ").title().replace("And", "and").replace("Of", "of")


def load_metadata_results():
    """Load all metadata_results_{vol}.json files and aggregate subjects.

    Uses the pre-aggregated by_term section for efficiency, falling back
    to by_document only when by_term is unavailable.
    """
    subjects = {}  # ref -> aggregated data
    doc_appearances = defaultdict(lambda: defaultdict(list))  # ref -> vol -> [docs]
    vols_processed = 0

    vol_dirs = sorted([d for d in DATA_DIR.iterdir() if d.is_dir()], key=lambda p: p.name)

    for vol_dir in vol_dirs:
        results_files = list(vol_dir.glob("metadata_results_*.json"))
        if not results_files:
            continue

        with open(results_files[0]) as f:
            results = json.load(f)

        vol_id = results["metadata"]["volume_id"]
        vols_processed += 1

        # Use by_term for efficient aggregation (avoids iterating all doc matches)
        by_term = results.get("by_term", {})
        if by_term:
            for ref, term_data in by_term.items():
                # Record doc appearances
                for doc_id in term_data.get("documents", []):
                    doc_appearances[ref][vol_id].append(doc_id)

                if ref not in subjects:
                    subjects[ref] = {
                        "name": term_data.get("term", ""),
                        "ref": ref,
                        "type": term_data.get("type", "topic"),
                        "category": term_data.get("category", "Uncategorized"),
                        "subcategory": term_data.get("subcategory", "General"),
                        "lcsh_uri": term_data.get("lcsh_uri", ""),
                        "lcsh_match": term_data.get("lcsh_match", ""),
                        "count": 0,
                        "volumes": set(),
                    }

                subjects[ref]["count"] += term_data.get("total_occurrences", 0)
                subjects[ref]["volumes"].add(vol_id)
        else:
            # Fallback: iterate by_document
            for doc_id, doc_data in results.get("by_document", {}).items():
                for match in doc_data.get("matches", []):
                    ref = match["ref"]
                    doc_appearances[ref][vol_id].append(doc_id)

                    if ref not in subjects:
                        subjects[ref] = {
                            "name": match["term"],
                            "ref": ref,
                            "type": match.get("type", "topic"),
                            "category": match.get("category", "Uncategorized"),
                            "subcategory": match.get("subcategory", "General"),
                            "lcsh_uri": match.get("lcsh_uri", ""),
                            "lcsh_match": match.get("lcsh_match", ""),
                            "count": 0,
                            "volumes": set(),
                        }

                    subjects[ref]["count"] += 1
                    subjects[ref]["volumes"].add(vol_id)

    # Finalize: sort doc lists and convert volume sets to counts
    for ref, data in subjects.items():
        data["volumes_count"] = len(data["volumes"])
        data["appears_in"] = ", ".join(sorted(data["volumes"]))
        del data["volumes"]

        # Sort doc IDs within each volume
        if ref in doc_appearances:
            for vol_id in doc_appearances[ref]:
                doc_appearances[ref][vol_id] = sorted(
                    set(doc_appearances[ref][vol_id]),
                    key=natural_sort_key
                )

    # Convert defaultdict to regular dict
    doc_apps = {ref: dict(vols) for ref, vols in doc_appearances.items()}

    print(f"Loaded {len(subjects)} unique subjects from {vols_processed} volumes")
    return subjects, doc_apps


def apply_config_overrides(subjects):
    """Apply category overrides from config/category_overrides.json."""
    overrides_path = CONFIG_DIR / "category_overrides.json"
    if not overrides_path.exists():
        return subjects

    with open(overrides_path) as f:
        overrides = json.load(f)

    applied = 0
    for entry in overrides:
        ref = entry["ref"]
        if ref in subjects:
            subjects[ref]["category"] = entry["to_category"]
            subjects[ref]["subcategory"] = entry["to_subcategory"]
            applied += 1

    print(f"  Applied {applied} category overrides")
    return subjects


def apply_review_state(subjects, doc_apps):
    """Apply decisions from metadata_review_state.json (exclusions, merges, etc.)."""
    state_path = PIPELINE_DIR / "metadata_review_state.json"
    if not state_path.exists():
        return subjects, doc_apps

    with open(state_path) as f:
        state = json.load(f)

    # Apply exclusions
    exclusions = state.get("exclusions", {})
    excluded = 0
    for ref in exclusions:
        if ref in subjects:
            subjects[ref]["excluded"] = True
            excluded += 1

    # Apply merges
    merges = state.get("merge_decisions", {})
    merged = 0
    for source_ref, merge_info in merges.items():
        target_ref = merge_info.get("targetRef", "")
        if source_ref in subjects and target_ref in subjects:
            # Transfer counts
            subjects[target_ref]["count"] += subjects[source_ref]["count"]
            # Transfer doc appearances
            for vol, docs in doc_apps.get(source_ref, {}).items():
                existing = set(doc_apps.get(target_ref, {}).get(vol, []))
                existing.update(docs)
                if target_ref not in doc_apps:
                    doc_apps[target_ref] = {}
                doc_apps[target_ref][vol] = sorted(existing, key=natural_sort_key)
            # Mark source as merged
            subjects[source_ref]["merged_into"] = target_ref
            merged += 1

    # Apply category overrides from review state
    cat_overrides = state.get("category_overrides", {})
    for ref, override in cat_overrides.items():
        if ref in subjects:
            subjects[ref]["category"] = override.get("to_category", subjects[ref]["category"])
            subjects[ref]["subcategory"] = override.get("to_subcategory", subjects[ref]["subcategory"])

    if excluded:
        print(f"  Applied {excluded} exclusions from review state")
    if merged:
        print(f"  Applied {merged} merges from review state")

    return subjects, doc_apps


def build_taxonomy_xml(subjects, doc_apps, apply_overrides=False):
    """Build the taxonomy XML document."""

    if apply_overrides:
        subjects = apply_config_overrides(subjects)

    subjects, doc_apps = apply_review_state(subjects, doc_apps)

    # Organize by category -> subcategory -> subjects
    categories = defaultdict(lambda: defaultdict(list))
    excluded_entries = []
    merged_entries = []

    for ref, data in subjects.items():
        if data.get("excluded"):
            excluded_entries.append((ref, data))
            continue
        if data.get("merged_into"):
            merged_entries.append((ref, data))
            continue

        cat = data["category"]
        sub = data["subcategory"]
        categories[cat][sub].append((ref, data))

    # Count active subjects
    active_count = sum(
        len(subjs)
        for subs in categories.values()
        for subjs in subs.values()
    )

    # Build XML
    root = etree.Element("taxonomy", attrib={
        "source": "tei-header-metadata",
        "authority": "Office of the Historian (history.state.gov)",
        "generated": date.today().isoformat(),
        "total-subjects": str(active_count),
        "pipeline": "metadata",
    })

    # Sort categories by total annotation count
    sorted_cats = sorted(
        categories.items(),
        key=lambda x: sum(
            d["count"] for subs in x[1].values() for _, d in subs
        ),
        reverse=True,
    )

    for cat_name, subcats in sorted_cats:
        cat_total = sum(d["count"] for subs in subcats.values() for _, d in subs)
        cat_count = sum(len(subs) for subs in subcats.values())

        cat_elem = etree.SubElement(root, "category", attrib={
            "label": cat_name,
            "total-annotations": str(cat_total),
            "total-subjects": str(cat_count),
        })

        sorted_subs = sorted(
            subcats.items(),
            key=lambda x: sum(d["count"] for _, d in x[1]),
            reverse=True,
        )

        for sub_name, subject_list in sorted_subs:
            sub_total = sum(d["count"] for _, d in subject_list)

            sub_elem = etree.SubElement(cat_elem, "subcategory", attrib={
                "label": sub_name,
                "total-annotations": str(sub_total),
                "total-subjects": str(len(subject_list)),
            })

            # Sort subjects by count
            for ref, sdata in sorted(subject_list, key=lambda x: x[1]["count"], reverse=True):
                apps = doc_apps.get(ref, {})
                count = sum(len(docs) for docs in apps.values()) if apps else sdata["count"]
                volumes = len(apps) if apps else sdata["volumes_count"]

                attribs = {
                    "ref": ref,
                    "type": sdata.get("type", "topic"),
                    "count": str(count),
                    "volumes": str(volumes),
                }
                if sdata.get("lcsh_uri") and sdata.get("lcsh_match") in ("exact", "good_close"):
                    attribs["lcsh-uri"] = sdata["lcsh_uri"]
                    attribs["lcsh-match"] = sdata["lcsh_match"]

                subj_elem = etree.SubElement(sub_elem, "subject", **attribs)

                name_elem = etree.SubElement(subj_elem, "name")
                name_elem.text = sdata["name"]

                if sdata.get("appears_in"):
                    ai_elem = etree.SubElement(subj_elem, "appearsIn")
                    ai_elem.text = sdata["appears_in"]

                # Document-level appearances
                if apps:
                    docs_elem = etree.SubElement(subj_elem, "documents")
                    for vol_id, doc_ids in sorted(apps.items()):
                        vol_elem = etree.SubElement(docs_elem, "volume", id=vol_id)
                        vol_elem.text = ", ".join(doc_ids)

    # Excluded section
    all_excluded = excluded_entries + merged_entries
    if all_excluded:
        excl_elem = etree.SubElement(root, "excluded", attrib={"total": str(len(all_excluded))})
        for ref, data in sorted(all_excluded, key=lambda x: x[1]["name"].lower()):
            attribs = {"ref": ref}
            if data.get("merged_into"):
                attribs["reason"] = "merged"
                attribs["canonical-ref"] = data["merged_into"]
            else:
                attribs["reason"] = "excluded"
            entry_elem = etree.SubElement(excl_elem, "entry", **attribs)
            name_elem = etree.SubElement(entry_elem, "name")
            name_elem.text = data["name"]

    # Write
    tree = etree.ElementTree(root)
    etree.indent(tree, space="    ")
    tree.write(str(OUTPUT_TAXONOMY), xml_declaration=True, encoding="UTF-8", pretty_print=True)

    print(f"\nTaxonomy written to: {OUTPUT_TAXONOMY}")
    print(f"  Active subjects: {active_count}")
    print(f"  Categories: {len(categories)}")
    for cat_name, subcats in sorted_cats:
        cat_count = sum(len(subs) for subs in subcats.values())
        print(f"    {cat_name}: {cat_count} subjects")
    if all_excluded:
        print(f"  Excluded entries: {len(all_excluded)}")

    return str(OUTPUT_TAXONOMY)


def save_doc_appearances(doc_apps):
    """Save document appearances JSON for use by review tools."""
    out_path = DOC_APPEARANCES_FILE
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(doc_apps, f, indent=2, ensure_ascii=False)
    print(f"Document appearances saved to {out_path} ({len(doc_apps)} subjects)")


def main():
    parser = argparse.ArgumentParser(description="Build taxonomy XML from TEI header metadata")
    parser.add_argument("--apply-overrides", action="store_true",
                        help="Apply category overrides from config/")
    args = parser.parse_args()

    print("=== Building Metadata Taxonomy ===")
    subjects, doc_apps = load_metadata_results()
    save_doc_appearances(doc_apps)
    build_taxonomy_xml(subjects, doc_apps, apply_overrides=args.apply_overrides)


if __name__ == "__main__":
    main()
