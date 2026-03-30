#!/usr/bin/env python3
"""
Generate mockup data for hsg-subjects-mockup.html from the current
taxonomy data (lcsh_mapping.json + document_appearances.json + doc_metadata.json).

Outputs two JSON structures:
  - sidebar_data: {category_name: [{id, name, docCount, subjects: [{ref, name, count}]}]}
  - subject_data: {ref: {name, lcsh, count, merged, volumes: {vol_id: {title, url, docs: [{id, title, date, url}]}}}}
"""

import json
import os
import re
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Import categorization from build script
from build_taxonomy_lcsh import HSG_TAXONOMY, categorize_by_hsg, _normalize_name, CATEGORY_OVERRIDES_FILE

# Import shared decision resolution
from resolve_decisions import (
    load_all_decisions,
    apply_dedup_to_mapping,
    apply_merges_to_categories,
    merge_appearances,
    is_excluded,
)

MAPPING_FILE = "../config/lcsh_mapping.json"
HSG_ONLY_SUBJECTS_FILE = "../config/hsg_only_subjects.json"
PROMOTED_CANDIDATES_FILE = "../config/promoted_candidates.json"
DOC_APPEARANCES_FILE = "../document_appearances.json"
DOC_METADATA_FILE = "../doc_metadata.json"
HSG_BASE = "https://history.state.gov/historicaldocuments"

# Global doc_apps reference for appearance-based counting
_doc_apps = {}


def appearance_count(ref, data):
    """Count actual document appearances instead of using the Airtable count.

    Checks both the data's document_appearances and the global doc_apps.
    """
    appearances = data.get("document_appearances", {})
    if not appearances and ref in _doc_apps:
        appearances = _doc_apps[ref]
    return sum(len(docs) for docs in appearances.values())


def _apply_mapping_decisions(mapping, doc_apps, decisions):
    """Apply exclusions and merge appearances from mapping-level merges.

    Handles merges where source refs exist in doc_apps (not promoted candidates).
    Also removes excluded refs.
    """
    # Apply merges: fold source appearances into target
    for source_ref, target_ref in decisions.merge_map.items():
        if source_ref in doc_apps and target_ref in mapping:
            target_apps = doc_apps.get(target_ref, {})
            merge_appearances(target_apps, doc_apps[source_ref])
            doc_apps[target_ref] = target_apps
            mapping[target_ref]["document_appearances"] = target_apps

            merged_refs = mapping[target_ref].get("merged_refs", [])
            if target_ref not in merged_refs:
                merged_refs.append(target_ref)
            if source_ref not in merged_refs:
                merged_refs.append(source_ref)
            mapping[target_ref]["merged_refs"] = merged_refs

        mapping.pop(source_ref, None)
        doc_apps.pop(source_ref, None)

    # Apply exclusions
    for ref in decisions.exclusions:
        mapping.pop(ref, None)
        doc_apps.pop(ref, None)

    print(f"  Applied: {len(decisions.exclusions)} exclusions, {len(decisions.merge_map)} merge sources processed")
    return mapping, doc_apps


def slugify(name):
    """Convert a subcategory name to a CSS-safe ID slug."""
    return re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')


def load_data():
    """Load all source data files."""
    with open(MAPPING_FILE) as f:
        mapping = json.load(f)

    doc_apps = {}
    if os.path.exists(DOC_APPEARANCES_FILE):
        with open(DOC_APPEARANCES_FILE) as f:
            doc_apps = json.load(f)

    doc_meta = {"documents": {}, "volumes": {}}
    if os.path.exists(DOC_METADATA_FILE):
        with open(DOC_METADATA_FILE) as f:
            doc_meta = json.load(f)

    return mapping, doc_apps, doc_meta


def categorize_all(mapping):
    """Categorize all subjects and deduplicate within subcategories.

    Returns: {cat_name: {sub_name: [(ref, data), ...]}}
    """
    categories = {}
    uncategorized = []

    # Load category overrides
    cat_overrides = {}
    if os.path.exists(CATEGORY_OVERRIDES_FILE):
        with open(CATEGORY_OVERRIDES_FILE) as f:
            for entry in json.load(f):
                cat_overrides[entry["ref"]] = (entry["to_category"], entry["to_subcategory"])
        print(f"  Loaded {len(cat_overrides)} category overrides")

    for ref, data in mapping.items():
        name = data.get("name", "")
        lcsh_label = (
            data.get("lcsh_label")
            if data.get("match_quality") in ("exact", "good_close")
            else None
        )

        # Check for manual override first
        if ref in cat_overrides:
            cat_name, sub_name = cat_overrides[ref]
        else:
            cat_name, sub_name = categorize_by_hsg(name, lcsh_label)

        if cat_name and cat_name != "Uncategorized":
            categories.setdefault(cat_name, {}).setdefault(sub_name, []).append(
                (ref, data)
            )
        else:
            uncategorized.append((ref, data))

    # Deduplicate within subcategories
    merged_count = 0
    for cat_name, subcats in categories.items():
        for sub_name, subjects in subcats.items():
            groups = {}
            for ref, data in subjects:
                key = _normalize_name(data.get("name", ""))
                groups.setdefault(key, []).append((ref, data))

            merged = []
            for norm_key, entries in groups.items():
                if len(entries) == 1:
                    merged.append(entries[0])
                    continue

                entries.sort(key=lambda x: appearance_count(x[0], x[1]), reverse=True)
                primary_ref, primary_data = entries[0]
                combined = dict(primary_data)
                combined["merged_refs"] = [r for r, _ in entries]

                # Merge appears_in
                all_vols = set()
                for _, d in entries:
                    for v in d.get("appears_in", "").split(", "):
                        v = v.strip()
                        if v:
                            all_vols.add(v)
                combined["appears_in"] = ", ".join(sorted(all_vols))

                # Merge document_appearances
                merged_docs = {}
                for _, d in entries:
                    merge_appearances(merged_docs, d.get("document_appearances", {}))
                combined["document_appearances"] = merged_docs

                # Keep best LCSH
                for _, d in entries:
                    if d.get("match_quality") == "exact" and d.get("lcsh_uri"):
                        combined["lcsh_uri"] = d["lcsh_uri"]
                        combined["lcsh_label"] = d.get("lcsh_label", "")
                        combined["match_quality"] = "exact"
                        combined["exact_match"] = True
                        break
                    elif d.get("match_quality") == "good_close" and d.get("lcsh_uri"):
                        combined["lcsh_uri"] = d["lcsh_uri"]
                        combined["lcsh_label"] = d.get("lcsh_label", "")

                merged.append((primary_ref, combined))
                merged_count += len(entries) - 1

            subcats[sub_name] = merged

    print(f"  Deduplicated: merged {merged_count} subjects")
    print(f"  Categorized: {sum(len(s) for sc in categories.values() for s in sc.values())} subjects")
    print(f"  Uncategorized: {len(uncategorized)}")

    return categories, uncategorized


def build_subject_entry(ref, data, doc_apps, doc_meta, ref_to_name=None):
    """Build a subject-data entry with full document details."""
    name = data.get("name", "")
    lcsh = data.get("lcsh_label") if data.get("match_quality") in ("exact", "good_close") else None
    merged_refs = data.get("merged_refs", [])
    if ref_to_name is None:
        ref_to_name = {}

    # Get document appearances from the merged data or from doc_apps
    appearances = data.get("document_appearances", {})
    if not appearances and ref in doc_apps:
        appearances = doc_apps[ref]

    # Also check merged refs for additional appearances
    for mref in merged_refs:
        if mref != ref and mref in doc_apps:
            for vol_id, doc_ids in doc_apps[mref].items():
                existing = set(appearances.get(vol_id, []))
                existing.update(doc_ids)
                appearances[vol_id] = sorted(existing)

    volumes = {}
    for vol_id, doc_ids in sorted(appearances.items()):
        if not doc_ids:
            continue  # Skip volumes with no document-level data
        vol_title = doc_meta.get("volumes", {}).get(vol_id, vol_id)
        vol_url = f"{HSG_BASE}/{vol_id}"

        docs = []
        for doc_id in doc_ids:
            doc_key = f"{vol_id}/{doc_id}"
            meta = doc_meta.get("documents", {}).get(doc_key, {})
            docs.append({
                "id": doc_id,
                "title": meta.get("t", doc_id),
                "date": meta.get("d", ""),
                "url": f"{HSG_BASE}/{vol_id}/{doc_id}",
            })
        volumes[vol_id] = {
            "title": vol_title,
            "url": vol_url,
            "docs": docs,
        }

    count = sum(len(vol["docs"]) for vol in volumes.values())

    # Fall back to the count field if no document-level appearances exist
    # (e.g., subjects whose counts come from candidate merges without per-doc data)
    if count == 0 and int(data.get("count", 0) or 0) > 0:
        count = int(data["count"])

    return {
        "name": name,
        "lcsh": lcsh,
        "count": count,
        "merged": merged_refs,
        "merged_names": [ref_to_name.get(r, r) for r in merged_refs],
        "volumes": volumes,
    }


def generate(categories, uncategorized, doc_apps, doc_meta, ref_to_name=None):
    """Generate sidebar_data and subject_data for all categories."""
    sidebar_data = {}
    subject_data = {}

    # Sort categories by total document appearance count
    sorted_cats = sorted(
        categories.items(),
        key=lambda x: sum(
            appearance_count(s[0], s[1])
            for subs in x[1].values()
            for s in subs
        ),
        reverse=True,
    )

    for cat_name, subcats in sorted_cats:
        cat_sidebar = []

        # Sort subcats by document appearance count
        sorted_subs = sorted(
            subcats.items(),
            key=lambda x: sum(appearance_count(s[0], s[1]) for s in x[1]),
            reverse=True,
        )

        for sub_name, subjects in sorted_subs:
            # Sort subjects within subcategory by appearance count
            subjects.sort(key=lambda x: appearance_count(x[0], x[1]), reverse=True)

            sub_id = slugify(sub_name)

            sub_subjects = []
            for ref, data in subjects:
                # Build full subject data entry first to get actual count
                subject_data[ref] = build_subject_entry(ref, data, doc_apps, doc_meta, ref_to_name)
                sub_subjects.append({
                    "ref": ref,
                    "name": data.get("name", ""),
                    "count": subject_data[ref]["count"],
                })

            sub_doc_count = sum(s["count"] for s in sub_subjects)

            cat_sidebar.append({
                "id": sub_id,
                "name": sub_name,
                "docCount": sub_doc_count,
                "subjects": sub_subjects,
            })

        sidebar_data[cat_name] = cat_sidebar

    # Category-level stats
    print("\n  Category stats:")
    for cat_name, subs in sidebar_data.items():
        total_subjects = sum(len(s["subjects"]) for s in subs)
        total_docs = sum(s["docCount"] for s in subs)
        print(f"    {cat_name}: {total_subjects} subjects, {total_docs:,} doc refs")

    print(f"\n  Total subjects in mockup: {len(subject_data)}")

    return sidebar_data, subject_data


def main():
    global _doc_apps
    print("Loading data...")
    mapping, doc_apps, doc_meta = load_data()
    _doc_apps = doc_apps

    # Merge doc_apps into mapping
    for ref, volumes in doc_apps.items():
        if ref in mapping:
            mapping[ref]["document_appearances"] = volumes

    print(f"  Mapping: {len(mapping)} subjects")
    print(f"  Doc appearances: {len(doc_apps)} subjects")
    print(f"  Doc metadata: {len(doc_meta.get('documents', {}))} documents, {len(doc_meta.get('volumes', {}))} volumes")

    # Build ref-to-name lookup BEFORE any merges/removals
    ref_to_name = {ref: data.get("name", ref) for ref, data in mapping.items()}

    # Load all decisions
    print("\nLoading decisions...")
    repo_root = os.path.dirname(os.path.abspath(os.path.join(__file__, "..")))
    decisions = load_all_decisions(repo_root)

    # Apply global dedup decisions
    print("Applying dedup decisions...")
    mapping = apply_dedup_to_mapping(mapping, decisions)

    # Apply mapping-level merges and exclusions
    print("Applying taxonomy decisions...")
    mapping, doc_apps = _apply_mapping_decisions(mapping, doc_apps, decisions)

    print("\nCategorizing...")
    categories, uncategorized = categorize_all(mapping)

    # Merge in HSG-only subjects (tags from the HSG taxonomy with no
    # annotation-pipeline source record)
    if os.path.exists(HSG_ONLY_SUBJECTS_FILE):
        with open(HSG_ONLY_SUBJECTS_FILE) as f:
            hsg_only_subjects = json.load(f)
        existing_refs = set()
        for cat_subs in categories.values():
            for subjects in cat_subs.values():
                for ref, _ in subjects:
                    existing_refs.add(ref)
        hsg_count = 0
        skipped_excluded = 0
        for entry in hsg_only_subjects:
            if entry["ref"] in existing_refs:
                continue
            # Skip excluded subjects
            if is_excluded(entry["ref"], decisions):
                skipped_excluded += 1
                continue
            cat_name = entry["category"]
            sub_name = entry["subcategory"]
            synth_data = {
                "name": entry["name"],
                "count": 0,
                "volumes": "0",
                "source": entry.get("source", "hsg-tags"),
                "type": "topic",
            }
            categories.setdefault(cat_name, {}).setdefault(sub_name, []).append(
                (entry["ref"], synth_data)
            )
            hsg_count += 1
        if hsg_count or skipped_excluded:
            print(f"  Added {hsg_count} HSG-only subjects (skipped {skipped_excluded} excluded)")

    # Merge in promoted discovery candidates (accepted via candidates-review.html)
    if os.path.exists(PROMOTED_CANDIDATES_FILE):
        with open(PROMOTED_CANDIDATES_FILE) as f:
            promoted_candidates = json.load(f)
        existing_refs = {}  # ref -> (cat_name, sub_name, index, sdata)
        for cat_name_key, cat_subs in categories.items():
            for sub_name_key, subjects in cat_subs.items():
                for idx, (ref, sdata) in enumerate(subjects):
                    existing_refs[ref] = (cat_name_key, sub_name_key, idx, sdata)
        promoted_count = 0
        skipped_merged = 0
        for entry in promoted_candidates:
            # Skip candidates that were merged into another subject
            if entry["ref"] in decisions.merge_map:
                skipped_merged += 1
                continue
            if entry["ref"] in existing_refs:
                # Augment existing subjects that have count=0 with promoted data
                cat_k, sub_k, idx, sdata = existing_refs[entry["ref"]]
                if int(sdata.get("count", 0)) == 0:
                    pc_doc_count = entry.get("doc_count", 0)
                    pc_vol_count = len(entry.get("volumes", []))
                    if entry["ref"] in doc_apps:
                        apps = doc_apps[entry["ref"]]
                        pc_doc_count = sum(len(docs) for docs in apps.values())
                        pc_vol_count = len(apps)
                    if pc_doc_count > 0:
                        sdata["count"] = pc_doc_count
                        sdata["volumes"] = str(pc_vol_count)
                    if not sdata.get("document_appearances"):
                        if entry["ref"] in doc_apps:
                            sdata["document_appearances"] = doc_apps[entry["ref"]]
                        elif entry.get("volume_docs"):
                            sdata["document_appearances"] = entry["volume_docs"]
                        elif entry.get("volumes"):
                            sdata["document_appearances"] = {
                                vol: [] for vol in entry["volumes"]
                            }
                continue
            cat_name = entry.get("category", "Uncategorized")
            sub_name = entry.get("subcategory", "General")
            # Use volume/doc_count data from promoted_candidates.json,
            # and also check document_appearances.json for string-match data
            pc_doc_count = entry.get("doc_count", 0)
            pc_vol_count = len(entry.get("volumes", []))
            # Prefer document_appearances data if available (more accurate)
            if entry["ref"] in doc_apps:
                apps = doc_apps[entry["ref"]]
                pc_doc_count = sum(len(docs) for docs in apps.values())
                pc_vol_count = len(apps)
            synth_data = {
                "name": entry["name"],
                "count": pc_doc_count,
                "volumes": str(pc_vol_count),
                "source": "discovery",
                "type": "topic",
            }
            # Inject per-volume doc refs as document_appearances so
            # the generate() function can build per-volume doc lists
            if entry["ref"] not in doc_apps:
                if entry.get("volume_docs"):
                    synth_data["document_appearances"] = entry["volume_docs"]
                elif entry.get("volumes"):
                    synth_data["document_appearances"] = {
                        vol: [] for vol in entry["volumes"]
                    }
            if entry.get("lcsh_uri"):
                synth_data["lcsh_uri"] = entry["lcsh_uri"]
            categories.setdefault(cat_name, {}).setdefault(sub_name, []).append(
                (entry["ref"], synth_data)
            )
            promoted_count += 1
        if promoted_count or skipped_merged:
            print(f"  Added {promoted_count} promoted candidates (skipped {skipped_merged} merged sources)")

    # Apply taxonomy review merges and candidate merges to categories
    print("\nApplying merges to categories...")
    categories = apply_merges_to_categories(categories, decisions)

    print("\nGenerating mockup data...")
    sidebar_data, subject_data = generate(categories, uncategorized, doc_apps, doc_meta, ref_to_name)

    # Write output files
    with open("../mockup_sidebar_data.json", "w") as f:
        json.dump(sidebar_data, f, separators=(",", ":"))
    with open("../mockup_subject_data.json", "w") as f:
        json.dump(subject_data, f, separators=(",", ":"))

    print(f"\nWrote mockup_sidebar_data.json ({os.path.getsize('../mockup_sidebar_data.json') / 1024:.0f} KB)")
    print(f"Wrote mockup_subject_data.json ({os.path.getsize('../mockup_subject_data.json') / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
