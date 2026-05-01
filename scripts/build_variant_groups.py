#!/usr/bin/env python3
"""
Build variant groups from dedup decisions and LCSH URI overlaps.

Reads dedup_decisions.json, semantic_dedup_decisions.json, and lcsh_mapping.json
to produce variant_groups.json — a mapping of variant term refs to canonical refs
with all search name forms.

Sources processed in priority order:
  1. dedup_decisions.json (human-reviewed exact dedup)
  2. semantic_dedup_decisions.json (human-reviewed semantic dedup)
  3. lcsh_mapping.json URI overlaps (automated LCSH-based grouping)

Manual overrides from variant_overrides.json can split or merge groups.
"""

import json
import os
import sys
from datetime import datetime
from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

DEDUP_FILE = "../config/dedup_decisions.json"
SEMANTIC_DEDUP_FILE = "../config/semantic_dedup_decisions.json"
LCSH_MAPPING_FILE = "../config/lcsh_mapping.json"
TAXONOMY_FILE = "../subject-taxonomy-lcsh.xml"
OVERRIDES_FILE = "../config/variant_overrides.json"
HSG_VARIANTS_FILE = "../config/hsg_variant_names.json"
OUTPUT_FILE = "../variant_groups.json"


def load_taxonomy_refs(path):
    """Load ref -> name mapping from taxonomy XML.

    Returns: {ref: name} for all active subjects (those with <name> child elements).
    """
    tree = etree.parse(path)
    refs = {}
    for subj in tree.getroot().iter("subject"):
        ref = subj.get("ref", "")
        name_el = subj.find("name")
        if ref and name_el is not None and name_el.text:
            refs[ref] = name_el.text.strip()
    return refs


def load_all_taxonomy_names(path):
    """Load ref -> name mapping for ALL subjects in XML, including rejected.

    Active subjects have <name> child elements; rejected ones store the name
    in a 'name' attribute. This broader lookup is used for manual merge
    overrides so rejected terms can be folded into active terms as variants.

    Returns: {ref: name} for all subjects.
    """
    tree = etree.parse(path)
    names = {}
    for subj in tree.getroot().iter("subject"):
        ref = subj.get("ref", "")
        if not ref:
            continue
        # Try child element first (active terms)
        name_el = subj.find("name")
        if name_el is not None and name_el.text:
            names[ref] = name_el.text.strip()
        # Fall back to name attribute (rejected terms)
        elif subj.get("name"):
            names[ref] = subj.get("name").strip()
    return names


def load_dedup_groups(path):
    """Load merge groups from dedup_decisions.json.

    Returns: list of {primary_ref, primary_name, all_refs, all_names}
    """
    if not os.path.exists(path):
        return []
    with open(path) as f:
        data = json.load(f)
    return data.get("merge", [])


def load_lcsh_uri_overlaps(mapping_path, taxonomy_refs):
    """Find groups of taxonomy refs sharing the same LCSH URI.

    Only considers refs currently in the taxonomy. Picks canonical
    by highest count, then alphabetically by name.

    Returns: list of {canonical_ref, canonical_name, lcsh_uri, all_refs, all_names}
    """
    if not os.path.exists(mapping_path):
        return []
    with open(mapping_path) as f:
        mapping = json.load(f)

    # Group by LCSH URI
    uri_groups = {}
    for ref, data in mapping.items():
        if ref not in taxonomy_refs:
            continue
        uri = data.get("lcsh_uri")
        if not uri:
            continue
        uri_groups.setdefault(uri, []).append({
            "ref": ref,
            "name": data.get("name", ""),
            "count": data.get("count", 0),
        })

    # Keep groups with 2+ members
    result = []
    for uri, members in uri_groups.items():
        if len(members) < 2:
            continue
        # Pick canonical by highest count, then alphabetically
        members.sort(key=lambda m: (-m["count"], m["name"].lower()))
        canonical = members[0]
        result.append({
            "canonical_ref": canonical["ref"],
            "canonical_name": canonical["name"],
            "lcsh_uri": uri,
            "all_refs": [m["ref"] for m in members],
            "all_names": [m["name"] for m in members],
        })

    return result


def load_overrides(path):
    """Load manual variant overrides.

    Returns: (splits: set of frozenset pairs to prevent grouping,
              merges: list of merge dicts,
              search_names: list of add_search_name dicts)
    """
    if not os.path.exists(path):
        return set(), [], []

    with open(path) as f:
        data = json.load(f)

    splits = set()
    merges = []
    search_names = []
    for override in data.get("overrides", []):
        action = override.get("action")
        if action == "split":
            refs = override.get("refs", [])
            # Store all pairs that should not be grouped together
            for i in range(len(refs)):
                for j in range(i + 1, len(refs)):
                    splits.add(frozenset([refs[i], refs[j]]))
        elif action == "merge":
            merges.append(override)
        elif action == "add_search_name":
            search_names.append(override)

    return splits, merges, search_names


def build_variant_groups(taxonomy_refs, all_names=None):
    """Build variant groups from all sources.

    Args:
        taxonomy_refs: {ref: name} for active subjects only
        all_names: {ref: name} for ALL subjects including rejected (used for merges)

    Returns: {groups: [...], ref_to_canonical: {...}}
    """
    if all_names is None:
        all_names = taxonomy_refs
    # Load sources
    dedup_groups = load_dedup_groups(DEDUP_FILE)
    semantic_groups = load_dedup_groups(SEMANTIC_DEDUP_FILE)
    lcsh_groups = load_lcsh_uri_overlaps(LCSH_MAPPING_FILE, taxonomy_refs)
    splits, manual_merges, search_name_overrides = load_overrides(OVERRIDES_FILE)

    assigned_refs = set()  # Refs already in a group
    groups = []
    stats = {"dedup": 0, "semantic_dedup": 0, "lcsh_uri": 0, "manual_merge": 0}

    # 1. Process dedup decisions (highest priority)
    for dg in dedup_groups:
        primary_ref = dg["primary_ref"]
        primary_name = dg["primary_name"]
        all_refs = dg["all_refs"]
        group_names = dg["all_names"]

        # Build search_names with in_taxonomy flag
        search_names = []
        variant_refs = []
        for ref, name in zip(all_refs, group_names):
            in_tax = ref in taxonomy_refs
            search_names.append({
                "name": name,
                "ref": ref,
                "in_taxonomy": in_tax,
            })
            if ref != primary_ref:
                variant_refs.append(ref)

        # Only create group if there are actual variant name forms
        # (different names, not just duplicate entries with same name)
        unique_names = set(n.lower() for n in group_names)
        if len(unique_names) < 2 and len(all_refs) < 2:
            continue

        groups.append({
            "canonical_ref": primary_ref,
            "canonical_name": primary_name,
            "source": "dedup",
            "variant_refs": variant_refs,
            "search_names": search_names,
        })
        assigned_refs.update(all_refs)
        stats["dedup"] += 1

    # 2. Process semantic dedup decisions
    for sg in semantic_groups:
        primary_ref = sg["primary_ref"]
        primary_name = sg["primary_name"]
        all_refs = sg["all_refs"]
        group_names = sg["all_names"]

        # Skip refs already assigned
        if any(ref in assigned_refs for ref in all_refs):
            # Only include unassigned refs
            new_refs = [r for r in all_refs if r not in assigned_refs]
            if len(new_refs) < 1:
                continue
            # If the primary is already assigned, skip entirely
            if primary_ref in assigned_refs:
                continue

        search_names = []
        variant_refs = []
        for ref, name in zip(all_refs, group_names):
            if ref in assigned_refs:
                continue
            in_tax = ref in taxonomy_refs
            search_names.append({
                "name": name,
                "ref": ref,
                "in_taxonomy": in_tax,
            })
            if ref != primary_ref:
                variant_refs.append(ref)

        if not search_names:
            continue

        unique_names = set(n.lower() for n in [sn["name"] for sn in search_names])
        if len(unique_names) < 2 and len(search_names) < 2:
            continue

        groups.append({
            "canonical_ref": primary_ref,
            "canonical_name": primary_name,
            "source": "semantic_dedup",
            "variant_refs": variant_refs,
            "search_names": search_names,
        })
        assigned_refs.update(r for r in all_refs if r not in assigned_refs)
        stats["semantic_dedup"] += 1

    # 3. Process LCSH URI overlap groups (lowest auto priority)
    for lg in lcsh_groups:
        canonical_ref = lg["canonical_ref"]
        all_refs = lg["all_refs"]

        # Filter out already-assigned refs
        available_refs = [r for r in all_refs if r not in assigned_refs]
        if len(available_refs) < 2:
            continue

        # Check split overrides — remove refs that should be split from canonical
        filtered_refs = [canonical_ref] if canonical_ref in available_refs else []
        for ref in available_refs:
            if ref == canonical_ref:
                continue
            # Check if this pair is in the split list
            if canonical_ref in available_refs and frozenset([canonical_ref, ref]) in splits:
                continue
            filtered_refs.append(ref)

        if len(filtered_refs) < 2:
            continue

        # Re-select canonical from filtered (might have changed)
        if canonical_ref not in filtered_refs:
            canonical_ref = filtered_refs[0]

        canonical_name = taxonomy_refs.get(canonical_ref, lg["canonical_name"])
        search_names = []
        variant_refs = []
        for ref in filtered_refs:
            name = taxonomy_refs.get(ref, "")
            search_names.append({
                "name": name,
                "ref": ref,
                "in_taxonomy": True,
            })
            if ref != canonical_ref:
                variant_refs.append(ref)

        groups.append({
            "canonical_ref": canonical_ref,
            "canonical_name": canonical_name,
            "source": "lcsh_uri",
            "lcsh_uri": lg["lcsh_uri"],
            "variant_refs": variant_refs,
            "search_names": search_names,
        })
        assigned_refs.update(filtered_refs)
        stats["lcsh_uri"] += 1

    # 4. Process manual merge overrides
    # Uses all_names (includes rejected terms) so folded-in variants get their names
    for mo in manual_merges:
        canonical_ref = mo.get("canonical_ref")
        variant_refs = mo.get("variant_refs", [])
        canonical_name = mo.get("canonical_name", all_names.get(canonical_ref, ""))

        all_refs = [canonical_ref] + variant_refs
        # Remove any from existing groups first
        for g in groups:
            g["variant_refs"] = [r for r in g["variant_refs"] if r not in all_refs]
            g["search_names"] = [sn for sn in g["search_names"] if sn["ref"] not in variant_refs or sn["ref"] == g["canonical_ref"]]

        search_names = []
        for ref in all_refs:
            name = all_names.get(ref, "")
            search_names.append({
                "name": name,
                "ref": ref,
                "in_taxonomy": ref in taxonomy_refs,
            })

        groups.append({
            "canonical_ref": canonical_ref,
            "canonical_name": canonical_name,
            "source": "manual",
            "variant_refs": variant_refs,
            "search_names": search_names,
        })
        assigned_refs.update(all_refs)
        stats["manual_merge"] += 1

    # 5. Process add_search_name overrides (from candidate review merges)
    # These add discovered index/LCSH terms as search name variants of existing subjects
    canonical_group_idx = {}  # canonical_ref -> index in groups[]
    for i, g in enumerate(groups):
        canonical_group_idx[g["canonical_ref"]] = i

    search_name_added = 0
    search_name_new_groups = 0
    for sn_override in search_name_overrides:
        canonical_ref = sn_override.get("canonical_ref", "")
        search_name = sn_override.get("search_name", "")
        if not canonical_ref or not search_name:
            continue

        # Optional matcher flags propagated from override
        case_sensitive = bool(sn_override.get("case_sensitive", False))
        boundary = sn_override.get("boundary")  # e.g. "whitespace"; default uses \w boundaries

        if canonical_ref in canonical_group_idx:
            # Add search name to existing group
            g = groups[canonical_group_idx[canonical_ref]]
            existing_names = {sn["name"].lower() for sn in g["search_names"]}
            if search_name.lower() not in existing_names:
                entry = {
                    "name": search_name,
                    "ref": canonical_ref,
                    "in_taxonomy": False,
                    "source": sn_override.get("source", "candidates-review"),
                }
                if case_sensitive:
                    entry["case_sensitive"] = True
                if boundary:
                    entry["boundary"] = boundary
                g["search_names"].append(entry)
                search_name_added += 1
        else:
            # Create a new group for this canonical ref
            canonical_name = all_names.get(canonical_ref, taxonomy_refs.get(canonical_ref, ""))
            if not canonical_name:
                continue
            sn_entry = {
                "name": search_name,
                "ref": canonical_ref,
                "in_taxonomy": False,
                "source": sn_override.get("source", "candidates-review"),
            }
            if case_sensitive:
                sn_entry["case_sensitive"] = True
            if boundary:
                sn_entry["boundary"] = boundary
            new_group = {
                "canonical_ref": canonical_ref,
                "canonical_name": canonical_name,
                "source": "candidates-review",
                "variant_refs": [],
                "search_names": [
                    {
                        "name": canonical_name,
                        "ref": canonical_ref,
                        "in_taxonomy": canonical_ref in taxonomy_refs,
                    },
                    sn_entry,
                ],
            }
            groups.append(new_group)
            canonical_group_idx[canonical_ref] = len(groups) - 1
            search_name_new_groups += 1
            search_name_added += 1

    if search_name_added:
        stats["search_name"] = search_name_added
        stats["search_name_new_groups"] = search_name_new_groups

    # Build reverse mapping
    ref_to_canonical = {}
    for g in groups:
        for vref in g["variant_refs"]:
            ref_to_canonical[vref] = g["canonical_ref"]
        # Also add search_names refs that aren't the canonical
        for sn in g["search_names"]:
            if sn["ref"] != g["canonical_ref"]:
                ref_to_canonical[sn["ref"]] = g["canonical_ref"]

    return {
        "generated": datetime.now().isoformat(),
        "sources": stats,
        "total_groups": len(groups),
        "total_variant_refs": len(ref_to_canonical),
        "groups": groups,
        "ref_to_canonical": ref_to_canonical,
    }, stats


def main():
    print("Building variant groups...")
    print()

    # Load taxonomy refs (active only — used for LCSH URI grouping)
    if not os.path.exists(TAXONOMY_FILE):
        print(f"ERROR: Taxonomy file not found: {TAXONOMY_FILE}")
        sys.exit(1)

    try:
        taxonomy_refs = load_taxonomy_refs(TAXONOMY_FILE)
    except Exception as e:
        print(f"ERROR: Failed to parse taxonomy file: {e}")
        sys.exit(1)

    print(f"Taxonomy: {len(taxonomy_refs)} active subjects")

    # Load all names including rejected (used for manual merge overrides)
    all_names = load_all_taxonomy_names(TAXONOMY_FILE)
    rejected_count = len(all_names) - len(taxonomy_refs)
    if rejected_count > 0:
        print(f"  ({rejected_count} rejected terms available for fold-in merges)")

    # Build groups
    result, stats = build_variant_groups(taxonomy_refs, all_names)

    print(f"\nGroups by source:")
    print(f"  Dedup decisions:          {stats['dedup']}")
    print(f"  Semantic dedup decisions:  {stats['semantic_dedup']}")
    print(f"  LCSH URI overlaps:         {stats['lcsh_uri']}")
    print(f"  Manual merges:             {stats['manual_merge']}")
    if stats.get('search_name'):
        print(f"  Candidate search names:    {stats['search_name']} ({stats.get('search_name_new_groups', 0)} new groups)")
    print(f"  Total groups:              {result['total_groups']}")
    print(f"  Total variant refs mapped: {result['total_variant_refs']}")

    # Show some examples
    print(f"\nSample groups:")
    for g in result["groups"][:10]:
        names = [sn["name"] for sn in g["search_names"]]
        tax_flags = ["*" if not sn["in_taxonomy"] else "" for sn in g["search_names"]]
        name_strs = [f"{n}{f}" for n, f in zip(names, tax_flags)]
        print(f"  [{g['source']}] {g['canonical_name']}: {' / '.join(name_strs)}")

    # 5. Append HSG-only variant groups
    if os.path.exists(HSG_VARIANTS_FILE):
        with open(HSG_VARIANTS_FILE) as f:
            hsg_data = json.load(f)
        hsg_variants = hsg_data.get("variants", {})
        existing_canonical_refs = {g["canonical_ref"] for g in result["groups"]}
        hsg_count = 0
        for ref, entry in hsg_variants.items():
            if ref in existing_canonical_refs:
                continue
            search_names = []
            seen = set()
            for sn in entry.get("search_names", []):
                if sn.lower() in seen:
                    continue
                seen.add(sn.lower())
                # First search name with canonical name is the taxonomy entry
                is_canonical = (sn == entry["name"])
                search_names.append({
                    "name": sn,
                    "ref": ref,
                    "in_taxonomy": is_canonical,
                })
            if search_names:
                result["groups"].append({
                    "canonical_ref": ref,
                    "canonical_name": entry["name"],
                    "source": "hsg-tags",
                    "variant_refs": [],
                    "search_names": search_names,
                })
                hsg_count += 1
        result["total_groups"] = len(result["groups"])
        if hsg_count:
            print(f"\n  HSG-only variant groups:   {hsg_count}")
            print(f"  Updated total groups:      {result['total_groups']}")

    # Write output
    try:
        with open(OUTPUT_FILE, "w") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"\nWrote {OUTPUT_FILE}")
    except OSError as e:
        print(f"ERROR: Failed to write {OUTPUT_FILE}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
