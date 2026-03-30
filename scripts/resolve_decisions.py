#!/usr/bin/env python3
"""
resolve_decisions.py — Shared module for loading and applying review decisions.

All scripts that need to resolve merges, exclusions, rejections, or LCSH decisions
import from this module instead of reimplementing the logic.

Usage:
    from resolve_decisions import load_all_decisions, resolve_merge_chain, merge_appearances
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ResolvedDecisions:
    """Unified view of all review decisions across the pipeline."""

    # Global merge map: source_ref -> final_target_ref (chains already resolved)
    merge_map: dict = field(default_factory=dict)

    # Refs excluded from taxonomy entirely
    exclusions: set = field(default_factory=set)

    # Refs globally rejected
    global_rejections: set = field(default_factory=set)

    # LCSH decisions: ref -> "accepted"|"rejected"
    lcsh_decisions: dict = field(default_factory=dict)

    # Candidate decisions by category: state_key -> {candidate_id -> decision_dict}
    candidate_merges: dict = field(default_factory=dict)

    # Per-volume rejections: volume_id -> set of "docId:ref:position" keys
    vol_rejections: dict = field(default_factory=dict)

    # Per-volume merge decisions: volume_id -> list of {source_ref, target_ref, ...}
    vol_merges: dict = field(default_factory=dict)

    # Per-volume LCSH decisions: volume_id -> {ref -> decision}
    vol_lcsh_decisions: dict = field(default_factory=dict)

    # Raw dedup groups for apply_dedup_to_mapping
    dedup_groups: list = field(default_factory=list)

    # Candidate data by category for count transfers: state_key -> {id -> candidate}
    candidate_data: dict = field(default_factory=dict)


# ── Core resolution functions ────────────────────────────────────────


def resolve_merge_chain(ref, merge_map):
    """Follow a ref through the merge map to its final target.

    Handles chains (A→B→C becomes A→C) and cycles (via seen set).
    """
    seen = set()
    current = ref
    while current in merge_map and current not in seen:
        seen.add(current)
        current = merge_map[current]
    return current


def is_excluded(ref, decisions):
    """Check if a ref is excluded from the taxonomy."""
    return ref in decisions.exclusions or ref in decisions.global_rejections


def is_rejected(doc_id, ref, position, decisions, volume_id):
    """Check if a specific match was rejected in per-volume review."""
    key = f"{doc_id}:{ref}:{position}"
    rejections = decisions.vol_rejections.get(volume_id, set())
    return key in rejections


def get_lcsh_decision(ref, decisions, volume_id=None):
    """Get the effective LCSH decision for a ref.

    Global decisions take precedence over volume-level decisions.
    Returns "accepted", "rejected", or None.
    """
    if ref in decisions.lcsh_decisions:
        return decisions.lcsh_decisions[ref]
    if volume_id:
        vol_lcsh = decisions.vol_lcsh_decisions.get(volume_id, {})
        if ref in vol_lcsh:
            return vol_lcsh[ref]
    return None


# ── Appearance and count transfer ────────────────────────────────────


def merge_appearances(target_apps, source_apps):
    """Merge source document appearances into target (union semantics).

    Both arguments are dicts of {volume_id: [doc_id, ...]}.
    Returns the updated target_apps dict.
    """
    for vol, docs in source_apps.items():
        existing = set(target_apps.get(vol, []))
        existing.update(docs)
        target_apps[vol] = sorted(existing)
    return target_apps


def merge_appears_in(target_str, source_str):
    """Merge two comma-separated appears_in strings."""
    target_set = set(v.strip() for v in (target_str or "").split(",") if v.strip())
    source_set = set(v.strip() for v in (source_str or "").split(",") if v.strip())
    combined = sorted(target_set | source_set)
    return ", ".join(combined) if combined else ""


def transfer_counts(target_data, source_data):
    """Transfer counts, volumes, appearances, and appears_in from source to target.

    Modifies target_data in place.
    """
    # Count
    target_data["count"] = int(target_data.get("count", 0) or 0) + int(source_data.get("count", 0) or 0)

    # Volume count
    target_data["volumes"] = str(
        int(target_data.get("volumes", 0) or 0) + int(source_data.get("volumes", 0) or 0)
    )

    # Document appearances
    source_apps = source_data.get("document_appearances", {})
    target_apps = target_data.get("document_appearances", {})
    target_data["document_appearances"] = merge_appearances(target_apps, source_apps)

    # Appears_in
    target_data["appears_in"] = merge_appears_in(
        target_data.get("appears_in", ""),
        source_data.get("appears_in", ""),
    )


def transfer_candidate_counts(target_data, candidate):
    """Transfer counts from a discovery candidate to a taxonomy target.

    Candidates use different field names (doc_count, volume_count, volume_docs).
    """
    target_data["count"] = int(target_data.get("count", 0) or 0) + int(candidate.get("doc_count", 0))

    source_vols = int(candidate.get("volume_count", len(candidate.get("volumes", []))))
    target_data["volumes"] = str(int(target_data.get("volumes", 0) or 0) + source_vols)

    vol_docs = candidate.get("volume_docs", {})
    if vol_docs:
        target_apps = target_data.get("document_appearances", {})
        target_data["document_appearances"] = merge_appearances(target_apps, vol_docs)


# ── Loading decisions ────────────────────────────────────────────────


def _load_json(path):
    """Load a JSON file, returning empty dict/list if missing."""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def _build_global_merge_map(taxonomy_state, dedup_groups, variant_overrides):
    """Build a unified merge map from all merge sources, then resolve chains."""
    raw_map = {}

    # Dedup merge groups: secondary refs → primary ref
    for group in dedup_groups:
        primary = group.get("primary_ref", "")
        for ref in group.get("all_refs", []):
            if ref != primary and primary:
                raw_map[ref] = primary

    # Taxonomy review merge decisions: source_ref → targetRef
    for source_ref, decision in taxonomy_state.get("merge_decisions", {}).items():
        target_ref = decision.get("targetRef", "")
        if target_ref:
            raw_map[source_ref] = target_ref

    # Variant overrides with action "merge": variant_refs → canonical_ref
    overrides = variant_overrides.get("overrides", []) if isinstance(variant_overrides, dict) else []
    for entry in overrides:
        if entry.get("action") == "merge":
            canonical = entry.get("canonical_ref", "")
            for vref in entry.get("variant_refs", []):
                if vref != canonical and canonical:
                    raw_map[vref] = canonical

    # Resolve chains
    resolved = {}
    for source_ref in raw_map:
        resolved[source_ref] = resolve_merge_chain(source_ref, raw_map)

    return resolved


def _load_candidate_data(repo_root):
    """Load candidate data from review files for count transfers."""
    candidate_data = {}
    files = {
        "candidate_decisions_topics": repo_root / "data" / "review_topics.json",
        "candidate_decisions_persons": repo_root / "data" / "review_persons.json",
        "candidate_decisions_organizations": repo_root / "data" / "review_organizations.json",
    }
    for state_key, path in files.items():
        if path.exists():
            with open(path) as f:
                cdata = json.load(f)
            candidates_list = cdata.get("candidates", cdata) if isinstance(cdata, dict) else cdata
            candidate_data[state_key] = {c["id"]: c for c in candidates_list}
    return candidate_data


def load_all_decisions(repo_root, volume_id=None):
    """Load all decision files and return a unified ResolvedDecisions.

    Args:
        repo_root: Path to the repository root
        volume_id: Optional volume ID to load per-volume decisions
    """
    repo_root = Path(repo_root)
    config_dir = repo_root / "config"

    # Load taxonomy review state
    taxonomy_state = _load_json(repo_root / "taxonomy_review_state.json")

    # Load dedup decisions
    dedup_data = _load_json(config_dir / "dedup_decisions.json")
    semantic_dedup = _load_json(config_dir / "semantic_dedup_decisions.json")
    dedup_groups = dedup_data.get("merge", []) + semantic_dedup.get("merge", [])

    # Load variant overrides
    variant_overrides = _load_json(config_dir / "variant_overrides.json")

    # Build global merge map
    merge_map = _build_global_merge_map(taxonomy_state, dedup_groups, variant_overrides)

    # Exclusions and global rejections
    exclusions = set(taxonomy_state.get("exclusions", {}).keys())
    global_rejections = set(taxonomy_state.get("global_rejections", {}).keys())

    # LCSH decisions
    lcsh_decisions = taxonomy_state.get("lcsh_decisions", {})

    # Candidate merge decisions
    candidate_merges = {}
    for key in ["candidate_decisions_topics", "candidate_decisions_persons",
                "candidate_decisions_organizations"]:
        decisions = taxonomy_state.get(key, {})
        if decisions:
            candidate_merges[key] = decisions

    # Load candidate data for count transfers
    candidate_data = _load_candidate_data(repo_root)

    # Per-volume decisions
    vol_rejections = {}
    vol_merges = {}
    vol_lcsh_decisions = {}

    if volume_id:
        # Check both locations for annotation_rejections
        for path in [
            config_dir / f"annotation_rejections_{volume_id}.json",
            repo_root / "data" / "documents" / volume_id / f"annotation_rejections_{volume_id}.json",
        ]:
            if path.exists():
                with open(path) as f:
                    data = json.load(f)

                rejections = set()
                for r in data.get("rejections", []):
                    key = r.get("key", "")
                    if key:
                        rejections.add(key)
                vol_rejections[volume_id] = rejections

                vol_merges[volume_id] = data.get("merge_decisions", [])

                vlcsh = {}
                for d in data.get("lcsh_decisions", []):
                    ref = d.get("ref", "")
                    if ref:
                        vlcsh[ref] = d.get("decision", "")
                vol_lcsh_decisions[volume_id] = vlcsh

                break

    return ResolvedDecisions(
        merge_map=merge_map,
        exclusions=exclusions,
        global_rejections=global_rejections,
        lcsh_decisions=lcsh_decisions,
        candidate_merges=candidate_merges,
        vol_rejections=vol_rejections,
        vol_merges=vol_merges,
        vol_lcsh_decisions=vol_lcsh_decisions,
        dedup_groups=dedup_groups,
        candidate_data=candidate_data,
    )


# ── Applying decisions to data structures ────────────────────────────


def apply_dedup_to_mapping(mapping, decisions):
    """Apply dedup merge groups to a mapping dict.

    For each merge group: sum counts, union appearances, keep best LCSH,
    mark secondary refs as merged_into.

    Moved from build_taxonomy_lcsh.py apply_dedup_decisions().
    """
    dedup_groups = decisions.dedup_groups
    if not dedup_groups:
        print("  No dedup merge decisions to apply")
        return mapping

    merged_count = 0

    for group in dedup_groups:
        primary_ref = group.get("primary_ref", "")
        all_refs = group.get("all_refs", [])
        secondary_refs = [r for r in all_refs if r != primary_ref]

        if primary_ref not in mapping:
            continue

        entries = [(ref, mapping[ref]) for ref in all_refs if ref in mapping]
        if len(entries) <= 1:
            continue

        primary_data = dict(mapping[primary_ref])
        primary_data["merged_refs"] = [r for r, _ in entries]
        primary_data["count"] = sum(int(d.get("count", 0)) for _, d in entries)

        # Merge appears_in
        all_vols = set()
        for _, d in entries:
            for v in (d.get("appears_in", "") or "").split(", "):
                v = v.strip()
                if v:
                    all_vols.add(v)
        primary_data["appears_in"] = ", ".join(sorted(all_vols))
        primary_data["volumes"] = len(all_vols)

        # Merge document appearances
        merged_docs = {}
        for _, d in entries:
            merge_appearances(merged_docs, d.get("document_appearances", {}))
        primary_data["document_appearances"] = merged_docs

        # Keep best LCSH match
        for _, d in entries:
            if d.get("match_quality") == "exact" and d.get("lcsh_uri"):
                primary_data["lcsh_uri"] = d["lcsh_uri"]
                primary_data["lcsh_label"] = d.get("lcsh_label", "")
                primary_data["match_quality"] = "exact"
                primary_data["exact_match"] = True
                break
            elif d.get("match_quality") == "good_close" and d.get("lcsh_uri"):
                primary_data["lcsh_uri"] = d["lcsh_uri"]
                primary_data["lcsh_label"] = d.get("lcsh_label", "")
                primary_data["match_quality"] = "good_close"

        mapping[primary_ref] = primary_data

        for ref in secondary_refs:
            if ref in mapping:
                original_name = mapping[ref].get("name", "")
                mapping[ref] = {
                    "status": "merged_into",
                    "canonical_ref": primary_ref,
                    "canonical_name": primary_data.get("name", ""),
                    "original_name": original_name,
                }
                merged_count += 1

    print(f"  Global dedup: merged {merged_count} entries into {len(dedup_groups)} primary entries")
    print(f"  Mapping now has {len(mapping)} subjects ({merged_count} marked as merged)")
    return mapping


def apply_merges_to_categories(categories, decisions):
    """Apply taxonomy review merges and candidate merges to a categories dict.

    categories: {cat_name: {sub_name: [(ref, data), ...]}}

    Transfers counts and appearances from merge sources to targets,
    then removes source entries from the categories structure.
    """
    # Build ref → (cat, sub, idx, data) lookup
    ref_lookup = {}
    for cat_name, cat_subs in categories.items():
        for sub_name, subjects in cat_subs.items():
            for idx, (ref, sdata) in enumerate(subjects):
                ref_lookup[ref] = (cat_name, sub_name, idx, sdata)

    # Apply taxonomy review merges (ref → ref)
    remove_refs = set()
    transferred = 0
    for source_ref, target_ref in decisions.merge_map.items():
        if source_ref not in ref_lookup or target_ref not in ref_lookup:
            continue
        _, _, _, source_data = ref_lookup[source_ref]
        _, _, _, target_data = ref_lookup[target_ref]
        transfer_counts(target_data, source_data)
        remove_refs.add(source_ref)
        transferred += 1

    if transferred:
        print(f"  Applied {transferred} taxonomy review merges (counts + appearances transferred)")

    # Apply candidate review merges (candidate → taxonomy ref)
    candidate_merge_count = 0
    for state_key, candidate_decisions in decisions.candidate_merges.items():
        candidates_by_id = decisions.candidate_data.get(state_key, {})
        if not candidates_by_id:
            continue

        for cid, decision in candidate_decisions.items():
            if decision.get("action") != "merged":
                continue
            target_ref = decision.get("mergeTarget", "")
            if not target_ref or target_ref not in ref_lookup:
                continue
            candidate = candidates_by_id.get(cid, {})
            if not candidate:
                continue

            _, _, _, target_data = ref_lookup[target_ref]
            transfer_candidate_counts(target_data, candidate)
            candidate_merge_count += 1

    if candidate_merge_count:
        print(f"  Applied {candidate_merge_count} candidate review merges (counts transferred)")

    # Remove merged source entries
    if remove_refs:
        for cat_name, cat_subs in categories.items():
            for sub_name in list(cat_subs.keys()):
                cat_subs[sub_name] = [(r, d) for r, d in cat_subs[sub_name] if r not in remove_refs]

    return categories
