#!/usr/bin/env python3
"""
Validate merge/split/variant decisions across all config sources.

Detects:
  - Cross-source merge conflicts (same ref → different targets)
  - Merge chains (A→B→C) — informational, not errors
  - Circular merge dependencies (A→B→…→A)
  - Split/merge contradictions (ref pair appears in both)
  - Duplicate search names assigned to different canonical refs
  - Orphaned merge targets (merging into a ref that is itself merged)

Decision sources checked:
  1. config/dedup_decisions.json         (dedup merge groups)
  2. config/semantic_dedup_decisions.json (semantic merge groups)
  3. config/variant_overrides.json       (manual merge/split/add_search_name)
  4. taxonomy_review_state.json          (merge_decisions from review UI)

Usage:
    python3 scripts/validate_decisions.py          # Run all checks
    python3 scripts/validate_decisions.py --quiet  # Errors and warnings only
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

os.chdir(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = Path("..").resolve()

# ANSI colors
RED = "\033[91m"
YELLOW = "\033[93m"
GREEN = "\033[92m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

QUIET = "--quiet" in sys.argv


def info(msg):
    if not QUIET:
        print(f"  {msg}")


def warn(msg):
    print(f"  {YELLOW}WARNING:{RESET} {msg}")


def error(msg):
    print(f"  {RED}ERROR:{RESET} {msg}")


def ok(msg):
    if not QUIET:
        print(f"  {GREEN}OK:{RESET} {msg}")


def section(msg):
    print(f"\n{BOLD}{msg}{RESET}")


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_json(path):
    """Load a JSON file, returning empty dict/list on missing file."""
    if not path.exists():
        warn(f"File not found: {path.name}")
        return {}
    with open(path) as f:
        return json.load(f)


def load_dedup_merges(path):
    """Extract ref→target mappings from dedup_decisions.json."""
    data = load_json(path)
    groups = data.get("merge", data) if isinstance(data, dict) else data
    if not isinstance(groups, list):
        groups = []
    merges = {}  # ref → (target, group_name)
    for group in groups:
        primary = group.get("primary_ref", "")
        primary_name = group.get("primary_name", "?")
        for ref in group.get("all_refs", []):
            if ref != primary and primary:
                merges[ref] = (primary, primary_name)
    return merges


def load_semantic_merges(path):
    """Extract ref→target mappings from semantic_dedup_decisions.json."""
    data = load_json(path)
    groups = data.get("merge", data) if isinstance(data, dict) else data
    if not isinstance(groups, list):
        groups = []
    merges = {}
    for group in groups:
        primary = group.get("primary_ref", "")
        primary_name = group.get("primary_name", "?")
        for ref in group.get("all_refs", []):
            if ref != primary and primary:
                merges[ref] = (primary, primary_name)
    return merges


def _get_override_entries(data):
    """Unwrap variant_overrides.json — may be a list or {overrides: [...]}."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("overrides", [])
    return []


def load_override_merges(path):
    """Extract merge actions from variant_overrides.json."""
    data = load_json(path)
    entries = _get_override_entries(data)
    merges = {}
    for entry in entries:
        if entry.get("action") == "merge":
            canonical = entry.get("canonical_ref", "")
            reason = entry.get("reason", "")
            for vref in entry.get("variant_refs", []):
                if vref != canonical and canonical:
                    merges[vref] = (canonical, reason)
    return merges


def load_override_splits(path):
    """Extract split actions from variant_overrides.json."""
    data = load_json(path)
    entries = _get_override_entries(data)
    splits = []
    for entry in entries:
        if entry.get("action") == "split":
            refs = entry.get("refs", [])
            reason = entry.get("reason", "")
            if len(refs) >= 2:
                splits.append((frozenset(refs), reason))
    return splits


def load_override_search_names(path):
    """Extract add_search_name actions from variant_overrides.json."""
    data = load_json(path)
    entries = _get_override_entries(data)
    names = []  # list of (search_name, canonical_ref, reason)
    for entry in entries:
        if entry.get("action") == "add_search_name":
            names.append((
                entry.get("search_name", ""),
                entry.get("canonical_ref", ""),
                entry.get("reason", ""),
            ))
    return names


def load_review_merges(path):
    """Extract merge_decisions from taxonomy_review_state.json."""
    data = load_json(path)
    decisions = data.get("merge_decisions", {})
    merges = {}
    for source_ref, decision in decisions.items():
        target = decision.get("targetRef", "")
        target_name = decision.get("targetName", "?")
        if target:
            merges[source_ref] = (target, target_name)
    return merges


def load_ref_names(lcsh_path):
    """Build ref→name lookup from lcsh_mapping.json for readable output."""
    names = {}
    data = load_json(lcsh_path)
    if isinstance(data, dict):
        for ref, entry in data.items():
            if isinstance(entry, dict) and entry.get("name"):
                names[ref] = entry["name"]
    return names


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_merge_conflicts(all_sources, ref_names):
    """Detect refs that are merged to different targets across sources."""
    section("1. Cross-source merge conflicts")
    conflicts = []

    # Build per-ref provenance: ref → [(source_name, target_ref, detail)]
    provenance = defaultdict(list)
    for source_name, merges in all_sources.items():
        for ref, (target, detail) in merges.items():
            provenance[ref].append((source_name, target, detail))

    for ref, entries in sorted(provenance.items()):
        targets = set(target for _, target, _ in entries)
        if len(targets) > 1:
            name = ref_names.get(ref, ref)
            error(f"'{name}' ({ref}) has conflicting merge targets:")
            for source_name, target, detail in entries:
                target_name = ref_names.get(target, target)
                print(f"      {CYAN}{source_name}{RESET}: → '{target_name}' ({target})")
                if detail:
                    print(f"        reason: {detail}")
            conflicts.append(ref)
        elif len(entries) > 1:
            # Same target from multiple sources — redundant but not conflicting
            target = entries[0][1]
            target_name = ref_names.get(target, target)
            name = ref_names.get(ref, ref)
            sources = ", ".join(s for s, _, _ in entries)
            info(f"'{name}' → '{target_name}' recorded in: {sources} (redundant)")

    if not conflicts:
        ok(f"No conflicts found across {sum(len(m) for m in all_sources.values())} merge entries")
    else:
        error(f"{len(conflicts)} ref(s) with conflicting merge targets")

    return conflicts


def check_chains_and_cycles(all_sources, ref_names):
    """Detect merge chains (A→B→C) and circular dependencies."""
    section("2. Merge chains and cycles")

    # Build unified raw map
    raw_map = {}
    for merges in all_sources.values():
        for ref, (target, _) in merges.items():
            raw_map[ref] = target

    chains = []
    cycles = []
    cycle_sets_seen = set()  # deduplicate cycles

    for start_ref in raw_map:
        path = [start_ref]
        seen = {start_ref}
        current = raw_map[start_ref]

        while current in raw_map:
            if current in seen:
                # Cycle detected — extract the minimal cycle
                if current in path:
                    cycle_start = path.index(current)
                    cycle_refs = path[cycle_start:]
                else:
                    cycle_refs = path
                cycle_key = frozenset(cycle_refs)
                if cycle_key not in cycle_sets_seen:
                    cycle_sets_seen.add(cycle_key)
                    cycles.append(cycle_refs + [current])
                break
            path.append(current)
            seen.add(current)
            current = raw_map[current]
        else:
            path.append(current)
            if len(path) > 2:
                chains.append(path)

    if cycles:
        for cycle in cycles:
            names = [ref_names.get(r, r) for r in cycle]
            error(f"Circular merge: {' → '.join(names)}")
            # Show which sources contribute to this cycle
            for i in range(len(cycle) - 1):
                src, tgt = cycle[i], cycle[i + 1]
                sources = []
                for source_name, merges in all_sources.items():
                    if src in merges and merges[src][0] == tgt:
                        sources.append(source_name)
                if sources:
                    src_name = ref_names.get(src, src)
                    tgt_name = ref_names.get(tgt, tgt)
                    print(f"      '{src_name}' → '{tgt_name}' in: {', '.join(sources)}")
    else:
        ok("No circular merges found")

    # Deduplicate chains (A→B→C and B→C are the same chain)
    unique_chains = []
    seen_endpoints = set()
    for chain in sorted(chains, key=len, reverse=True):
        if chain[0] not in seen_endpoints:
            unique_chains.append(chain)
        for ref in chain[:-1]:
            seen_endpoints.add(ref)

    if unique_chains:
        warn(f"{len(unique_chains)} merge chain(s) found (A→B→C resolved to A→C at build time):")
        for chain in unique_chains[:20]:
            names = [ref_names.get(r, r) for r in chain]
            print(f"      {' → '.join(names)}")
        if len(unique_chains) > 20:
            print(f"      ... and {len(unique_chains) - 20} more")
    else:
        ok("No merge chains found")

    return cycles, unique_chains


def check_split_merge_contradictions(override_splits, all_sources, ref_names):
    """Check for refs that appear in both a split and a merge together."""
    section("3. Split/merge contradictions")
    contradictions = []

    # Build set of ref pairs that are merged together
    merged_pairs = set()
    for merges in all_sources.values():
        for ref, (target, _) in merges.items():
            merged_pairs.add(frozenset([ref, target]))

    for split_refs, reason in override_splits:
        for pair in merged_pairs:
            if pair <= split_refs:
                refs_list = sorted(pair)
                names = [ref_names.get(r, r) for r in refs_list]
                error(
                    f"Refs are split AND merged: '{names[0]}' / '{names[1]}'"
                )
                print(f"      Split reason: {reason}")
                contradictions.append(pair)

    if not contradictions:
        ok(f"No contradictions between {len(override_splits)} split(s) and merges")

    return contradictions


def check_duplicate_search_names(search_names, ref_names):
    """Check for the same search name assigned to different canonical refs."""
    section("4. Duplicate search name assignments")
    dupes = []

    # Group by search_name
    by_name = defaultdict(list)
    for name, canonical, reason in search_names:
        if name:
            by_name[name].append((canonical, reason))

    for name, entries in sorted(by_name.items()):
        refs = set(canonical for canonical, _ in entries)
        if len(refs) > 1:
            warn(f"Search name '{name}' assigned to {len(refs)} different subjects:")
            for canonical, reason in entries:
                canonical_name = ref_names.get(canonical, canonical)
                print(f"      → '{canonical_name}' ({canonical})")
            dupes.append(name)
        elif len(entries) > 1:
            # Same name, same ref — just redundant
            canonical = entries[0][0]
            canonical_name = ref_names.get(canonical, canonical)
            info(f"Search name '{name}' → '{canonical_name}' recorded {len(entries)}x (redundant)")

    if not dupes:
        ok(f"No conflicting search name assignments across {len(search_names)} entries")
    else:
        warn(f"{len(dupes)} search name(s) with conflicting assignments")

    return dupes


def check_orphaned_targets(all_sources, ref_names):
    """Check for merge targets that are themselves merged elsewhere."""
    section("5. Orphaned merge targets")
    orphaned = []

    # All refs being merged away
    all_merged_refs = set()
    for merges in all_sources.values():
        all_merged_refs.update(merges.keys())

    # All target refs
    all_targets = set()
    for merges in all_sources.values():
        for _, (target, _) in merges.items():
            all_targets.add(target)

    # Targets that are themselves merged away
    for target in sorted(all_targets):
        if target in all_merged_refs:
            # Find what it's merged into
            for source_name, merges in all_sources.items():
                if target in merges:
                    final_target, detail = merges[target]
                    target_name = ref_names.get(target, target)
                    final_name = ref_names.get(final_target, final_target)
                    # This is a chain — already reported in check 2, but note source
                    info(
                        f"Target '{target_name}' is itself merged → "
                        f"'{final_name}' (via {source_name})"
                    )
                    orphaned.append(target)
                    break

    if orphaned:
        info(f"{len(orphaned)} merge target(s) are themselves merged (creates chains)")
    else:
        ok("All merge targets are stable (not themselves merged)")

    return orphaned


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print(f"{BOLD}Validating merge/split/variant decisions...{RESET}")

    # Load all sources
    dedup = load_dedup_merges(BASE_DIR / "config" / "dedup_decisions.json")
    semantic = load_dedup_merges(BASE_DIR / "config" / "semantic_dedup_decisions.json")
    override_merges = load_override_merges(BASE_DIR / "config" / "variant_overrides.json")
    review_merges = load_review_merges(BASE_DIR / "taxonomy_review_state.json")
    override_splits = load_override_splits(BASE_DIR / "config" / "variant_overrides.json")
    search_names = load_override_search_names(BASE_DIR / "config" / "variant_overrides.json")

    ref_names = load_ref_names(BASE_DIR / "config" / "lcsh_mapping.json")

    info(
        f"Loaded: {len(dedup)} dedup, {len(semantic)} semantic, "
        f"{len(override_merges)} override merges, {len(review_merges)} review merges, "
        f"{len(override_splits)} splits, {len(search_names)} search names"
    )

    all_sources = {
        "dedup_decisions": dedup,
        "semantic_dedup": semantic,
        "variant_overrides": override_merges,
        "taxonomy_review_state": review_merges,
    }

    # Run checks
    errors_found = 0

    conflicts = check_merge_conflicts(all_sources, ref_names)
    errors_found += len(conflicts)

    cycles, chains = check_chains_and_cycles(all_sources, ref_names)
    errors_found += len(cycles)

    contradictions = check_split_merge_contradictions(
        override_splits, all_sources, ref_names
    )
    errors_found += len(contradictions)

    dupes = check_duplicate_search_names(search_names, ref_names)

    orphaned = check_orphaned_targets(all_sources, ref_names)

    # Summary
    print(f"\n{'=' * 60}")
    if errors_found == 0 and not dupes:
        print(f"{GREEN}{BOLD}All decision checks passed!{RESET}")
    else:
        parts = []
        if conflicts:
            parts.append(f"{len(conflicts)} merge conflict(s)")
        if cycles:
            parts.append(f"{len(cycles)} circular merge(s)")
        if contradictions:
            parts.append(f"{len(contradictions)} split/merge contradiction(s)")
        if dupes:
            parts.append(f"{len(dupes)} search name conflict(s)")
        print(f"{BOLD}Decision issues: {', '.join(parts)}{RESET}")

    return 1 if errors_found > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
