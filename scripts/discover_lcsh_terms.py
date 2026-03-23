#!/usr/bin/env python3
"""
Tier 3 — LCSH-seeded discovery for term discovery.

Two modes:
  --offline  Uses only data already in lcsh_mapping.json (no API calls).
             Mines the 'all_suggestions' field for LCSH-related terms that
             appear near existing taxonomy terms but aren't in the taxonomy yet.
             Ranks by semantic proximity (shared broader heading clusters).

  (default)  Full LCSH expansion: fetches narrower terms from id.loc.gov
             for the broader headings that cluster existing taxonomy terms.
             Requires network access to id.loc.gov.

Output:
  - data/lcsh_candidates.json  — full candidate list with LCSH provenance
  - data/lcsh_candidates.txt   — human-readable review list

Usage:
  python3 scripts/discover_lcsh_terms.py [--offline] [--mapping FILE]
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from collections import defaultdict
from datetime import datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

LCSH_MAPPING_FILE = "../config/lcsh_mapping.json"
TAXONOMY_FILE = "../subject-taxonomy-lcsh.xml"
OUTPUT_DIR = "../data"
CACHE_FILE = "../data/lcsh_cache.json"

SKOS_URL_TEMPLATE = "https://id.loc.gov/authorities/subjects/{}.skos.json"
SKOS_BROADER = "http://www.w3.org/2004/02/skos/core#broader"
SKOS_NARROWER = "http://www.w3.org/2004/02/skos/core#narrower"
SKOS_PREFLABEL = "http://www.w3.org/2004/02/skos/core#prefLabel"
REQUEST_DELAY = 0.35


# ── Taxonomy loading ──────────────────────────────────────────────

def load_taxonomy_names(taxonomy_path):
    """Load all subject names from the taxonomy XML."""
    from lxml import etree
    tree = etree.parse(taxonomy_path)
    root = tree.getroot()
    names = root.xpath('//subject/name/text()')
    return set(n.lower().strip() for n in names)


def load_mapping(mapping_path):
    """Load lcsh_mapping.json."""
    with open(mapping_path) as f:
        return json.load(f)


# ── LCSH relevance filter ─────────────────────────────────────────

# Patterns that indicate LCSH headings NOT relevant to diplomatic history
IRRELEVANT_PATTERNS = [
    r'--Abstracts$',
    r'--Bibliography$',
    r'--Periodicals$',
    r'--Juvenile',
    r'--Fiction',
    r'--Pictorial',
    r'--Poetry',
    r'--Drama$',
    r'--Songs',
    r'--Caricatures',
    r'--Humor$',
    r'--Anecdotes$',
    r'--Examinations',
    r'--Study and teaching',
    r'in literature$',
    r'in art$',
    r'in motion pictures$',
    r'\bculture\b',  # aquaculture, etc.
    r'\bcooking\b',
    r'\bgardening\b',
    r'\brecipes\b',
]
IRRELEVANT_RE = re.compile('|'.join(IRRELEVANT_PATTERNS), re.IGNORECASE)

# LCSH headings that are too generic or off-topic
TOO_GENERIC = {
    'social sciences', 'civilization', 'auxiliary sciences of history',
    'history', 'science', 'technology', 'philosophy', 'religion',
    'arts', 'literature', 'music', 'education', 'language',
    'geography', 'mathematics', 'medicine',
}


def is_relevant_suggestion(term, existing_names):
    """Filter out LCSH suggestions that are irrelevant to FRUS."""
    tl = term.lower().strip()

    # Already in taxonomy
    if tl in existing_names:
        return False

    # LCSH subdivisions that are bibliographic/literary
    if IRRELEVANT_RE.search(term):
        return False

    # Too generic
    if tl in TOO_GENERIC:
        return False

    # Very short terms (likely noise)
    if len(term) < 4:
        return False

    # Raw LCSH identifiers (unresolved labels like "sh85124003")
    if re.match(r'^sh\d+$', tl):
        return False

    return True


# ── Offline mode: mine existing mapping data ──────────────────────

def discover_offline(mapping_path, taxonomy_path):
    """Discover candidates using only data already in lcsh_mapping.json.

    Strategy:
    1. Collect all 'all_suggestions' from the mapping — these are LCSH headings
       returned by suggest2 when querying our existing taxonomy terms.
    2. For each suggestion, score it by how many taxonomy terms it appeared near.
    3. Filter out irrelevant suggestions.
    4. Rank by relevance score.
    """
    mapping = load_mapping(mapping_path)
    existing_names = load_taxonomy_names(taxonomy_path)

    print(f"Loaded {len(mapping)} mapping entries, {len(existing_names)} taxonomy terms")

    # Collect suggestions with provenance
    suggestion_sources = defaultdict(list)  # suggestion → [taxonomy term names]

    for ref, entry in mapping.items():
        name = entry.get('name', '')
        suggestions = entry.get('all_suggestions', [])
        for sugg in suggestions:
            sugg_clean = sugg.strip()
            if sugg_clean and is_relevant_suggestion(sugg_clean, existing_names):
                suggestion_sources[sugg_clean].append(name)

    print(f"\n{len(suggestion_sources)} unique relevant suggestions")

    # Also mine broader terms — the labels of broader terms themselves can be candidates
    broader_labels = defaultdict(list)
    for ref, entry in mapping.items():
        name = entry.get('name', '')
        for bt in entry.get('broader_terms', []):
            if isinstance(bt, dict):
                label = bt.get('label', '')
                if label and is_relevant_suggestion(label, existing_names):
                    broader_labels[label].append(name)
        for bt in entry.get('broader_chain_2lvl', []):
            if isinstance(bt, dict):
                label = bt.get('label', '')
                if label and is_relevant_suggestion(label, existing_names):
                    broader_labels[label].append(name)

    # Merge broader terms as a second signal
    for label, sources in broader_labels.items():
        if label in suggestion_sources:
            # Boost score
            suggestion_sources[label].extend(sources)
        else:
            suggestion_sources[label] = sources

    # Build candidates
    candidates = []
    for term, sources in suggestion_sources.items():
        unique_sources = sorted(set(sources))
        # Skip terms that only appeared near one taxonomy term (low confidence)
        if len(unique_sources) < 2:
            continue

        # Skip if it's a geographic subdivision
        if '--' in term:
            base_term = term.split('--')[0].strip()
            # Keep only if base term itself is novel
            if base_term.lower() in existing_names:
                continue

        candidates.append({
            'term': term,
            'source_count': len(unique_sources),
            'source_terms': unique_sources[:10],
            'origin': 'lcsh_suggestion',
        })

    # Sort by source count (more taxonomy terms → more relevant neighborhood)
    candidates.sort(key=lambda c: (-c['source_count'], c['term']))

    stats = {
        'mapping_entries': len(mapping),
        'taxonomy_terms': len(existing_names),
        'unique_suggestions': len(suggestion_sources),
        'candidates_after_filter': len(candidates),
        'mode': 'offline',
        'generated': datetime.now().isoformat(),
    }

    print(f"Candidates (appeared near 2+ taxonomy terms): {len(candidates)}")
    return candidates, stats


# ── Online mode: full LCSH expansion ──────────────────────────────

def discover_online(mapping_path, taxonomy_path, max_broader=200):
    """Full LCSH expansion via id.loc.gov API.

    Uses broader-term clusters to fetch narrower terms and find siblings.
    """
    mapping = load_mapping(mapping_path)
    existing_names = load_taxonomy_names(taxonomy_path)

    # Build broader-term clusters from mapping
    broader_to_children = defaultdict(list)
    broader_labels = {}

    for ref, entry in mapping.items():
        name = entry.get('name', '')
        if not name:
            continue
        for bt in entry.get('broader_terms', []) + entry.get('broader_chain_2lvl', []):
            if isinstance(bt, dict):
                uri = bt.get('uri', '')
                label = bt.get('label', '')
                if uri:
                    broader_to_children[uri].append(name)
                    if label:
                        broader_labels[uri] = label

    ranked = sorted(broader_to_children.items(), key=lambda x: -len(set(x[1])))
    relevant = [(uri, names) for uri, names in ranked if len(set(names)) >= 2]

    print(f"{len(relevant)} broader headings with 2+ taxonomy children")

    # Load/init cache
    cache = {}
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE) as f:
            cache = json.load(f)

    to_explore = relevant[:max_broader]
    all_narrower = {}
    api_calls = 0
    failures = 0

    for i, (broader_uri, taxonomy_children) in enumerate(to_explore):
        cache_key = f"skos:{broader_uri}"
        if cache_key in cache and cache[cache_key] is not None:
            skos_data = cache[cache_key]
        else:
            lccn = broader_uri.rstrip("/").split("/")[-1]
            skos_url = SKOS_URL_TEMPLATE.format(lccn)
            try:
                time.sleep(REQUEST_DELAY)
                api_calls += 1
                req = urllib.request.Request(skos_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    skos_data = json.loads(resp.read().decode("utf-8"))
                cache[cache_key] = skos_data
            except Exception as e:
                failures += 1
                if failures <= 3:
                    print(f"  API error for {broader_uri}: {e}")
                elif failures == 4:
                    print(f"  (suppressing further API errors...)")
                cache[cache_key] = None
                continue

        if not skos_data:
            continue

        parent_label = broader_labels.get(broader_uri, broader_uri)

        # Find narrower terms in SKOS data
        for item in skos_data:
            if item.get("@id", "").rstrip("/") == broader_uri.rstrip("/"):
                narrower_refs = item.get(SKOS_NARROWER, [])
                if not isinstance(narrower_refs, list):
                    narrower_refs = [narrower_refs]
                for ref in narrower_refs:
                    n_uri = ref.get("@id", "") if isinstance(ref, dict) else str(ref)
                    if n_uri and n_uri not in all_narrower:
                        all_narrower[n_uri] = {
                            'uri': n_uri,
                            'parent_label': parent_label,
                            'parent_uri': broader_uri,
                            'taxonomy_siblings': sorted(set(taxonomy_children)),
                            'sibling_count': len(set(taxonomy_children)),
                        }

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(to_explore)} processed, {len(all_narrower)} narrower found")
            with open(CACHE_FILE, 'w') as f:
                json.dump(cache, f, indent=2)

    # Save cache
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

    if failures == len(to_explore):
        print(f"\n  All {failures} API calls failed — falling back to offline mode.")
        return discover_offline(mapping_path, taxonomy_path)

    # Resolve labels and diff
    candidates = []
    for n_uri, info in all_narrower.items():
        cache_key = f"skos:{n_uri}"
        if cache_key in cache and cache[cache_key] is not None:
            skos_data = cache[cache_key]
        else:
            lccn = n_uri.rstrip("/").split("/")[-1]
            skos_url = SKOS_URL_TEMPLATE.format(lccn)
            try:
                time.sleep(REQUEST_DELAY)
                api_calls += 1
                req = urllib.request.Request(skos_url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    skos_data = json.loads(resp.read().decode("utf-8"))
                cache[cache_key] = skos_data
            except Exception:
                continue

        if not skos_data:
            continue

        label = None
        for item in skos_data:
            if item.get("@id", "").rstrip("/") == n_uri.rstrip("/"):
                pref_labels = item.get(SKOS_PREFLABEL, [])
                if isinstance(pref_labels, list):
                    for pl in pref_labels:
                        if isinstance(pl, dict):
                            label = pl.get("@value", "")
                        elif isinstance(pl, str):
                            label = pl
                        if label:
                            break

        if not label:
            continue
        if not is_relevant_suggestion(label, existing_names):
            continue
        if '--' in label:
            continue

        candidates.append({
            'term': label,
            'lcsh_uri': n_uri,
            'parent_label': info['parent_label'],
            'parent_uri': info['parent_uri'],
            'taxonomy_siblings': info['taxonomy_siblings'],
            'sibling_count': info['sibling_count'],
            'origin': 'lcsh_narrower',
        })

    candidates.sort(key=lambda c: (-c['sibling_count'], c['term']))

    stats = {
        'broader_explored': len(to_explore),
        'total_narrower': len(all_narrower),
        'candidates': len(candidates),
        'api_calls': api_calls,
        'api_failures': failures,
        'mode': 'online',
        'generated': datetime.now().isoformat(),
    }

    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

    return candidates, stats


# ── Output ────────────────────────────────────────────────────────

def write_outputs(candidates, stats, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(output_dir, 'lcsh_candidates.json')
    with open(json_path, 'w') as f:
        json.dump({'stats': stats, 'candidates': candidates}, f, indent=2)
    print(f"\nWrote {json_path}")

    txt_path = os.path.join(output_dir, 'lcsh_candidates.txt')
    with open(txt_path, 'w') as f:
        f.write("FRUS LCSH-Seeded Discovery — Candidate Terms\n")
        f.write(f"Generated: {stats['generated']}\n")
        f.write(f"Mode: {stats['mode']}\n")
        f.write(f"Candidates: {stats.get('candidates_after_filter', stats.get('candidates', 0))}\n")
        f.write("=" * 72 + "\n\n")

        if stats['mode'] == 'offline':
            # Group by source count tiers
            high = [c for c in candidates if c['source_count'] >= 5]
            medium = [c for c in candidates if 3 <= c['source_count'] < 5]
            low = [c for c in candidates if c['source_count'] == 2]

            for tier_label, tier_items in [
                (f"HIGH CONFIDENCE ({len(high)}) — near 5+ taxonomy terms", high),
                (f"MEDIUM CONFIDENCE ({len(medium)}) — near 3-4 taxonomy terms", medium),
                (f"LOWER CONFIDENCE ({len(low)}) — near 2 taxonomy terms", low),
            ]:
                if not tier_items:
                    continue
                f.write(f"\n── {tier_label} ──\n\n")
                for c in tier_items:
                    f.write(f"  {c['term']}\n")
                    f.write(f"    Near: {', '.join(c['source_terms'][:5])}\n")
                    if len(c['source_terms']) > 5:
                        f.write(f"    + {len(c['source_terms']) - 5} more\n")
                    f.write("\n")
        else:
            # Group by parent
            by_parent = defaultdict(list)
            for c in candidates:
                by_parent[c['parent_label']].append(c)

            sorted_parents = sorted(
                by_parent.items(),
                key=lambda x: (-x[1][0]['sibling_count'], x[0])
            )

            for parent_label, parent_candidates in sorted_parents:
                sibling_count = parent_candidates[0]['sibling_count']
                siblings = parent_candidates[0]['taxonomy_siblings']
                f.write(f"\n── Under: {parent_label} ──\n")
                f.write(f"   ({sibling_count} existing taxonomy terms)\n")
                f.write(f"   Existing: {', '.join(siblings[:5])}\n\n")
                for c in parent_candidates:
                    f.write(f"    + {c['term']}\n")
                    f.write(f"      {c['lcsh_uri']}\n")

    print(f"Wrote {txt_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Discover candidate taxonomy terms via LCSH expansion'
    )
    parser.add_argument('--mapping', default=LCSH_MAPPING_FILE)
    parser.add_argument('--taxonomy', default=TAXONOMY_FILE)
    parser.add_argument('--output', default=OUTPUT_DIR)
    parser.add_argument('--max-broader', type=int, default=200)
    parser.add_argument('--offline', action='store_true',
                        help='Use only existing mapping data (no API calls)')
    args = parser.parse_args()

    if args.offline:
        candidates, stats = discover_offline(args.mapping, args.taxonomy)
    else:
        candidates, stats = discover_online(
            args.mapping, args.taxonomy, args.max_broader
        )

    write_outputs(candidates, stats, args.output)

    if candidates:
        print(f"\n── Top 20 candidates ──")
        for c in candidates[:20]:
            if c.get('source_count'):
                print(f"  [{c['source_count']} near] {c['term']}")
            else:
                print(f"  [{c.get('sibling_count', '?')} siblings] {c['term']}")


if __name__ == '__main__':
    main()
