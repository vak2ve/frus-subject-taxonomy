#!/usr/bin/env python3
"""
Promote accepted discovery candidates into the taxonomy pipeline.

Reads accepted candidates from taxonomy_review_state.json → candidate_decisions,
generates new subject entries with assigned categories, and writes them to
config/promoted_candidates.json. These entries are then picked up by
build_taxonomy_lcsh.py on the next rebuild, making them appear in the
taxonomy XML and therefore in taxonomy-review.html.

Also creates search terms for the annotation engine so promoted terms
get matched against documents.

Usage:
    python3 scripts/promote_candidates.py

After running:
    make taxonomy-review   (to see promoted terms in the main review tool)
    make discover          (to update candidate lists, now excluding promoted terms)
"""

import json
import os
import sys
from datetime import datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

STATE_FILE = "../taxonomy_review_state.json"
INDEX_CANDIDATES_FILE = "../data/index_candidates.json"
LCSH_CANDIDATES_FILE = "../data/lcsh_candidates.json"
PROMOTED_FILE = "../config/promoted_candidates.json"
VARIANT_OVERRIDES_FILE = "../config/variant_overrides.json"


def load_state():
    if not os.path.exists(STATE_FILE):
        print("No taxonomy_review_state.json found.")
        return {}
    with open(STATE_FILE) as f:
        return json.load(f)


def load_candidates():
    """Load full candidate data from discovery outputs."""
    candidates = {}

    if os.path.exists(INDEX_CANDIDATES_FILE):
        with open(INDEX_CANDIDATES_FILE) as f:
            data = json.load(f)
        for i, c in enumerate(data.get('candidates', [])):
            cid = f'idx-{i:04d}'
            candidates[cid] = c

    if os.path.exists(LCSH_CANDIDATES_FILE):
        with open(LCSH_CANDIDATES_FILE) as f:
            data = json.load(f)
        for i, c in enumerate(data.get('candidates', [])):
            cid = f'lcsh-{i:04d}'
            candidates[cid] = c

    return candidates


def generate_ref():
    """Generate a unique ref ID for a promoted candidate."""
    import hashlib
    import time
    raw = f"promoted-{time.time_ns()}"
    return f"rec{hashlib.sha1(raw.encode()).hexdigest()[:15]}"


def main():
    state = load_state()
    # Collect decisions from all per-category keys and legacy key
    decisions = {}
    for key in ('candidate_decisions', 'candidate_decisions_persons',
                'candidate_decisions_organizations', 'candidate_decisions_topics'):
        category_decisions = state.get(key, {})
        if category_decisions:
            print(f"  {key}: {len(category_decisions)} decisions")
            decisions.update(category_decisions)

    if not decisions:
        print("No candidate decisions found in state.")
        return

    accepted = {k: v for k, v in decisions.items() if v.get('action') == 'accepted'}
    merged = {k: v for k, v in decisions.items() if v.get('action') == 'merged'}
    rejected = {k: v for k, v in decisions.items() if v.get('action') == 'rejected'}

    print(f"Candidate decisions: {len(accepted)} accepted, "
          f"{len(merged)} merged, {len(rejected)} rejected")

    if not accepted and not merged:
        print("Nothing to promote.")
        return

    # Load full candidate data
    all_candidates = load_candidates()

    # Load existing promoted candidates
    existing_promoted = []
    if os.path.exists(PROMOTED_FILE):
        with open(PROMOTED_FILE) as f:
            existing_promoted = json.load(f)

    existing_terms = {p['name'].lower() for p in existing_promoted}

    # Build promoted entries for accepted candidates
    new_promoted = []
    for cid, decision in accepted.items():
        term = decision.get('term', '')
        if not term or term.lower() in existing_terms:
            continue

        candidate_data = all_candidates.get(cid, {})

        ref = generate_ref()
        entry = {
            'ref': ref,
            'name': term,
            'category': decision.get('category', 'Uncategorized'),
            'subcategory': decision.get('subcategory', 'General'),
            'source': 'discovery',
            'discovery_id': cid,
            'promoted_at': datetime.now().isoformat(),
        }

        # Add volume/doc info if from index
        if candidate_data.get('volumes'):
            entry['volumes'] = candidate_data['volumes']
        if candidate_data.get('doc_count'):
            entry['doc_count'] = candidate_data['doc_count']
        if candidate_data.get('volume_docs'):
            entry['volume_docs'] = candidate_data['volume_docs']

        # Add LCSH info if from LCSH discovery
        if candidate_data.get('lcsh_uri'):
            entry['lcsh_uri'] = candidate_data['lcsh_uri']

        new_promoted.append(entry)
        existing_terms.add(term.lower())

    # Handle merged candidates: add as variant overrides
    new_merge_overrides = []
    for cid, decision in merged.items():
        term = decision.get('term', '')
        target_ref = decision.get('mergeTarget', '')
        target_name = decision.get('mergeTargetName', '')
        if not term or not target_ref:
            continue

        # We don't create a new ref — instead, add as a search variant
        # of the existing term via variant_overrides
        new_merge_overrides.append({
            'action': 'add_search_name',
            'canonical_ref': target_ref,
            'search_name': term,
            'reason': f"Candidate review: index/LCSH term '{term}' merged into '{target_name}'",
            'source': 'candidates-review',
        })

    # Write promoted candidates
    all_promoted = existing_promoted + new_promoted
    with open(PROMOTED_FILE, 'w') as f:
        json.dump(all_promoted, f, indent=2, ensure_ascii=False)
        f.write('\n')
    print(f"Wrote {PROMOTED_FILE}: {len(new_promoted)} new, {len(all_promoted)} total")

    # Write merge overrides
    if new_merge_overrides:
        overrides_data = {"overrides": []}
        if os.path.exists(VARIANT_OVERRIDES_FILE):
            with open(VARIANT_OVERRIDES_FILE) as f:
                overrides_data = json.load(f)

        # Remove old candidates-review entries to avoid duplicates
        kept = [o for o in overrides_data.get('overrides', [])
                if o.get('source') != 'candidates-review']
        kept.extend(new_merge_overrides)
        overrides_data['overrides'] = kept
        overrides_data['updated'] = datetime.now().strftime('%Y-%m-%d')

        with open(VARIANT_OVERRIDES_FILE, 'w') as f:
            json.dump(overrides_data, f, indent=2, ensure_ascii=False)
            f.write('\n')
        print(f"Added {len(new_merge_overrides)} search-name variants to variant_overrides.json")

    print(f"\nTo see promoted terms in the taxonomy review tool:")
    print(f"  1. Run: make taxonomy-review")
    print(f"  2. Open taxonomy-review.html")


if __name__ == '__main__':
    main()
