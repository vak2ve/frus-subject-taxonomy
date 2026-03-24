#!/usr/bin/env python3
"""
Apply automated triage decisions to taxonomy_review_state.json.

Reads triage results from data/triage_results.json and applies:
  1. Topics misclassification: reject person/place/country names + reassign to proper category
  2. Organization merge groups: flag duplicate org candidates for merging
  3. Cross-reference matches: flag candidates that duplicate existing taxonomy subjects

Usage:
    python3 scripts/apply_triage_decisions.py [--dry-run]
"""

import json
import os
import sys
from datetime import datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

STATE_FILE = '../taxonomy_review_state.json'
TRIAGE_FILE = '../data/triage_results.json'


def load_state():
    with open(STATE_FILE) as f:
        return json.load(f)


def save_state(state):
    state['saved'] = datetime.now().isoformat()
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def apply_topic_misclassifications(state, misclassified):
    """Reject person/place/country entries from topics and reassign them."""
    decisions = state.setdefault('candidate_decisions_topics', {})
    reassign_persons = state.setdefault('reassigned_to_persons', [])
    reassign_orgs = state.setdefault('reassigned_to_organizations', [])

    # Load topics candidates to get full entry data for reassignment
    with open('../data/review_topics.json') as f:
        topics_data = json.load(f)
    candidates_by_id = {c['id']: c for c in topics_data.get('candidates', [])}

    stats = {'person': 0, 'country': 0, 'place': 0, 'skipped': 0}

    for entry in misclassified:
        cid = entry['id']
        reason = entry['reason']

        # Skip if already decided
        if cid in decisions:
            stats['skipped'] += 1
            continue

        candidate = candidates_by_id.get(cid)
        if not candidate:
            stats['skipped'] += 1
            continue

        if reason == 'person_name':
            # Mark as reassigned in topics, add to persons queue
            decisions[cid] = {
                'action': 'reassigned',
                'targetCategory': 'persons',
                'term': entry['term'],
                'auto_triage': True,
                'triage_reason': reason,
            }
            reassign_entry = {
                **candidate,
                'id': f"reassign-topics-{cid}",
                'reassignedFrom': 'topics',
            }
            existing_ids = {e['id'] for e in reassign_persons}
            if reassign_entry['id'] not in existing_ids:
                reassign_persons.append(reassign_entry)
            stats['person'] += 1

        elif reason == 'country_name':
            # Countries go to organizations
            decisions[cid] = {
                'action': 'reassigned',
                'targetCategory': 'organizations',
                'term': entry['term'],
                'auto_triage': True,
                'triage_reason': reason,
            }
            reassign_entry = {
                **candidate,
                'id': f"reassign-topics-{cid}",
                'reassignedFrom': 'topics',
            }
            existing_ids = {e['id'] for e in reassign_orgs}
            if reassign_entry['id'] not in existing_ids:
                reassign_orgs.append(reassign_entry)
            stats['country'] += 1

        elif reason == 'place_name':
            # Places rejected from topics (no places category yet)
            decisions[cid] = {
                'action': 'rejected',
                'term': entry['term'],
                'auto_triage': True,
                'triage_reason': reason,
            }
            stats['place'] += 1

    return stats


def apply_org_merge_groups(state, merge_groups):
    """Flag organization candidates that should be merged together."""
    decisions = state.setdefault('candidate_decisions_organizations', {})
    stats = {'groups': 0, 'merged_into_existing': 0, 'merged_candidates': 0, 'skipped': 0}

    for group in merge_groups:
        candidates = group.get('candidates', [])
        existing_match = group.get('existing_taxonomy_match')
        action = group.get('recommended_action', 'merge_candidates')

        if len(candidates) < 2 and not existing_match:
            stats['skipped'] += 1
            continue

        stats['groups'] += 1

        if action == 'merge_into_existing' and existing_match:
            # All candidates merge into existing taxonomy subject
            for c in candidates:
                cid = c['id']
                if cid in decisions:
                    continue
                decisions[cid] = {
                    'action': 'merged',
                    'mergeTarget': existing_match['ref'],
                    'mergeTargetName': existing_match['name'],
                    'term': c['term'],
                    'auto_triage': True,
                    'triage_reason': 'org_duplicate_of_existing',
                }
                stats['merged_into_existing'] += 1
        else:
            # Merge candidates together — keep first, merge rest into it
            primary = candidates[0]
            for c in candidates[1:]:
                cid = c['id']
                if cid in decisions:
                    continue
                decisions[cid] = {
                    'action': 'merged',
                    'mergeTarget': primary['id'],
                    'mergeTargetName': primary['term'],
                    'term': c['term'],
                    'auto_triage': True,
                    'triage_reason': 'org_variant_merge',
                }
                stats['merged_candidates'] += 1

    return stats


def apply_cross_references(state, cross_refs):
    """Flag candidates that match existing taxonomy subjects."""
    category_keys = {
        'topics': 'candidate_decisions_topics',
        'persons': 'candidate_decisions_persons',
        'organizations': 'candidate_decisions_organizations',
    }
    stats = {'exact': 0, 'sub_entry': 0, 'near_duplicate': 0, 'variant': 0, 'skipped': 0}

    for match in cross_refs:
        category = match.get('category', 'topics')
        state_key = category_keys.get(category)
        if not state_key:
            stats['skipped'] += 1
            continue

        decisions = state.setdefault(state_key, {})
        cid = match['candidate_id']

        # Skip if already decided
        if cid in decisions:
            stats['skipped'] += 1
            continue

        match_type = match.get('match_type', 'near_duplicate')

        decisions[cid] = {
            'action': 'merged',
            'mergeTarget': match['existing_ref'],
            'mergeTargetName': match['existing_name'],
            'term': match['candidate_term'],
            'auto_triage': True,
            'triage_reason': f'cross_ref_{match_type}',
        }
        stats[match_type] = stats.get(match_type, 0) + 1

    return stats


def main():
    dry_run = '--dry-run' in sys.argv

    if not os.path.exists(TRIAGE_FILE):
        print(f"ERROR: {TRIAGE_FILE} not found. Run triage analysis first.")
        sys.exit(1)

    with open(TRIAGE_FILE) as f:
        triage = json.load(f)

    state = load_state()

    print("Applying triage decisions...")
    print()

    # 1. Topic misclassifications
    misclassified = triage.get('topic_misclassifications', [])
    if misclassified:
        stats = apply_topic_misclassifications(state, misclassified)
        print(f"Topic misclassifications: {len(misclassified)} entries")
        print(f"  → {stats['person']} reassigned to persons")
        print(f"  → {stats['country']} reassigned to organizations")
        print(f"  → {stats['place']} rejected (place names)")
        print(f"  → {stats['skipped']} skipped (already decided)")
        print()

    # 2. Org merge groups
    merge_groups = triage.get('org_merge_groups', [])
    if merge_groups:
        stats = apply_org_merge_groups(state, merge_groups)
        print(f"Organization merge groups: {stats['groups']} groups")
        print(f"  → {stats['merged_into_existing']} merged into existing taxonomy subjects")
        print(f"  → {stats['merged_candidates']} merged as candidate variants")
        print(f"  → {stats['skipped']} skipped")
        print()

    # 3. Cross-references
    cross_refs = triage.get('cross_references', [])
    if cross_refs:
        stats = apply_cross_references(state, cross_refs)
        print(f"Cross-references: {len(cross_refs)} matches")
        print(f"  → {stats['exact']} exact matches")
        print(f"  → {stats['sub_entry']} sub-entries")
        print(f"  → {stats['near_duplicate']} near-duplicates")
        print(f"  → {stats['variant']} variants")
        print(f"  → {stats['skipped']} skipped")
        print()

    # Summary
    total_decisions = 0
    for key in ['candidate_decisions_topics', 'candidate_decisions_persons',
                'candidate_decisions_organizations']:
        if key in state:
            auto = sum(1 for d in state[key].values()
                       if isinstance(d, dict) and d.get('auto_triage'))
            total_decisions += auto
    print(f"Total auto-triage decisions: {total_decisions}")

    if dry_run:
        print("\n[DRY RUN] No changes saved.")
    else:
        save_state(state)
        print(f"\nSaved to {STATE_FILE}")


if __name__ == '__main__':
    main()
