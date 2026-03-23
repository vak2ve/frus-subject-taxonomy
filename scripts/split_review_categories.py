#!/usr/bin/env python3
"""
Split discovery candidates into separate category JSON files for
parallel review workflows.

Reads:
  - data/index_candidates.json    (Tier 2 back-of-book index candidates)
  - data/lcsh_candidates.json     (Tier 3 LCSH-seeded candidates)

Writes:
  - data/review_persons.json      (person-name entries)
  - data/review_organizations.json (organization + country entries)
  - data/review_topics.json       (topic + event + treaty + LCSH entries)

Each output file contains a "candidates" array in the same format as the
build_candidates_review.py pipeline expects, plus "stats" with counts.

Usage:
    python3 split_review_categories.py
"""

import json
import os
import re
from datetime import datetime

os.chdir(os.path.dirname(os.path.abspath(__file__)))

INDEX_CANDIDATES_FILE = "../data/index_candidates.json"
LCSH_CANDIDATES_FILE = "../data/lcsh_candidates.json"

OUTPUT_DIR = "../data"
OUTPUT_FILES = {
    "persons": os.path.join(OUTPUT_DIR, "review_persons.json"),
    "organizations": os.path.join(OUTPUT_DIR, "review_organizations.json"),
    "topics": os.path.join(OUTPUT_DIR, "review_topics.json"),
}


# ── Person-name detection (mirrors build_candidates_review.py) ──────────

_LNAME_WORD = r'[A-Z][a-zéèêëáàâäóòôöúùûüíìîïA-Z\'\-]+'
_PARTICLE = (
    r'(?:[Dd]e|[Dd]i|[Dd]a|[Dd]el|[Dd]ella|[Vv]an|[Vv]on|'
    r'[Ll]e|[Ll]a|[Ee]l|[Aa]l|[Bb]in|[Ii]bn|[Dd]en|[Dd]er|'
    r'[Dd]u|[Dd]os|[Dd]as|of)'
)
_LNAME = (
    r'(?:'
    + _PARTICLE + r'\s+' + _LNAME_WORD
    + r'|'
    + _LNAME_WORD
    + r'(?:\s+' + _PARTICLE + r'\s+' + _LNAME_WORD + r')*'
    + r')'
)
_TITLE = (
    r'(?:,?\s*(?:Gen|Adm|Col|Maj|Lt|Sgt|Capt|Cmdr|Cdr|Dr|Sir|Lord|'
    r'Prince|King|Emperor|Rev|Gov|Sen|Rep|Amb|Msgr|Fr|Brig)\.?\s*)?'
)
_PERSON_WESTERN = re.compile(
    rf'^{_LNAME},\s+'
    rf'{_TITLE}'
    r'[A-Z]',
    re.UNICODE
)

_NON_PERSON_GEO = {
    'australia', 'micronesia', 'korea', 'bahamas', 'trinidad',
    'bosnia', 'congo', 'ivory', 'guinea', 'sierra', 'sri',
    'papua', 'solomon', 'timor', 'burkina', 'czech',
    'dominican', 'equatorial', 'northern', 'china', 'germany',
    'saudi', 'south', 'united', 'vietnam', 'yemen', 'formosa',
    'marshall', 'great', 'costa', 'el', 'new', 'north',
    'hong', 'french', 'west', 'east', 'central', 'british',
}


def _looks_like_person(term):
    """Detect person names — same logic as build_candidates_review.py."""
    # Western pattern: "Lastname, Firstname..."
    if _PERSON_WESTERN.match(term):
        surname = term.split(',')[0].strip().lower()
        first_word = surname.split()[0] if surname else ''
        if first_word in _NON_PERSON_GEO:
            return False
        term_lower = term.lower()
        if any(term_lower.startswith(g) for g in [
            'marshall islands', 'micronesia, federated',
            'great britain', 'costa rica', 'el salvador',
            'new zealand', 'hong kong', 'french indochina',
        ]):
            return False
        after_comma = term.split(',', 1)[1].strip().lower() if ',' in term else ''
        concept_after = {
            'recognition', 'policy', 'relations', 'agreement', 'act',
            'conference', 'commission', 'committee', 'council',
        }
        if after_comma and after_comma.split()[0] in concept_after:
            return False
        return True

    # Non-Western names: short (2-5 words), all capitalized
    cleaned = re.sub(
        r',?\s*(?:Gen|Adm|Col|Maj|Lt|Sgt|Capt|Dr|Sir|Lord|'
        r'Prince|King|Emperor|Rev|Gov|Sen|Rep|Amb)\.?\s*$',
        '', term
    ).strip()
    words = cleaned.split()
    if 2 <= len(words) <= 5:
        if all(re.match(r'^[A-Z][a-zéèêëáàâäóòôöúùûü\'-]+$', w) for w in words):
            lowered = cleaned.lower()
            concept_words = {
                'act', 'agreement', 'alliance', 'charter', 'code', 'convention',
                'council', 'doctrine', 'fund', 'movement', 'national', 'party',
                'plan', 'pact', 'policy', 'program', 'project', 'reform',
                'revolution', 'service', 'system', 'treaty', 'union',
            }
            if any(w in concept_words for w in lowered.split()):
                return False
            return True

    return False


def classify_candidate(candidate):
    """Classify a candidate as 'persons', 'organizations', or 'topics'."""
    ctype = candidate.get('type', 'topic')

    # Organizations and countries → organizations
    if ctype in ('organization', 'country'):
        return 'organizations'

    # Person-name heuristic
    if _looks_like_person(candidate['term']):
        return 'persons'

    # Everything else → topics
    return 'topics'


def load_all_candidates():
    """Load and normalize candidates from both discovery tiers."""
    candidates = []

    # Tier 2: Index candidates
    if os.path.exists(INDEX_CANDIDATES_FILE):
        with open(INDEX_CANDIDATES_FILE) as f:
            data = json.load(f)
        for i, c in enumerate(data.get('candidates', [])):
            candidates.append({
                'id': f'idx-{i:04d}',
                'term': c['term'],
                'source': 'index',
                'type': c.get('type', 'topic'),
                'volume_count': c.get('volume_count', 0),
                'doc_count': c.get('doc_count', 0),
                'volumes': c.get('volumes', []),
                'sub_entries': c.get('sub_entries', []),
                'variants': c.get('variants', []),
                'normalized': c.get('normalized', ''),
            })

    # Tier 3: LCSH candidates
    if os.path.exists(LCSH_CANDIDATES_FILE):
        with open(LCSH_CANDIDATES_FILE) as f:
            data = json.load(f)
        for i, c in enumerate(data.get('candidates', [])):
            candidates.append({
                'id': f'lcsh-{i:04d}',
                'term': c['term'],
                'source': 'lcsh',
                'type': 'topic',
                'source_count': c.get('source_count', 0),
                'source_terms': c.get('source_terms', []),
                'lcsh_uri': c.get('lcsh_uri', ''),
                'parent_label': c.get('parent_label', ''),
            })

    # Filter malformed entries
    before = len(candidates)
    candidates = [c for c in candidates
                  if not re.search(r',\s*\d{2,3},\s*\n', c['term'])]
    malformed = before - len(candidates)
    if malformed:
        print(f"  Filtered out {malformed} malformed entries")

    return candidates


def main():
    print("Loading all candidates...")
    candidates = load_all_candidates()
    print(f"  {len(candidates)} total candidates loaded")

    # Classify into buckets
    buckets = {'persons': [], 'organizations': [], 'topics': []}
    for c in candidates:
        category = classify_candidate(c)
        buckets[category].append(c)

    # Write each category
    generated = datetime.now().isoformat()
    for category, items in buckets.items():
        output = {
            'stats': {
                'category': category,
                'count': len(items),
                'generated': generated,
            },
            'candidates': items,
        }
        outpath = OUTPUT_FILES[category]
        with open(outpath, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        print(f"  Wrote {outpath}: {len(items)} {category}")

    print(f"\nSummary:")
    print(f"  Persons:       {len(buckets['persons']):>6}")
    print(f"  Organizations: {len(buckets['organizations']):>6}")
    print(f"  Topics:        {len(buckets['topics']):>6}")
    print(f"  Total:         {sum(len(b) for b in buckets.values()):>6}")


if __name__ == '__main__':
    main()
