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


# ── Person-name detection ────────────────────────────────────────────────
# Handles Western "Surname, First" names (including multi-word surnames,
# special apostrophes, Roman-numeral royals) and non-Western names
# (2-5 capitalized words without commas).

# Surname word: uppercase start, accented/apostrophe/hyphen chars allowed.
# Uses broad Unicode ranges for Latin Extended characters to cover all
# European, Turkish, and transliterated names without listing every char.
_SNAME_UPPER = (
    r"[A-Z"
    r"\u00C0-\u00D6\u00D8-\u00DE"      # Latin-1 Supplement uppercase (À-Ö, Ø-Þ)
    r"\u0100-\u024E"                     # Latin Extended-A/B uppercase (Ā-Ɏ)
    r"]"
)
_SNAME_CHAR = (
    r"[a-z"
    r"\u00C0-\u00FF"                     # Latin-1 Supplement (covers àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ + uppercase)
    r"\u0100-\u024F"                     # Latin Extended-A/B (covers ā-ɏ: š,č,ž,ř,ğ,ş,ů,ĕ,ő,ű,etc.)
    r"A-Z"
    r"'\u2019\u02BC"                     # apostrophe variants
    r"\-"
    r"]"
)
_SNAME_WORD_RE = re.compile(r"^" + _SNAME_UPPER + _SNAME_CHAR + r"+$")

# Also match "al-Name", "al–Name" (em dash variant) as a surname word
_AL_PREFIX_RE = re.compile(
    r"^al[\-\u2013]" + r"[A-Z]" + _SNAME_CHAR + r"*$"
)

# "St." as a surname component (St. Laurent, etc.)
_ST_PREFIX_RE = re.compile(r"^St\.$")

# "d'" or "D'" contractions (d'Estaing, D'Orlandi)
_D_PREFIX_RE = re.compile(
    r"^[dD]['\u2019\u02BC]" + r"[A-Z]" + _SNAME_CHAR + r"*$"
)

# Surname particles (lowercase in multi-part names)
_PARTICLES = {
    'de', 'di', 'da', 'del', 'della', 'van', 'von', 'le', 'la', 'el',
    'al', 'al-', 'bin', 'ibn', 'den', 'der', 'du', 'dos', 'das', 'des',
    'of', 'ben', 'y', 'et', 'e', 'los', 'las', 'und', 'and',
    'abu', 'bu', 'abd', 'bint',  # Arabic name particles
}

# Roman numeral pattern (for royal/papal names like "Pius XII, Pope")
_ROMAN_RE = re.compile(r'^[IVXLCDM]+$')

# Geographic first-words that look like surnames but aren't
_NON_PERSON_GEO = {
    'australia', 'micronesia', 'korea', 'bahamas', 'trinidad',
    'bosnia', 'congo', 'ivory', 'guinea', 'sierra', 'sri',
    'papua', 'solomon', 'timor', 'burkina', 'czech',
    'dominican', 'equatorial', 'northern', 'china', 'germany',
    'saudi', 'south', 'united', 'vietnam', 'yemen', 'formosa',
    'marshall', 'great', 'costa', 'el', 'new', 'north',
    'hong', 'french', 'west', 'east', 'central', 'british',
}

# Multi-word geographic phrases
_GEO_PHRASES = [
    'marshall islands', 'micronesia, federated',
    'great britain', 'costa rica', 'el salvador',
    'new zealand', 'hong kong', 'french indochina',
]

# Concept words that appear after the comma in non-person entries
_CONCEPT_AFTER_COMMA = {
    'recognition', 'policy', 'relations', 'agreement', 'act',
    'conference', 'commission', 'committee', 'council', 'conventions',
    'international', 'federal', 'republic', 'economic', 'political',
    'military', 'bilateral', 'diplomatic', 'removal', 'attitude',
    'question', 'regency', 'accession', 'death', 'succeeds',
    'inc', 'ltd', 'corp',
}

# Title/honorific words that appear after the comma confirming a person
_TITLE_AFTER_COMMA = {
    'gen', 'adm', 'col', 'maj', 'lt', 'sgt', 'capt', 'cmdr', 'cdr',
    'dr', 'sir', 'lord', 'prince', 'princess', 'king', 'queen',
    'emperor', 'empress', 'rev', 'gov', 'sen', 'rep', 'amb', 'msgr',
    'fr', 'brig', 'generalissimo', 'pope', 'archbishop', 'sultan',
    'tsar', 'madame', 'mme', 'mr', 'mrs', 'baron', 'count', 'duke',
    'marshal', 'field', 'rear', 'vice', 'senator', 'general',
    'admiral', 'colonel', 'captain', 'brigadier',
}

# Abbreviation-in-parentheses at end of term
_ABBREV_PARENS_RE = re.compile(r'\(([A-Z][A-Za-z]*)\)\s*$')

# Concept words that indicate non-person in non-Western name detection
_CONCEPT_WORDS = {
    'act', 'agreement', 'alliance', 'army', 'charter', 'church', 'code',
    'command', 'conference', 'convention', 'council', 'defense', 'doctrine',
    'force', 'front', 'fund', 'group', 'intelligence', 'liberation',
    'missile', 'movement', 'national', 'nuclear', 'operation', 'party',
    'petroleum', 'plan', 'pact', 'policy', 'press', 'program', 'project',
    'radar', 'radio', 'reform', 'republic', 'revolution', 'service',
    'special', 'strategic', 'system', 'transport', 'treaty', 'union',
    'warning',
}


def _is_surname_word(word):
    """Check if a word looks like part of a surname.

    Accepts standard capitalized words, al-prefixed Arabic names,
    St. abbreviation, and d'/D' French contractions.
    """
    return bool(
        _SNAME_WORD_RE.match(word)
        or _AL_PREFIX_RE.match(word)
        or _ST_PREFIX_RE.match(word)
        or _D_PREFIX_RE.match(word)
    )


def _looks_like_person(term):
    """Detect person names using multiple heuristics.

    Handles:
    - Western "Surname, Firstname" (including multi-word/accented surnames)
    - Royal/papal names with Roman numerals ("Pius XII, Pope")
    - Names with special apostrophes (O'Connor, OʼShaughnessy)
    - Non-Western names (2-5 capitalized words, no comma)
    """
    # Skip index cross-references
    term_lower = term.lower()
    if 'see also' in term_lower or '(see ' in term_lower:
        return False
    if term.startswith('(') or term.endswith('. See'):
        return False

    # ── Pattern 1: "Surname(s), Something..." ──
    if ',' in term:
        before_comma = term.split(',', 1)[0].strip()
        after_comma = term.split(',', 1)[1].strip()

        if not after_comma:
            return False

        # Strip trailing parenthetical disambiguation like "(USSR)"
        before_test = re.sub(r'\s*\([^)]*\)\s*$', '', before_comma).strip()
        after_test = re.sub(r'\s*\([^)]*\)\s*$', '', after_comma).strip()
        if not after_test:
            return False

        first_after = after_test.split()[0].rstrip('.,;:').lower()

        # Concept word after comma → not a person
        if first_after in _CONCEPT_AFTER_COMMA:
            return False

        surname_words = before_test.split()
        if not surname_words:
            return False

        # Company indicators → not a person
        if any(w.rstrip('.,') in ('Co', 'Inc', 'Corp', 'Ltd', 'LLC')
               for w in surname_words):
            return False

        # Check geographic exclusions (skip for al- prefixed names)
        first_word_lower = surname_words[0].lower()
        if not first_word_lower.startswith('al-') and not first_word_lower.startswith('al\u2013'):
            # Multi-word geo phrases always excluded
            if any(before_test.lower().startswith(g) for g in _GEO_PHRASES):
                return False
            # Single-word surnames matching geo names excluded;
            # multi-word surnames only excluded if they look geographic
            if first_word_lower in _NON_PERSON_GEO:
                if len(surname_words) == 1:
                    return False
                # Multi-word: only exclude if all words are in geo set
                # (allows "Costa e Silva" but blocks "South Korea")
                non_particle = [w for w in surname_words
                                if w.lower() not in _PARTICLES]
                if all(w.lower() in _NON_PERSON_GEO for w in non_particle):
                    return False

        # Roman numeral royal/papal names: "Name ROMAN, Title"
        if len(surname_words) >= 2 and _ROMAN_RE.match(surname_words[-1]):
            name_part = surname_words[:-1]
            if all(_is_surname_word(w) or w.lower() in _PARTICLES
                   for w in name_part):
                return True

        # Standard surname check: 1-4 words, each a surname word or particle
        # Handle "al–Name" (en-dash variant) by normalizing to "al-Name"
        normalized_words = [
            w.replace('\u2013', '-') for w in surname_words
        ]
        if 1 <= len(normalized_words) <= 4:
            surname_ok = all(
                _is_surname_word(w) or w.lower() in _PARTICLES
                for w in normalized_words
            )
            if surname_ok:
                # After comma must start with uppercase (name or title)
                if after_test[0].isupper():
                    return True
                # Or a known title even if oddly cased
                if first_after in _TITLE_AFTER_COMMA:
                    return True

    # ── Pattern 2: Non-Western names (no comma, 2-5 capitalized words) ──
    cleaned = re.sub(
        r',?\s*(?:Gen|Adm|Col|Maj|Lt|Sgt|Capt|Dr|Sir|Lord|'
        r'Prince|King|Emperor|Rev|Gov|Sen|Rep|Amb)\.?\s*$',
        '', term
    ).strip()
    # Also strip trailing parenthetical
    cleaned = re.sub(r'\s*\([^)]*\)\s*$', '', cleaned).strip()
    words = cleaned.split()
    if 2 <= len(words) <= 5:
        if all(re.match(
            r"^" + _SNAME_UPPER + _SNAME_CHAR + r"+$", w
        ) for w in words):
            if any(w.lower() in _CONCEPT_WORDS for w in words):
                return False
            return True

    return False


def _looks_like_org_abbrev(term):
    """Detect entries ending with (ABBREVIATION) that are organizations.

    Returns True for multi-word entries with an acronym/abbreviation in
    trailing parentheses, e.g. "Voice of America (VOA)".
    Returns False for person-name entries with a disambiguation suffix,
    e.g. "Kovalev (USSR)".
    """
    m = _ABBREV_PARENS_RE.search(term)
    if not m:
        return False

    before = term[:m.start()].strip()
    if not before:
        # Bare abbreviation like "(IADB)" — treat as org
        return True

    # If the before-parens part contains a comma, check further.
    # Multi-word org titles can contain commas:
    #   "Commander in Chief, Far East (CINCFE)"
    # But person names with disambiguation also have commas:
    #   "Masri, al- (UAR)"
    if ',' in before:
        # If it has many words (4+), it's almost certainly an org title
        all_words = before.split()
        if len(all_words) >= 4:
            return True
        # Short entries with comma are likely person disambiguation
        return False

    words = before.split()

    # Single word before parens: almost always a person surname with
    # country/org disambiguation — unless it has multiple hyphens
    # (like "Systems-engineering-technical-direction")
    if len(words) == 1:
        if words[0].count('-') >= 2:
            return True  # compound technical term
        return False  # likely a surname

    # 2+ words: likely an org name with its acronym
    return True


def _looks_like_person_with_affiliation(term):
    """Detect single-surname entries with parenthetical disambiguation.

    Matches patterns like "Kovalev (USSR)", "Hunt (UK)", "Lam (RVN)" —
    a single capitalized word followed by a short abbreviation in parens.
    """
    m = _ABBREV_PARENS_RE.search(term)
    if not m:
        return False
    before = term[:m.start()].strip()
    # Handle "al-Surname (COUNTRY)" and "Masri, al- (COUNTRY)" patterns
    before_clean = re.sub(r',?\s*al-\s*$', '', before).strip()
    before_clean = re.sub(r'^al-\s*', '', before_clean).strip()
    words = before_clean.split()
    if len(words) == 1 and _is_surname_word(words[0]):
        # Single surname-looking word — it's a person
        return True
    if len(words) == 2 and all(_is_surname_word(w) for w in words):
        # Two capitalized words — likely a person name, not an org
        # (orgs tend to have common nouns, articles, etc.)
        lower_words = [w.lower() for w in words]
        org_indicators = {
            'air', 'army', 'catholic', 'civil', 'democratic', 'federal',
            'free', 'general', 'liberal', 'mutual', 'radio', 'special',
            'united', 'viet',
        }
        if any(w in org_indicators for w in lower_words):
            return False
        return True
    return False


def classify_candidate(candidate):
    """Classify a candidate as 'persons', 'organizations', or 'topics'."""
    ctype = candidate.get('type', 'topic')
    term = candidate['term']

    # Organizations and countries by type → organizations
    if ctype in ('organization', 'country'):
        return 'organizations'

    # Single-surname + (COUNTRY) pattern → persons
    # (must check before org-abbrev — these look like abbreviations but are names)
    if _looks_like_person_with_affiliation(term):
        return 'persons'

    # Abbreviation-in-parentheses heuristic for organizations
    if _looks_like_org_abbrev(term):
        return 'organizations'

    # Person-name heuristic
    if _looks_like_person(term):
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
