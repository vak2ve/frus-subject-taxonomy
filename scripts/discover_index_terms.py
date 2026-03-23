#!/usr/bin/env python3
"""
Tier 2 — Back-of-book index extraction for term discovery.

Parses the <div subtype="index" xml:id="index"> sections from FRUS TEI
volumes, extracts top-level (main) subject headings, normalizes them,
and diffs against the existing taxonomy to surface candidate terms that
appear in professionally-curated indexes but are not yet in the taxonomy.

Handles two index reference styles:
  - Document numbers: "References are to document numbers." (post-Nixon)
  - Page numbers: references to page numbers (pre-Nixon, mapped via <pb>)

Output:
  - data/index_candidates.json   — full candidate list with provenance
  - data/index_candidates.txt    — human-readable review list

Usage:
  python3 scripts/discover_index_terms.py [--volumes DIR] [--taxonomy FILE]
"""

import argparse
import json
import os
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import datetime
from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

TEI_NS = "http://www.tei-c.org/ns/1.0"
NS = {"tei": TEI_NS}
# Default: full FRUS corpus (not the already-annotated volumes in ../volumes)
VOLUMES_DIR = os.environ.get(
    "FRUS_CORPUS_DIR",
    os.path.expanduser("~/mnt/Subject taxonomy/volumes"),
)
TAXONOMY_FILE = "../subject-taxonomy-lcsh.xml"
OUTPUT_DIR = "../data"
# Volumes already in the annotation pipeline — skip these for discovery
ANNOTATED_VOLUMES_DIR = "../volumes"
# Minimum file size to skip unpublished slugs
MIN_VOLUME_SIZE = 10_000  # 10 KB — slugs are ~2.4 KB

# ── Heuristics for classifying index entries ─────────────────────

# Person-name patterns: entries that are clearly person names
# Pattern: "Lastname, Firstname" with optional middle/nickname/suffix
# Handles both ASCII and Unicode curly quotes for nicknames
_LNAME = r'[A-Z][a-zéèêëáàâäóòôöúùûüíìîïñ\']+'
_FNAME = r'[A-Z][a-zéèêëáàâäóòôöúùûüíìîïñ]+'
_NICK = r'(?:\s*["""\u2018\u2019\u201c\u201d][^"""\u2018\u2019\u201c\u201d]+["""\u2018\u2019\u201c\u201d])?'
PERSON_PATTERN = re.compile(
    rf'^{_LNAME},\s+'           # Lastname,
    rf'{_FNAME}'                # Firstname
    rf'{_NICK}'                 # optional "Nickname" or "Nickname"
    r'(?:\s+[A-Z]\.?)?'        # optional middle initial
    r'(?:\s+(?:Jr|Sr|III?|IV)\.?)?'  # optional suffix
    rf'(?:\s+{_FNAME})?'       # optional second name
    r',?$'
)

# Cross-reference patterns
SEE_PATTERN = re.compile(r'\bSee\b', re.IGNORECASE)

# Entries that are abbreviation/acronym cross-refs
ABBREV_XREF = re.compile(r'^[A-Z]{2,}\.\s*See\b', re.IGNORECASE)

# Words that strongly indicate a person entry
PERSON_INDICATORS = {
    'meeting with', 'meetings with', 'role in', 'role of',
    'views on', 'participant in', 'visit to', 'death',
    'health', 'appointment', 'resignation', 'assassination',
}


def is_person_entry(text, has_sub_entries=False, sub_texts=None):
    """Heuristic: does this index main entry look like a person name?

    Checks:
    1. Matches "Lastname, Firstname" pattern
    2. Sub-entries use person-specific language ("meeting with", "role in")
    """
    # Direct pattern match
    # Strip doc refs for the check: "Smith, John, 42, 277" → "Smith, John"
    name_part = re.sub(r',?\s*\d[\d,\s]*$', '', text).strip()
    name_part = re.sub(r':$', '', name_part).strip()

    if PERSON_PATTERN.match(name_part):
        return True

    # Check sub-entry language
    if sub_texts:
        person_signals = sum(
            1 for st in sub_texts
            if any(ind in st.lower() for ind in PERSON_INDICATORS)
        )
        if person_signals >= 2 and len(sub_texts) >= 2:
            return True
        # If >50% of sub-entries match person indicators
        if sub_texts and person_signals / len(sub_texts) > 0.5:
            return True

    return False


# ── Page-to-document mapping ──────────────────────────────────────

def build_page_to_doc_map(tree):
    """Build a mapping from page numbers to document IDs.

    Walks all <pb> elements, tracking which <div type="document"> they
    fall inside. Returns dict: page_number_str → document_id.

    Used for pre-Nixon volumes where indexes reference page numbers.
    """
    ns = NS
    page_map = {}
    # Find all documents
    docs = tree.xpath(
        '//tei:div[@type="document"]',
        namespaces=ns
    )

    for doc in docs:
        doc_id = doc.get('{http://www.w3.org/XML/1998/namespace}id', '')
        doc_n = doc.get('n', '')
        # Find all <pb> elements inside this document
        pbs = doc.xpath('.//tei:pb', namespaces=ns)
        for pb in pbs:
            page_n = pb.get('n', '')
            if page_n and page_n.isdigit():
                page_map[page_n] = {
                    'doc_id': doc_id,
                    'doc_n': doc_n
                }

    return page_map


# ── Index parsing ─────────────────────────────────────────────────

def get_text_content(elem):
    """Extract all text from an element, ignoring child structure."""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(get_text_content(child))
        if child.tail:
            parts.append(child.tail)
    return ''.join(parts)


def extract_heading_text(item_elem):
    """Extract the heading text from an index <item>, stripping doc refs.

    Handles two formats:
      1. Post-Nixon: items with nested <list> sub-entries — take text before
         the first <list>.
      2. Pre-Nixon (inline): heading text followed directly by <ref> page
         numbers and inline sub-entries separated by semicolons.
         e.g., "Finland, 236, 249; U.S. military assistance..."
         We extract only "Finland".

    Returns the subject heading text without page/doc numbers or sub-topics.
    """
    # Extract heading text: take text before the first <ref> or <list>.
    # This handles both formats:
    #   - Post-Nixon: items with nested <list> sub-entries
    #   - Pre-Nixon: inline text followed by <ref> page numbers
    # Some entries mix both (inline refs before a nested <list>), so we
    # always stop at whichever comes first.
    stop_tags = {'ref', 'list'}
    parts = []
    if item_elem.text:
        parts.append(item_elem.text)
    for child in item_elem:
        tag = child.tag.split('}')[-1] if '}' in child.tag else child.tag
        if tag in stop_tags:
            break
        parts.append(get_text_content(child))
        if child.tail:
            parts.append(child.tail)
    full = ''.join(parts)

    # Strip inline sub-entries after semicolons:
    # "Finland; U.S. military assistance..." → "Finland"
    if '; ' in full:
        full = full.split('; ')[0]

    # Strip trailing page/doc number refs: ", 42, 277, 378:"
    full = re.sub(r'[,\s]*\d[\d,\snp–\-]*:?$', '', full).strip()
    # Strip trailing colon or comma
    full = re.sub(r'[:,]$', '', full).strip()
    # Collapse whitespace (from TEI indentation)
    full = re.sub(r'\s+', ' ', full).strip()

    return full


def detect_reference_style(index_div):
    """Detect whether index uses document numbers or page numbers.

    Returns 'document' or 'page'.
    """
    ns = NS
    # Check for explicit statement
    paragraphs = index_div.findall(f'{{{TEI_NS}}}p')
    for p in paragraphs:
        text = get_text_content(p).lower()
        if 'document number' in text:
            return 'document'
        if 'page number' in text or 'pages' in text:
            return 'page'

    # Default: check ref targets — #d123 = document, #pg_123 = page
    refs = index_div.xpath('.//tei:ref/@target', namespaces=ns)
    doc_refs = sum(1 for r in refs if r.startswith('#d'))
    page_refs = sum(1 for r in refs if r.startswith('#pg_') or r.startswith('#p'))
    if doc_refs > page_refs:
        return 'document'
    return 'page' if page_refs > 0 else 'document'


def extract_doc_refs(item_elem):
    """Extract document IDs referenced by this index entry."""
    refs = item_elem.findall(f'.//{{{TEI_NS}}}ref')
    doc_ids = []
    for ref in refs:
        target = ref.get('target', '')
        # Skip cross-references to other index entries
        if target.startswith('#main-') or target.startswith('#sub-'):
            continue
        if target.startswith('#d'):
            doc_ids.append(target.lstrip('#'))
    return doc_ids


def parse_index(volume_path):
    """Parse a volume's back-of-book index.

    Returns list of dicts:
      {
        'heading': str,          # Normalized heading text
        'raw_heading': str,      # Original heading text
        'is_person': bool,       # Heuristic classification
        'is_see_ref': bool,      # "See" cross-reference
        'doc_refs': [str],       # Document IDs referenced
        'sub_entries': [str],    # Sub-entry texts
        'volume': str,           # Volume ID
        'xml_id': str,           # Index entry xml:id
      }
    """
    tree = etree.parse(volume_path)
    vol_id = os.path.basename(volume_path).replace('.xml', '')

    ns = NS

    # Find the main subject index (xml:id="index"), not terms/persons
    index_div = tree.xpath(
        '//tei:div[@subtype="index"][@xml:id="index"]',
        namespaces=ns
    )
    if not index_div:
        # Try without xml:id restriction — some volumes may differ
        all_indexes = tree.xpath(
            '//tei:div[@subtype="index"]',
            namespaces=ns
        )
        # Skip terms and persons sections
        for idx in all_indexes:
            xid = idx.get('{http://www.w3.org/XML/1998/namespace}id', '')
            if xid not in ('terms', 'persons'):
                index_div = [idx]
                break

    if not index_div:
        return []

    index_div = index_div[0]
    ref_style = detect_reference_style(index_div)

    # If page-number style, build mapping
    page_map = None
    if ref_style == 'page':
        page_map = build_page_to_doc_map(tree)

    # Find main (top-level) entries.
    # Post-Nixon volumes use xml:id="main-*"; earlier volumes use plain
    # <item> children of the top-level <list>.
    main_items = index_div.xpath(
        './/tei:item[starts-with(@xml:id, "main-")]',
        namespaces=ns
    )
    if not main_items:
        # Fallback: direct children of the first top-level <list>
        top_lists = index_div.xpath('./tei:list', namespaces=ns)
        if top_lists:
            main_items = top_lists[0].xpath('./tei:item', namespaces=ns)

    entries = []
    for item in main_items:
        xml_id = item.get('{http://www.w3.org/XML/1998/namespace}id', '')
        heading = extract_heading_text(item)

        if not heading or len(heading) < 2:
            continue

        # Check for "See" cross-reference
        is_see = bool(item.xpath('.//tei:hi[@rend="italic"][contains(text(), "See")]',
                                 namespaces=ns))
        if not is_see:
            is_see = bool(SEE_PATTERN.search(heading))

        # Extract sub-entries
        sub_items = item.xpath(
            './tei:list/tei:item',
            namespaces=ns
        )
        sub_texts = []
        for si in sub_items:
            st = get_text_content(si)
            st = re.sub(r'[,\s]*\d[\d,\s]*$', '', st).strip()
            sub_texts.append(st)

        # Document references
        doc_refs = extract_doc_refs(item)

        # Person classification — use <persName> tag if present (older volumes)
        has_persname = bool(item.xpath('.//tei:persName', namespaces=ns))
        person = has_persname or is_person_entry(heading, bool(sub_texts), sub_texts)

        entries.append({
            'heading': heading,
            'raw_heading': heading,
            'is_person': person,
            'is_see_ref': is_see,
            'doc_refs': doc_refs,
            'sub_entries': sub_texts[:5],  # Cap for output size
            'volume': vol_id,
            'xml_id': xml_id,
            'ref_style': ref_style,
        })

    return entries


# ── Normalization ────────────────────────────────────────────────

def classify_entry_type(heading, sub_texts=None):
    """Classify an index entry into a broad type for review grouping.

    Returns one of: 'country', 'organization', 'treaty', 'event', 'topic'
    """
    hl = heading.lower()

    # Countries/regions — check against known patterns
    country_suffixes = [
        'republic', 'kingdom', 'islands', 'island',
    ]
    country_indicators = [
        'north ', 'south ', 'east ', 'west ',
    ]
    # Well-known country/region names that appear in FRUS indexes
    known_geo = {
        'afghanistan', 'alaska', 'albania', 'algeria', 'angola', 'argentina',
        'australia', 'austria', 'bahamas', 'bahrain', 'bangladesh', 'barbados',
        'belgium', 'belize', 'benin', 'bhutan', 'bolivia', 'bosnia', 'botswana',
        'brazil', 'brunei', 'bulgaria', 'burma', 'burundi', 'cambodia',
        'cameroon', 'canada', 'chad', 'chile', 'china', 'colombia', 'comoros',
        'congo', 'costa rica', 'croatia', 'cuba', 'cyprus', 'czechoslovakia',
        'denmark', 'djibouti', 'dominica', 'dominican republic', 'ecuador',
        'egypt', 'el salvador', 'eritrea', 'estonia', 'ethiopia', 'fiji',
        'finland', 'formosa', 'france', 'gabon', 'gambia', 'georgia', 'germany',
        'ghana', 'greece', 'grenada', 'guatemala', 'guinea', 'guyana', 'haiti',
        'honduras', 'hong kong', 'hungary', 'iceland', 'india', 'indonesia',
        'iran', 'iraq', 'ireland', 'israel', 'italy', 'ivory coast', 'jamaica',
        'japan', 'jordan', 'kenya', 'korea', 'kosovo', 'kuwait', 'laos',
        'latvia', 'lebanon', 'lesotho', 'liberia', 'libya', 'lithuania',
        'luxembourg', 'macau', 'madagascar', 'malawi', 'malaya', 'malaysia',
        'maldives', 'mali', 'malta', 'mauritania', 'mauritius', 'mexico',
        'micronesia', 'moldova', 'mongolia', 'montenegro', 'morocco',
        'mozambique', 'namibia', 'nepal', 'netherlands', 'new zealand',
        'nicaragua', 'niger', 'nigeria', 'norway', 'oman', 'pakistan', 'panama',
        'papua new guinea', 'paraguay', 'peru', 'philippines', 'poland',
        'portugal', 'puerto rico', 'qatar', 'rhodesia', 'romania', 'rwanda',
        'samoa', 'saudi arabia', 'senegal', 'serbia', 'sierra leone',
        'singapore', 'slovakia', 'slovenia', 'somalia', 'south africa',
        'south korea', 'north korea', 'spain', 'sri lanka', 'sudan',
        'suriname', 'swaziland', 'sweden', 'switzerland', 'syria', 'taiwan',
        'tanzania', 'thailand', 'tibet', 'togo', 'trinidad', 'trinidad and tobago',
        'tunisia', 'turkey', 'turkmenistan', 'uganda', 'ukraine',
        'united arab emirates', 'united kingdom', 'united states', 'uruguay',
        'ussr', 'uzbekistan', 'vatican', 'venezuela', 'vietnam', 'yemen',
        'yugoslavia', 'zaire', 'zambia', 'zimbabwe', 'zanzibar',
        # Regions
        'middle east', 'southeast asia', 'central america', 'latin america',
        'south asia', 'east asia', 'europe', 'africa', 'caribbean',
        'pacific islands', 'indian ocean', 'persian gulf', 'soviet union',
        'western europe', 'eastern europe', 'near east', 'far east',
        'indochina', 'french indochina', 'french guiana', 'suez canal',
        'berlin', 'manchuria', 'okinawa',
    }
    if hl in known_geo:
        return 'country'
    # Match "Country, qualifier" patterns like "China, People's Republic of"
    # or "Korea, Republic of" or "Congo (Kinshasa)"
    base = re.split(r'[,(]', hl)[0].strip()
    if base in known_geo:
        return 'country'

    # Organizations/agencies
    org_patterns = [
        r'\b(?:agency|department|bureau|commission|committee|council|office)\b',
        r'\b(?:organization|association|federation|institute|foundation)\b',
        r'\b(?:bank|fund|corps|service|authority|administration)\b',
        r'\bU\.S\.\s', r'\bUN\b', r'\bNATO\b', r'\bOAS\b',
    ]
    for pat in org_patterns:
        if re.search(pat, heading, re.IGNORECASE):
            return 'organization'

    # Treaties/agreements
    treaty_patterns = [
        r'\b(?:treaty|agreement|accord|convention|protocol|pact)\b',
        r'\b(?:SALT|START|ABM|INF|CFE|NPT)\b',
    ]
    for pat in treaty_patterns:
        if re.search(pat, heading, re.IGNORECASE):
            return 'treaty'

    # Events
    event_patterns = [
        r'\b(?:war|crisis|incident|summit|conference|talks|invasion)\b',
        r'\(\d{4}\)', r'\b\d{4}\b',  # Has a year
    ]
    for pat in event_patterns:
        if re.search(pat, heading, re.IGNORECASE):
            return 'event'

    return 'topic'


def normalize_term(text):
    """Normalize an index heading for comparison against taxonomy.

    - Strip parenthetical clarifiers: "SALT (Strategic Arms Limitation Talks)" → "SALT"
    - Collapse whitespace
    - Strip leading/trailing punctuation
    - NFC unicode normalization
    """
    text = unicodedata.normalize('NFC', text)
    text = text.strip()
    # Remove trailing parenthetical if it's an expansion
    # But keep parenthetical if it's part of the name: "ABM (Anti-ballistic Missile)"
    # We keep both forms for matching
    text = re.sub(r'\s+', ' ', text)
    text = text.strip('.,;: ')
    return text


def normalize_for_matching(text):
    """Aggressively normalize for fuzzy matching."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


# ── Taxonomy loading ──────────────────────────────────────────────

def load_taxonomy_terms(taxonomy_path):
    """Load all subject names from the taxonomy XML.

    Returns:
      - exact_names: set of lowercase exact names
      - normalized_names: set of aggressively normalized names
      - name_list: list of original names
    """
    tree = etree.parse(taxonomy_path)
    root = tree.getroot()

    names = root.xpath('//subject/name/text()')

    exact_names = set()
    normalized_names = set()
    for n in names:
        exact_names.add(n.lower().strip())
        normalized_names.add(normalize_for_matching(n))

    return exact_names, normalized_names, names


# ── Main pipeline ─────────────────────────────────────────────────

def discover_candidates(volumes_dir, taxonomy_path):
    """Run the full discovery pipeline.

    Returns:
      - candidates: list of candidate terms not in taxonomy
      - stats: summary statistics
    """
    print("Loading taxonomy...")
    exact_names, norm_names, all_names = load_taxonomy_terms(taxonomy_path)
    print(f"  {len(all_names)} taxonomy subjects loaded")

    # Find volumes with indexes — excluding already-annotated ones and slugs
    import glob

    # Build set of volume IDs already in the annotation pipeline
    annotated_ids = set()
    ann_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'volumes')
    for f in glob.glob(os.path.join(ann_dir, 'frus*.xml')):
        vid = os.path.basename(f).replace('-annotated.xml', '').replace('.xml', '')
        annotated_ids.add(vid)
    print(f"  {len(annotated_ids)} already-annotated volumes (excluded)")

    vol_files = sorted(glob.glob(os.path.join(volumes_dir, 'frus*.xml')))
    vol_files = [v for v in vol_files if 'annotated' not in v]

    # Filter out annotated volumes and unpublished slugs (tiny files)
    filtered = []
    skipped_annotated = 0
    skipped_slugs = 0
    for vf in vol_files:
        vid = os.path.basename(vf).replace('.xml', '')
        if vid in annotated_ids:
            skipped_annotated += 1
            continue
        if os.path.getsize(vf) < MIN_VOLUME_SIZE:
            skipped_slugs += 1
            continue
        filtered.append(vf)
    vol_files = filtered
    print(f"  {skipped_annotated} skipped (already annotated)")
    print(f"  {skipped_slugs} skipped (unpublished slugs)")
    print(f"  {len(vol_files)} volumes to scan")

    all_entries = []
    volumes_with_index = 0
    volumes_without_index = 0

    for vf in vol_files:
        vol_id = os.path.basename(vf).replace('.xml', '')
        entries = parse_index(vf)
        if entries:
            volumes_with_index += 1
            all_entries.extend(entries)
            print(f"  {vol_id}: {len(entries)} main entries "
                  f"({sum(1 for e in entries if not e['is_person'])} non-person)")
        else:
            volumes_without_index += 1

    print(f"\nTotal: {len(all_entries)} index entries from {volumes_with_index} volumes "
          f"({volumes_without_index} without index)")

    # Aggregate by normalized heading
    term_occurrences = defaultdict(lambda: {
        'heading': '',
        'volumes': set(),
        'doc_refs': set(),
        'is_person': True,  # Start True, any non-person vote flips it
        'is_see_ref': False,
        'sub_entries': [],
        'raw_headings': set(),
    })

    for entry in all_entries:
        key = normalize_for_matching(entry['heading'])
        if not key:
            continue

        occ = term_occurrences[key]
        occ['heading'] = entry['heading']  # Keep last seen form
        occ['raw_headings'].add(entry['heading'])
        occ['volumes'].add(entry['volume'])
        occ['doc_refs'].update(entry['doc_refs'])
        if not entry['is_person']:
            occ['is_person'] = False
        if entry['is_see_ref']:
            occ['is_see_ref'] = True
        occ['sub_entries'].extend(entry['sub_entries'][:3])

    print(f"\n{len(term_occurrences)} unique normalized headings")

    # Classify
    persons = {k: v for k, v in term_occurrences.items() if v['is_person']}
    topics = {k: v for k, v in term_occurrences.items() if not v['is_person']}
    see_refs = {k: v for k, v in term_occurrences.items() if v['is_see_ref']}

    print(f"  {len(persons)} person entries")
    print(f"  {len(topics)} topic/subject entries")
    print(f"  {len(see_refs)} cross-references")

    # Diff topics against taxonomy
    in_taxonomy = {}
    not_in_taxonomy = {}

    def check_in_taxonomy(text):
        """Check if a text matches any taxonomy entry."""
        if text.lower().strip() in exact_names:
            return True
        if normalize_for_matching(text) in norm_names:
            return True
        # Also try stripping parenthetical: "National Security Council (NSC)" → "National Security Council"
        stripped = re.sub(r'\s*\([^)]*\)\s*$', '', text).strip()
        if stripped != text:
            if stripped.lower() in exact_names:
                return True
            if normalize_for_matching(stripped) in norm_names:
                return True
        # Try the expansion inside parenthetical: "NSC (National Security Council)" → "National Security Council"
        paren_match = re.search(r'\(([^)]+)\)$', text)
        if paren_match:
            expansion = paren_match.group(1).strip()
            if expansion.lower() in exact_names:
                return True
            if normalize_for_matching(expansion) in norm_names:
                return True
        return False

    for key, occ in topics.items():
        # Skip pure "See" cross-references with no own doc refs
        if occ['is_see_ref'] and not occ['doc_refs']:
            continue

        # Check all forms
        matched = check_in_taxonomy(occ['heading'])
        if not matched:
            for rh in occ['raw_headings']:
                if check_in_taxonomy(rh):
                    matched = True
                    break

        if matched:
            in_taxonomy[key] = occ
        else:
            not_in_taxonomy[key] = occ

    print(f"\nTopic entries already in taxonomy: {len(in_taxonomy)}")
    print(f"Candidate new terms: {len(not_in_taxonomy)}")

    # Sort candidates by number of volumes (broader coverage = higher priority)
    candidates = []
    for key, occ in not_in_taxonomy.items():
        entry_type = classify_entry_type(occ['heading'], occ['sub_entries'])
        candidates.append({
            'term': occ['heading'],
            'normalized': key,
            'type': entry_type,
            'variants': sorted(occ['raw_headings']),
            'volumes': sorted(occ['volumes']),
            'volume_count': len(occ['volumes']),
            'doc_count': len(occ['doc_refs']),
            'sample_docs': sorted(occ['doc_refs'])[:10],
            'sub_entries': occ['sub_entries'][:5],
        })

    candidates.sort(key=lambda c: (-c['volume_count'], -c['doc_count'], c['term']))

    stats = {
        'volumes_with_index': volumes_with_index,
        'volumes_without_index': volumes_without_index,
        'total_index_entries': len(all_entries),
        'unique_headings': len(term_occurrences),
        'person_entries': len(persons),
        'topic_entries': len(topics),
        'already_in_taxonomy': len(in_taxonomy),
        'candidates': len(not_in_taxonomy),
        'generated': datetime.now().isoformat(),
    }

    return candidates, stats


def write_outputs(candidates, stats, output_dir):
    """Write candidate lists for review."""
    os.makedirs(output_dir, exist_ok=True)

    # JSON output (full detail)
    json_path = os.path.join(output_dir, 'index_candidates.json')
    with open(json_path, 'w') as f:
        json.dump({
            'stats': stats,
            'candidates': candidates,
        }, f, indent=2)
    print(f"\nWrote {json_path}")

    # Human-readable text output
    txt_path = os.path.join(output_dir, 'index_candidates.txt')
    with open(txt_path, 'w') as f:
        f.write("FRUS Index Term Discovery — Candidate Terms\n")
        f.write(f"Generated: {stats['generated']}\n")
        f.write(f"Volumes scanned: {stats['volumes_with_index']} (with index)\n")
        f.write(f"Total index entries parsed: {stats['total_index_entries']}\n")
        f.write(f"Person entries (excluded): {stats['person_entries']}\n")
        f.write(f"Topic entries: {stats['topic_entries']}\n")
        f.write(f"Already in taxonomy: {stats['already_in_taxonomy']}\n")
        f.write(f"Candidates below: {stats['candidates']}\n")
        f.write("=" * 72 + "\n\n")

        # Group by type, then by volume count
        type_labels = {
            'country': 'COUNTRIES & REGIONS',
            'organization': 'ORGANIZATIONS & AGENCIES',
            'treaty': 'TREATIES & AGREEMENTS',
            'event': 'EVENTS & INCIDENTS',
            'topic': 'SUBJECT TOPICS',
        }

        for entry_type, label in type_labels.items():
            type_candidates = [c for c in candidates if c['type'] == entry_type]
            if not type_candidates:
                continue

            multi = [c for c in type_candidates if c['volume_count'] > 1]
            single = [c for c in type_candidates if c['volume_count'] == 1]

            f.write(f"\n{'=' * 72}\n")
            f.write(f"  {label} ({len(type_candidates)} candidates)\n")
            f.write(f"{'=' * 72}\n\n")

            if multi:
                f.write(f"  ── Multi-volume ({len(multi)}) ──\n\n")
                for c in multi:
                    f.write(f"    {c['term']}\n")
                    f.write(f"      Volumes ({c['volume_count']}): {', '.join(c['volumes'])}\n")
                    f.write(f"      Docs: {c['doc_count']}\n")
                    if c['sub_entries']:
                        f.write(f"      Sub-entries: {'; '.join(c['sub_entries'][:3])}\n")
                    f.write("\n")

            if single:
                f.write(f"  ── Single-volume ({len(single)}) ──\n\n")
                for c in single:
                    f.write(f"    {c['term']}  [{c['volumes'][0]}]  ({c['doc_count']} docs)\n")

    print(f"Wrote {txt_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Discover candidate taxonomy terms from FRUS back-of-book indexes'
    )
    parser.add_argument('--volumes', default=VOLUMES_DIR,
                        help='Path to volumes directory')
    parser.add_argument('--taxonomy', default=TAXONOMY_FILE,
                        help='Path to taxonomy XML')
    parser.add_argument('--output', default=OUTPUT_DIR,
                        help='Output directory')
    args = parser.parse_args()

    candidates, stats = discover_candidates(args.volumes, args.taxonomy)
    write_outputs(candidates, stats, args.output)

    # Print top candidates
    print("\n── Top 20 candidates (by volume coverage) ──")
    for c in candidates[:20]:
        print(f"  [{c['volume_count']}v, {c['doc_count']}d] {c['term']}")


if __name__ == '__main__':
    main()
