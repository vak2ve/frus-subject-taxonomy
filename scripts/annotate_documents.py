#!/usr/bin/env python3
"""
Annotate FRUS documents by exact string matching against taxonomy terms.

For each document, finds all taxonomy terms that appear verbatim in the
document body (case-insensitive, word-boundary matching). Uses a
longest-match-first algorithm so that shorter terms don't match inside
longer ones (e.g., "National Security" won't match where
"National Security Council" already matched).

Searches document body only — excludes headers, source notes, and footnotes.
"""

import json
import os
import re
import sys
import glob
from datetime import datetime
from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

TEI_NS = "http://www.tei-c.org/ns/1.0"
NS = {"tei": TEI_NS}
TAXONOMY_FILE = "../subject-taxonomy-lcsh.xml"
LCSH_MAPPING_FILE = "../config/lcsh_mapping.json"
STOPLIST_FILE = "../config/annotation_stoplist.json"
VARIANT_GROUPS_FILE = "../variant_groups.json"
VARIANT_OVERRIDES_FILE = "../config/variant_overrides.json"
MIN_TERM_LENGTH = 3  # Exclude 1-2 char terms (IV, MI, WG) to avoid false positives

# Tags to skip entirely (their text and children are excluded, but .tail is kept)
SKIP_TAGS = {f"{{{TEI_NS}}}head", f"{{{TEI_NS}}}note"}


# ── Taxonomy loading ──────────────────────────────────────────

def load_stoplist(path):
    """Load stoplisted refs from annotation_stoplist.json.

    Returns: set of ref strings to exclude from matching.
    """
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        data = json.load(f)
    return {entry["ref"] for entry in data.get("stoplist", [])}


def load_variant_groups(groups_path):
    """Load variant groups from variant_groups.json.

    Returns:
        ref_to_canonical: dict mapping variant ref -> canonical ref
        canonical_info: dict mapping canonical_ref -> {
            canonical_name, variant_names, variant_refs, search_names
        }
    """
    if not os.path.exists(groups_path):
        return {}, {}

    with open(groups_path) as f:
        data = json.load(f)

    ref_to_canonical = data.get("ref_to_canonical", {})

    canonical_info = {}
    for g in data.get("groups", []):
        cref = g["canonical_ref"]
        canonical_info[cref] = {
            "canonical_name": g["canonical_name"],
            "variant_refs": g.get("variant_refs", []),
            "search_names": g.get("search_names", []),
            "variant_names": [sn["name"] for sn in g.get("search_names", [])],
        }

    return ref_to_canonical, canonical_info


def load_lcsh_mapping(path):
    """Load LCSH mapping data to supplement taxonomy terms.

    Returns: {ref: {lcsh_uri, lcsh_label, match_quality}} for refs with LCSH data.
    """
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        data = json.load(f)
    result = {}
    for ref, entry in data.items():
        uri = entry.get("lcsh_uri")
        if uri:
            result[ref] = {
                "lcsh_uri": uri,
                "lcsh_label": entry.get("lcsh_label", ""),
                "match_quality": entry.get("match_quality", ""),
            }
    return result


def load_taxonomy(path, stoplist=None):
    """Load all subject terms from the taxonomy XML.

    Args:
        path: path to taxonomy XML
        stoplist: optional set of refs to exclude

    Returns: list of dicts sorted by term length (longest first):
        [{term, ref, type, count, category, subcategory, lcsh_uri, lcsh_match}]
    """
    tree = etree.parse(path)
    root = tree.getroot()
    terms = []
    skipped = 0

    for cat_elem in root.findall("category"):
        cat_label = cat_elem.get("label", "Uncategorized")
        for sub_elem in cat_elem.findall("subcategory"):
            sub_label = sub_elem.get("label", "General")
            for subj in sub_elem.findall("subject"):
                name_el = subj.find("name")
                if name_el is None or not name_el.text:
                    continue
                term_text = name_el.text.strip()
                if len(term_text) < MIN_TERM_LENGTH:
                    continue

                ref = subj.get("ref", "")
                if stoplist and ref in stoplist:
                    skipped += 1
                    continue

                terms.append({
                    "term": term_text,
                    "ref": ref,
                    "type": subj.get("type", "topic"),
                    "count": int(subj.get("count", "0")),
                    "category": cat_label,
                    "subcategory": sub_label,
                    "lcsh_uri": subj.get("lcsh-uri", ""),
                    "lcsh_match": subj.get("lcsh-match", ""),
                })

    if skipped:
        print(f"  Skipped {skipped} stoplisted terms")

    # Sort longest first for longest-match-wins algorithm
    terms.sort(key=lambda t: len(t["term"]), reverse=True)
    return terms


def expand_terms_with_variants(terms, canonical_info, ref_to_canonical, stoplist=None):
    """Add variant name forms from dedup groups as additional search terms.

    For variant names not already in the taxonomy (in_taxonomy=false),
    creates synthetic term entries that search for the variant name but
    attribute matches to the canonical ref.

    Returns: expanded terms list, re-sorted by length (longest first).
    """
    # Build set of existing term names (lowercase) for dedup
    existing_names = {t["term"].lower() for t in terms}
    # Build ref -> term data for looking up canonical info
    ref_to_term = {t["ref"]: t for t in terms}

    added = 0
    for cref, info in canonical_info.items():
        canonical_term = ref_to_term.get(cref)
        if not canonical_term:
            continue  # Canonical not in taxonomy (shouldn't happen)
        if stoplist and cref in stoplist:
            continue  # Canonical is stoplisted

        for sn in info["search_names"]:
            name = sn["name"]
            # Skip if already in taxonomy terms
            if name.lower() in existing_names:
                continue
            if len(name) < MIN_TERM_LENGTH:
                continue
            if stoplist and sn.get("ref", "") in stoplist:
                continue

            # Create synthetic term entry attributed to canonical
            terms.append({
                "term": name,
                "ref": cref,  # Attribute to canonical
                "type": canonical_term["type"],
                "count": canonical_term["count"],
                "category": canonical_term["category"],
                "subcategory": canonical_term["subcategory"],
                "lcsh_uri": canonical_term["lcsh_uri"],
                "lcsh_match": canonical_term.get("lcsh_match", ""),
                "is_variant_form": True,
                "original_variant_ref": sn.get("ref", ""),
            })
            existing_names.add(name.lower())
            added += 1

    if added:
        print(f"  Added {added} variant name forms as search terms")

    # Re-sort longest first
    terms.sort(key=lambda t: len(t["term"]), reverse=True)
    return terms


# ── Compile term patterns ─────────────────────────────────────

def compile_term_patterns(terms):
    """Compile regex patterns for each term with word boundaries.

    Uses (?<!\\w) and (?!\\w) for clean boundary matching with
    hyphens, en-dashes, periods, parentheses, etc.
    """
    compiled = []
    for t in terms:
        try:
            pattern = re.compile(
                r"(?<!\w)" + re.escape(t["term"]) + r"(?!\w)",
                re.IGNORECASE,
            )
            compiled.append((t, pattern))
        except re.error as e:
            print(f"  WARNING: Could not compile pattern for '{t['term']}': {e}")
    return compiled


# ── TEI text extraction ───────────────────────────────────────

def extract_body_text(doc_path):
    """Extract plain text from document body, excluding heads and notes.

    Returns: (full_text, paragraphs)
        full_text: single normalized string of all body text
        paragraphs: list of {text, start, end} for sentence extraction
    """
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(doc_path, parser)
    root = tree.getroot()
    body = root.find(f".//{{{TEI_NS}}}text/{{{TEI_NS}}}body")
    if body is None:
        return "", []

    # Collect text fragments, tracking paragraph boundaries
    paragraphs = []
    current_para_parts = []

    def flush_para():
        if current_para_parts:
            text = " ".join(current_para_parts)
            text = re.sub(r"\s+", " ", text).strip()
            if text:
                paragraphs.append(text)
            current_para_parts.clear()

    def walk(elem, in_block=False):
        """Recursively walk the element tree, collecting text."""
        tag = elem.tag

        # Skip head and note elements entirely (but collect their tail)
        if tag in SKIP_TAGS:
            if elem.tail:
                tail = elem.tail.strip()
                if tail:
                    current_para_parts.append(tail)
            return

        # Block-level elements start a new paragraph
        is_block = tag in {
            f"{{{TEI_NS}}}p",
            f"{{{TEI_NS}}}opener",
            f"{{{TEI_NS}}}closer",
            f"{{{TEI_NS}}}item",
            f"{{{TEI_NS}}}salute",
            f"{{{TEI_NS}}}signed",
        }

        if is_block and in_block:
            flush_para()

        # Collect this element's text
        if elem.text:
            text = elem.text.strip()
            if text:
                current_para_parts.append(text)

        # Recurse into children
        for child in elem:
            walk(child, in_block=True)

        if is_block:
            flush_para()

        # Collect tail text (belongs to parent context)
        if elem.tail:
            tail = elem.tail.strip()
            if tail:
                current_para_parts.append(tail)

    walk(body)
    flush_para()

    # Build full text with paragraph separators
    full_text = " ".join(paragraphs)
    full_text = re.sub(r"\s+", " ", full_text).strip()

    return full_text, paragraphs


def extract_doc_metadata(doc_path):
    """Extract title, date, and document type from a split TEI document.

    Split documents have no teiHeader; metadata lives in the document div:
      - Title: <head> text content, excluding <note> children
      - Date: frus:doc-dateTime-min attribute on the <div>
      - Doc type: subtype attribute on the <div>
    """
    FRUS_NS = "http://history.state.gov/frus/ns/1.0"
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(doc_path, parser)
    root = tree.getroot()

    # Find the document div
    doc_div = root.find(f".//{{{TEI_NS}}}div[@type='document']")
    if doc_div is None:
        return {"title": "", "date": "", "type": ""}

    # Title: <head> text, excluding <note> children
    head = doc_div.find(f"{{{TEI_NS}}}head")
    if head is not None:
        # Get text from head but skip note elements
        parts = []
        if head.text:
            parts.append(head.text)
        for child in head:
            tag = child.tag if isinstance(child.tag, str) else ""
            if f"{{{TEI_NS}}}note" not in tag:
                # Include this element's text content
                parts.append(etree.tostring(child, method="text", encoding="unicode"))
            # Always skip tail of note elements, include tail of others
            if f"{{{TEI_NS}}}note" not in tag and child.tail:
                parts.append(child.tail)
        title = re.sub(r"\s+", " ", "".join(parts)).strip()
        # Strip leading document number (e.g., "5. " or "123. ")
        title = re.sub(r"^\d+\.\s*", "", title)
    else:
        title = ""

    # Date: from frus:doc-dateTime-min attribute
    date_min = doc_div.get(f"{{{FRUS_NS}}}doc-dateTime-min", "")
    if date_min:
        # Parse ISO datetime to a readable date string
        try:
            from datetime import datetime as dt
            date_obj = dt.fromisoformat(date_min)
            date_text = date_obj.strftime("%B %d, %Y").replace(" 0", " ")
        except (ValueError, TypeError):
            date_text = date_min[:10]  # Fall back to YYYY-MM-DD
    else:
        date_text = ""

    # Doc type: subtype attribute
    doc_type = doc_div.get("subtype", "")

    return {"title": title, "date": date_text, "type": doc_type}


# ── Sentence extraction ───────────────────────────────────────

def extract_sentence(text, match_start, match_end, max_chars=300):
    """Extract the sentence containing the match from the full text.

    Looks for sentence boundaries (. ! ? followed by space+uppercase)
    and returns the containing sentence, truncated to max_chars.
    """
    # Find sentence start: scan backward for sentence-ending punctuation
    # followed by space (or start of text)
    sent_start = 0
    for i in range(match_start - 1, -1, -1):
        if text[i] in ".!?" and i + 2 < len(text) and text[i + 1] == " " and text[i + 2].isupper():
            # Check it's not an abbreviation like "U.S." or numbered para "2."
            # Simple heuristic: if preceded by a single uppercase letter, likely abbreviation
            if i > 0 and text[i - 1].isupper() and (i < 2 or not text[i - 2].isalpha()):
                continue  # likely abbreviation like "U.S."
            if i > 0 and text[i - 1].isdigit():
                continue  # likely numbered paragraph "2."
            sent_start = i + 2
            break

    # Find sentence end: scan forward
    sent_end = len(text)
    for i in range(match_end, len(text)):
        if text[i] in ".!?" and i + 2 < len(text) and text[i + 1] == " " and text[i + 2].isupper():
            if i > 0 and text[i - 1].isupper() and (i < 2 or not text[i - 2].isalpha()):
                continue
            if i > 0 and text[i - 1].isdigit():
                continue
            sent_end = i + 1
            break

    sentence = text[sent_start:sent_end].strip()

    # Truncate if too long
    if len(sentence) > max_chars:
        # Center on the match
        mid = (match_start + match_end) // 2 - sent_start
        half = max_chars // 2
        trunc_start = max(0, mid - half)
        trunc_end = min(len(sentence), mid + half)
        sentence = ("..." if trunc_start > 0 else "") + sentence[trunc_start:trunc_end] + ("..." if trunc_end < len(sentence) else "")

    return sentence


# ── Main matching ─────────────────────────────────────────────

def match_document(text, compiled_terms, ref_to_canonical=None):
    """Find all taxonomy term matches in document text using longest-match-first.

    If ref_to_canonical is provided, remaps variant refs to their canonical ref.

    Returns: list of {term, ref, canonical_ref, matched_ref, type, category,
             subcategory, lcsh_uri, position, matched_text, sentence,
             is_variant_form, is_consolidated}
    """
    if not text:
        return []

    claimed = set()  # Character positions already matched by a longer term
    matches = []

    for term_data, pattern in compiled_terms:
        for m in pattern.finditer(text):
            start, end = m.start(), m.end()
            # Check if any position in this match is already claimed
            span = range(start, end)
            if any(pos in claimed for pos in span):
                continue

            # Claim these positions
            claimed.update(span)

            sentence = extract_sentence(text, start, end)

            # Determine canonical ref
            matched_ref = term_data["ref"]
            canonical_ref = matched_ref
            if ref_to_canonical and matched_ref in ref_to_canonical:
                canonical_ref = ref_to_canonical[matched_ref]

            is_variant = term_data.get("is_variant_form", False)
            is_consolidated = canonical_ref != matched_ref

            matches.append({
                "term": term_data["term"],
                "ref": canonical_ref,
                "canonical_ref": canonical_ref,
                "matched_ref": matched_ref,
                "type": term_data["type"],
                "category": term_data["category"],
                "subcategory": term_data["subcategory"],
                "lcsh_uri": term_data["lcsh_uri"],
                "lcsh_match": term_data.get("lcsh_match", ""),
                "position": start,
                "matched_text": m.group(),
                "sentence": sentence,
                "is_variant_form": is_variant,
                "is_consolidated": is_consolidated,
            })

    # Sort by position
    matches.sort(key=lambda x: x["position"])
    return matches


# ── Main ──────────────────────────────────────────────────────

def main():
    if len(sys.argv) > 1:
        docs_dir = sys.argv[1]
    else:
        docs_dir = os.path.join("..", "volumes", "frus1969-76v19p2")

    if not os.path.isdir(docs_dir):
        print(f"ERROR: Documents directory not found: {docs_dir}")
        sys.exit(1)

    # Derive volume ID from directory name
    volume_id = os.path.basename(os.path.normpath(docs_dir))

    print(f"Annotating volume: {volume_id}")
    print(f"Documents dir: {docs_dir}")
    print()

    # 1. Load stoplist
    print("Loading stoplist...")
    stoplist = load_stoplist(STOPLIST_FILE)
    if stoplist:
        print(f"  {len(stoplist)} terms in stoplist")
    else:
        print("  No stoplist file found (all terms included)")

    # 2. Load variant groups
    print("\nLoading variant groups...")
    ref_to_canonical, canonical_info = load_variant_groups(VARIANT_GROUPS_FILE)
    if ref_to_canonical:
        print(f"  {len(canonical_info)} variant groups, {len(ref_to_canonical)} refs mapped")
    else:
        print("  No variant groups file found (no consolidation)")

    # 3. Load LCSH mapping (to supplement taxonomy XML)
    print("\nLoading LCSH mapping...")
    lcsh_mapping = load_lcsh_mapping(LCSH_MAPPING_FILE)
    if lcsh_mapping:
        print(f"  {len(lcsh_mapping)} refs with LCSH data")
    else:
        print("  No LCSH mapping file found")

    # 4. Load taxonomy
    print("\nLoading taxonomy...")
    terms = load_taxonomy(TAXONOMY_FILE, stoplist=stoplist)
    print(f"  Loaded {len(terms)} terms (min length: {MIN_TERM_LENGTH})")

    # Supplement LCSH data from lcsh_mapping.json for terms missing it in XML
    supplemented = 0
    for t in terms:
        ref = t["ref"]
        if ref in lcsh_mapping:
            lm = lcsh_mapping[ref]
            if not t["lcsh_uri"] and lm["lcsh_uri"]:
                t["lcsh_uri"] = lm["lcsh_uri"]
                t["lcsh_match"] = lm["match_quality"]
                supplemented += 1
            elif t["lcsh_uri"] and not t["lcsh_match"]:
                # Has URI from XML but missing match quality
                t["lcsh_match"] = lm.get("match_quality", "")
    if supplemented:
        print(f"  Supplemented {supplemented} terms with LCSH data from lcsh_mapping.json")

    # 4. Expand with variant name forms
    if canonical_info:
        print("\nExpanding with variant name forms...")
        terms = expand_terms_with_variants(terms, canonical_info, ref_to_canonical, stoplist)

    print(f"  Total search terms: {len(terms)}")
    print(f"  Longest: '{terms[0]['term']}' ({len(terms[0]['term'])} chars)")
    print(f"  Shortest: '{terms[-1]['term']}' ({len(terms[-1]['term'])} chars)")

    # 5. Compile patterns
    print("\nCompiling regex patterns...")
    compiled = compile_term_patterns(terms)
    print(f"  Compiled {len(compiled)} patterns")

    # 3. Find all documents
    doc_files = sorted(glob.glob(os.path.join(docs_dir, "d*.xml")))
    print(f"\nFound {len(doc_files)} documents")

    # 4. Process each document
    print("\nAnnotating documents...")
    by_document = {}
    by_term = {}  # ref -> {term info + documents + total_occurrences}
    all_matched_refs = set()
    total_matches = 0

    for i, doc_path in enumerate(doc_files):
        doc_id = os.path.splitext(os.path.basename(doc_path))[0]
        metadata = extract_doc_metadata(doc_path)
        text, paragraphs = extract_body_text(doc_path)

        matches = match_document(text, compiled, ref_to_canonical)
        total_matches += len(matches)

        # Track unique terms matched (using canonical ref)
        for m in matches:
            cref = m["canonical_ref"]
            all_matched_refs.add(cref)
            if cref not in by_term:
                by_term[cref] = {
                    "term": m["term"],
                    "type": m["type"],
                    "category": m["category"],
                    "subcategory": m["subcategory"],
                    "lcsh_uri": m["lcsh_uri"],
                    "lcsh_match": m.get("lcsh_match", ""),
                    "documents": {},
                    "total_occurrences": 0,
                    "variant_names": [],
                    "variant_refs": [],
                }
                # Add variant info if this is part of a group
                if cref in canonical_info:
                    by_term[cref]["variant_names"] = canonical_info[cref]["variant_names"]
                    by_term[cref]["variant_refs"] = canonical_info[cref]["variant_refs"]
            bt = by_term[cref]
            bt["total_occurrences"] += 1
            if doc_id not in bt["documents"]:
                bt["documents"][doc_id] = []
            bt["documents"][doc_id].append({
                "sentence": m["sentence"],
                "matched_text": m["matched_text"],
                "position": m["position"],
                "matched_ref": m["matched_ref"],
                "is_consolidated": m["is_consolidated"],
                "is_variant_form": m["is_variant_form"],
            })

        by_document[doc_id] = {
            "title": metadata["title"],
            "date": metadata["date"],
            "doc_type": metadata["type"],
            "match_count": len(matches),
            "unique_terms": len(set(m["ref"] for m in matches)),
            "matches": matches,
            "body_length": len(text),
        }

        if (i + 1) % 20 == 0 or i == len(doc_files) - 1:
            print(f"  Processed {i + 1}/{len(doc_files)} documents, {total_matches} matches so far")

    # Build unmatched terms list (only from original taxonomy terms, not synthetic variants)
    all_refs = {t["ref"] for t in terms if not t.get("is_variant_form")}
    unmatched_refs = all_refs - all_matched_refs
    unmatched_terms = [
        {"term": t["term"], "ref": t["ref"], "category": t["category"], "subcategory": t["subcategory"]}
        for t in terms if t["ref"] in unmatched_refs
    ]
    # Sort alphabetically
    unmatched_terms.sort(key=lambda x: x["term"].lower())

    # Convert by_term documents dict to sorted list for JSON
    for ref, bt in by_term.items():
        bt["document_count"] = len(bt["documents"])

    # Count consolidated matches
    consolidated_count = sum(
        1 for d in by_document.values()
        for m in d["matches"]
        if m.get("is_consolidated")
    )

    # Build results
    taxonomy_term_count = len([t for t in terms if not t.get("is_variant_form")])
    variant_term_count = len([t for t in terms if t.get("is_variant_form")])
    results = {
        "metadata": {
            "volume_id": volume_id,
            "generated": datetime.now().isoformat(),
            "total_documents": len(doc_files),
            "documents_with_matches": sum(1 for d in by_document.values() if d["match_count"] > 0),
            "total_matches": total_matches,
            "unique_terms_matched": len(all_matched_refs),
            "total_terms_searched": taxonomy_term_count,
            "terms_not_matched": len(unmatched_refs),
            "min_term_length": MIN_TERM_LENGTH,
            "stoplist_applied": bool(stoplist),
            "stoplisted_terms": len(stoplist),
            "variant_consolidation_applied": bool(ref_to_canonical),
            "variant_groups_count": len(canonical_info),
            "variant_names_added": variant_term_count,
            "consolidated_matches": consolidated_count,
        },
        "by_document": by_document,
        "by_term": by_term,
        "unmatched_terms": unmatched_terms,
    }

    # 7. Write output
    output_dir = f"../data/documents/{volume_id}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/string_match_results_{volume_id}.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\n{'=' * 60}")
    print(f"Results written to: {output_path}")
    print(f"  Total documents: {len(doc_files)}")
    print(f"  Documents with matches: {results['metadata']['documents_with_matches']}")
    print(f"  Total matches: {total_matches}")
    print(f"  Unique terms matched: {len(all_matched_refs)} / {len(terms)}")
    print(f"  Terms not matched: {len(unmatched_refs)}")

    # Top matched terms
    top_terms = sorted(by_term.items(), key=lambda x: x[1]["total_occurrences"], reverse=True)[:15]
    print(f"\nTop 15 matched terms:")
    for ref, bt in top_terms:
        print(f"  {bt['term']}: {bt['total_occurrences']} occurrences in {bt['document_count']} docs")

    # Top documents by match count
    top_docs = sorted(by_document.items(), key=lambda x: x[1]["match_count"], reverse=True)[:10]
    print(f"\nTop 10 documents by match count:")
    for doc_id, dd in top_docs:
        print(f"  {doc_id}: {dd['match_count']} matches - {dd['title'][:60]}")


if __name__ == "__main__":
    main()
