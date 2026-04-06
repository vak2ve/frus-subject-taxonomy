#!/usr/bin/env python3
"""
Annotate FRUS documents by exact string matching against taxonomy terms.

For each document, finds all taxonomy terms that appear verbatim in the
document body (case-insensitive, word-boundary matching). Uses a
longest-match-first algorithm so that shorter terms don't match inside
longer ones (e.g., "National Security" won't match where
"National Security Council" already matched).

Searches document body only — excludes headers, source notes, and footnotes.

Usage:
    # Annotate a single volume
    python3 annotate_documents.py ../data/documents/frus1969-76v19p2

    # Annotate all volumes (skips those with existing results)
    python3 annotate_documents.py --all

    # Force re-annotation of all volumes
    python3 annotate_documents.py --all --force

    # Annotate specific volume(s)
    python3 annotate_documents.py --volume frus1969-76v19p2 --volume frus1981-88v10

    # Use parallel processing
    python3 annotate_documents.py --all --workers 4

    # Dry run: show what would be processed
    python3 annotate_documents.py --all --dry-run
"""

import argparse
import json
import os
import re
import sys
import glob
import multiprocessing
from datetime import datetime
from pathlib import Path
from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

TEI_NS = "http://www.tei-c.org/ns/1.0"
NS = {"tei": TEI_NS}
TAXONOMY_FILE = "../subject-taxonomy-lcsh.xml"
LCSH_MAPPING_FILE = "../config/lcsh_mapping.json"
STOPLIST_FILE = "../config/annotation_stoplist.json"
VARIANT_GROUPS_FILE = "../variant_groups.json"
VARIANT_OVERRIDES_FILE = "../config/variant_overrides.json"
DOCUMENTS_DIR = "../data/documents"
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
    """Compile a single combined regex for all terms (longest-first alternation).

    Uses (?<!\\w) and (?!\\w) for clean boundary matching with
    hyphens, en-dashes, periods, parentheses, etc.

    Returns: (combined_pattern, term_lookup)
        combined_pattern: compiled regex with all terms as alternations
        term_lookup: dict mapping lowercased term text -> term data
    """
    # Build lookup from lowercased term text to term data (first match wins = longest)
    term_lookup = {}
    unique_escaped = []
    seen_lower = set()

    for t in terms:  # already sorted longest-first
        key = t["term"].lower()
        if key not in seen_lower:
            seen_lower.add(key)
            term_lookup[key] = t
            try:
                unique_escaped.append(re.escape(t["term"]))
            except re.error as e:
                print(f"  WARNING: Could not escape term '{t['term']}': {e}")

    if not unique_escaped:
        empty = re.compile(r"(?!)")  # matches nothing
        return empty, term_lookup

    combined = re.compile(
        r"(?<!\w)(" + "|".join(unique_escaped) + r")(?!\w)",
        re.IGNORECASE,
    )
    return combined, term_lookup


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
            if not isinstance(child.tag, str):
                # Skip comments/processing instructions, but keep tail text
                if child.tail:
                    parts.append(child.tail)
                continue
            if f"{{{TEI_NS}}}note" not in child.tag:
                # Include this element's full text (itertext excludes tail)
                parts.append("".join(child.itertext()))
                if child.tail:
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

    compiled_terms is a (combined_pattern, term_lookup) tuple from compile_term_patterns.
    If ref_to_canonical is provided, remaps variant refs to their canonical ref.

    Returns: list of {term, ref, canonical_ref, matched_ref, type, category,
             subcategory, lcsh_uri, position, matched_text, sentence,
             is_variant_form, is_consolidated}
    """
    if not text:
        return []

    combined_pattern, term_lookup = compiled_terms

    # Collect all raw matches from the combined regex
    raw_matches = []
    for m in combined_pattern.finditer(text):
        raw_matches.append((m.start(), m.end(), m.group()))

    # Sort by length descending (longest first), then by position
    raw_matches.sort(key=lambda x: (-(x[1] - x[0]), x[0]))

    # Resolve overlaps: longest match wins
    claimed = set()
    matches = []

    for start, end, matched_text in raw_matches:
        # Check if any position in this match is already claimed
        if any(pos in claimed for pos in range(start, end)):
            continue

        # Claim these positions
        claimed.update(range(start, end))

        # Look up term data
        term_data = term_lookup.get(matched_text.lower())
        if not term_data:
            continue

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
            "matched_text": matched_text,
            "sentence": sentence,
            "is_variant_form": is_variant,
            "is_consolidated": is_consolidated,
        })

    # Sort by position
    matches.sort(key=lambda x: x["position"])
    return matches


# ── Shared annotation resources ───────────────────────────────

def load_annotation_resources(quiet=False):
    """Load all shared resources needed for annotation.

    Loads taxonomy, stoplist, variant groups, LCSH mapping, and compiles
    the regex pattern. This is expensive (~2-5s) so should be done once
    and reused across volumes.

    Returns: dict with keys:
        compiled, terms, ref_to_canonical, canonical_info, stoplist
    """
    log = (lambda *a: None) if quiet else print

    log("Loading annotation resources...")

    # 1. Stoplist
    stoplist = load_stoplist(STOPLIST_FILE)
    log(f"  Stoplist: {len(stoplist)} terms")

    # 2. Variant groups
    ref_to_canonical, canonical_info = load_variant_groups(VARIANT_GROUPS_FILE)
    log(f"  Variant groups: {len(canonical_info)}")

    # 3. LCSH mapping
    lcsh_mapping = load_lcsh_mapping(LCSH_MAPPING_FILE)
    log(f"  LCSH mapping: {len(lcsh_mapping)} refs")

    # 4. Taxonomy
    terms = load_taxonomy(TAXONOMY_FILE, stoplist=stoplist)

    # Supplement LCSH data
    for t in terms:
        ref = t["ref"]
        if ref in lcsh_mapping:
            lm = lcsh_mapping[ref]
            if not t["lcsh_uri"] and lm["lcsh_uri"]:
                t["lcsh_uri"] = lm["lcsh_uri"]
                t["lcsh_match"] = lm["match_quality"]
            elif t["lcsh_uri"] and not t["lcsh_match"]:
                t["lcsh_match"] = lm.get("match_quality", "")

    # 5. Expand with variants
    if canonical_info:
        terms = expand_terms_with_variants(terms, canonical_info, ref_to_canonical, stoplist)

    log(f"  Total search terms: {len(terms)}")

    # 6. Compile regex
    compiled = compile_term_patterns(terms)
    _, term_lookup = compiled
    log(f"  Compiled {len(term_lookup)} unique terms into regex")

    return {
        "compiled": compiled,
        "terms": terms,
        "ref_to_canonical": ref_to_canonical,
        "canonical_info": canonical_info,
        "stoplist": stoplist,
    }


# ── Per-volume annotation ────────────────────────────────────

def annotate_volume(volume_id, resources, docs_dir=None, quiet=False):
    """Annotate all documents in a volume.

    Args:
        volume_id: e.g. 'frus1969-76v19p2'
        resources: dict from load_annotation_resources()
        docs_dir: optional override for document directory path
        quiet: suppress progress output

    Returns: results dict (same structure as string_match_results JSON)
    """
    log = (lambda *a: None) if quiet else print

    if docs_dir is None:
        docs_dir = os.path.join(DOCUMENTS_DIR, volume_id)

    compiled = resources["compiled"]
    terms = resources["terms"]
    ref_to_canonical = resources["ref_to_canonical"]
    canonical_info = resources["canonical_info"]

    doc_files = sorted(glob.glob(os.path.join(docs_dir, "d*.xml")))
    if not doc_files:
        log(f"  WARNING: No document files found in {docs_dir}")
        return None

    log(f"  Annotating {len(doc_files)} documents...")

    by_document = {}
    by_term = {}
    all_matched_refs = set()
    total_matches = 0

    for i, doc_path in enumerate(doc_files):
        doc_id = os.path.splitext(os.path.basename(doc_path))[0]
        metadata = extract_doc_metadata(doc_path)
        text, paragraphs = extract_body_text(doc_path)

        matches = match_document(text, compiled, ref_to_canonical)
        total_matches += len(matches)

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

        if not quiet and ((i + 1) % 50 == 0 or i == len(doc_files) - 1):
            log(f"    {i + 1}/{len(doc_files)} docs, {total_matches} matches")

    # Build unmatched terms list
    all_refs = {t["ref"] for t in terms if not t.get("is_variant_form")}
    unmatched_refs = all_refs - all_matched_refs
    unmatched_terms = [
        {"term": t["term"], "ref": t["ref"], "category": t["category"], "subcategory": t["subcategory"]}
        for t in terms if t["ref"] in unmatched_refs
    ]
    unmatched_terms.sort(key=lambda x: x["term"].lower())

    for ref, bt in by_term.items():
        bt["document_count"] = len(bt["documents"])

    consolidated_count = sum(
        1 for d in by_document.values()
        for m in d["matches"]
        if m.get("is_consolidated")
    )

    taxonomy_term_count = len([t for t in terms if not t.get("is_variant_form")])
    variant_term_count = len([t for t in terms if t.get("is_variant_form")])
    stoplist = resources["stoplist"]

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

    return results


def write_results(volume_id, results):
    """Write annotation results to the standard output path."""
    output_dir = os.path.join(DOCUMENTS_DIR, volume_id)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"string_match_results_{volume_id}.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    return output_path


def results_exist(volume_id):
    """Check whether annotation results already exist for a volume."""
    return os.path.exists(
        os.path.join(DOCUMENTS_DIR, volume_id, f"string_match_results_{volume_id}.json")
    )


def has_documents(volume_id):
    """Check whether a volume has split document files."""
    doc_dir = os.path.join(DOCUMENTS_DIR, volume_id)
    return os.path.isdir(doc_dir) and bool(glob.glob(os.path.join(doc_dir, "d*.xml")))


# ── Volume discovery ─────────────────────────────────────────

def discover_volumes():
    """Find all volume IDs that have split documents.

    Returns: sorted list of volume ID strings.
    """
    docs_base = Path(DOCUMENTS_DIR)
    if not docs_base.exists():
        return []

    volumes = []
    for d in sorted(docs_base.iterdir()):
        if d.is_dir() and any(d.glob("d*.xml")):
            volumes.append(d.name)
    return volumes


# ── Worker for parallel processing ────────────────────────────

def _annotate_worker(args):
    """Worker function for multiprocessing.

    Takes (volume_id, resources_are_preloaded) and returns
    (volume_id, success, message).

    Note: resources must be loaded in each worker process because
    compiled regex objects can't be pickled across processes.
    """
    volume_id = args
    try:
        resources = load_annotation_resources(quiet=True)
        results = annotate_volume(volume_id, resources, quiet=True)
        if results is None:
            return (volume_id, False, "No documents found")
        output_path = write_results(volume_id, results)
        meta = results["metadata"]
        msg = (f"{meta['total_documents']} docs, "
               f"{meta['total_matches']} matches, "
               f"{meta['unique_terms_matched']} terms")
        return (volume_id, True, msg)
    except Exception as e:
        return (volume_id, False, str(e))


# ── CLI ───────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Annotate FRUS documents by string matching against taxonomy terms."
    )
    parser.add_argument(
        "docs_dir", nargs="?", default=None,
        help="Path to a volume's document directory (legacy single-volume mode)"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Process all volumes with split documents"
    )
    parser.add_argument(
        "--volume", action="append", dest="volumes", metavar="VOL_ID",
        help="Process specific volume(s) by ID (can be repeated)"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-annotate even if results already exist"
    )
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Number of parallel workers (default: 1, sequential)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be processed without actually running"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Determine which volumes to process
    if args.all:
        all_volumes = discover_volumes()
        if not all_volumes:
            print("No volumes with split documents found.")
            sys.exit(0)
        if args.force:
            target_volumes = all_volumes
        else:
            target_volumes = [v for v in all_volumes if not results_exist(v)]
            skipped = len(all_volumes) - len(target_volumes)
            if skipped:
                print(f"Skipping {skipped} volumes with existing results (use --force to override)")
        if not target_volumes:
            print("All volumes already annotated. Nothing to do.")
            sys.exit(0)

    elif args.volumes:
        target_volumes = []
        for vol_id in args.volumes:
            if not has_documents(vol_id):
                print(f"WARNING: No documents found for {vol_id}, skipping")
                continue
            if not args.force and results_exist(vol_id):
                print(f"Skipping {vol_id}: results exist (use --force to override)")
                continue
            target_volumes.append(vol_id)
        if not target_volumes:
            print("No volumes to process.")
            sys.exit(0)

    elif args.docs_dir:
        # Legacy single-volume mode (positional argument)
        docs_dir = args.docs_dir
        if not os.path.isdir(docs_dir):
            print(f"ERROR: Documents directory not found: {docs_dir}")
            sys.exit(1)
        volume_id = os.path.basename(os.path.normpath(docs_dir))

        if not args.force and results_exist(volume_id):
            print(f"Results already exist for {volume_id}. Use --force to re-annotate.")
            sys.exit(0)

        # Single volume: load resources once, annotate, write
        resources = load_annotation_resources()
        print(f"\nAnnotating volume: {volume_id}")
        results = annotate_volume(volume_id, resources, docs_dir=docs_dir)
        if results is None:
            print("No documents found.")
            sys.exit(1)
        output_path = write_results(volume_id, results)
        meta = results["metadata"]
        print(f"\nResults written to: {output_path}")
        print(f"  Documents: {meta['total_documents']}")
        print(f"  Matches: {meta['total_matches']}")
        print(f"  Unique terms: {meta['unique_terms_matched']}")
        sys.exit(0)

    else:
        print("Usage: annotate_documents.py [DOCS_DIR | --all | --volume VOL_ID]")
        print("  Run with --help for full options.")
        sys.exit(1)

    # Multi-volume mode
    print(f"\nVolumes to process: {len(target_volumes)}")

    if args.dry_run:
        print("\nDry run — would process:")
        for v in target_volumes:
            status = "FORCE re-annotate" if results_exist(v) else "new"
            print(f"  {v} ({status})")
        sys.exit(0)

    workers = min(args.workers, len(target_volumes))

    if workers > 1:
        # Parallel mode
        print(f"Using {workers} parallel workers")
        print("  (Each worker loads its own resources)\n")

        with multiprocessing.Pool(workers) as pool:
            results_iter = pool.imap_unordered(_annotate_worker, target_volumes)
            completed = 0
            errors = []
            for volume_id, success, msg in results_iter:
                completed += 1
                if success:
                    print(f"  [{completed}/{len(target_volumes)}] {volume_id}: {msg}")
                else:
                    print(f"  [{completed}/{len(target_volumes)}] {volume_id}: ERROR - {msg}")
                    errors.append((volume_id, msg))

    else:
        # Sequential mode: load resources once, reuse across volumes
        resources = load_annotation_resources()
        print()

        completed = 0
        errors = []
        for volume_id in target_volumes:
            completed += 1
            print(f"[{completed}/{len(target_volumes)}] {volume_id}")
            try:
                results = annotate_volume(volume_id, resources)
                if results is None:
                    print(f"  ERROR: No documents found")
                    errors.append((volume_id, "No documents found"))
                    continue
                output_path = write_results(volume_id, results)
                meta = results["metadata"]
                print(f"  Done: {meta['total_documents']} docs, "
                      f"{meta['total_matches']} matches, "
                      f"{meta['unique_terms_matched']} terms")
            except Exception as e:
                print(f"  ERROR: {e}")
                errors.append((volume_id, str(e)))

    # Summary
    print(f"\n{'=' * 60}")
    print(f"COMPLETE: {completed - len(errors)}/{len(target_volumes)} volumes annotated")
    if errors:
        print(f"\nErrors ({len(errors)}):")
        for vol_id, err in errors:
            print(f"  {vol_id}: {err}")


if __name__ == "__main__":
    main()
