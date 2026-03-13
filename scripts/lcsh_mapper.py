#!/usr/bin/env python3
"""
Map annotation-derived subjects to Library of Congress Subject Headings (LCSH).

Queries the id.loc.gov suggest2 API for each subject, retrieves the best match,
then fetches the broader terms (BT) from the LCSH hierarchy to build a
proper hierarchical taxonomy.

Outputs: lcsh_mapping.json (intermediate) and subject-taxonomy-lcsh.xml (final)
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
from lxml import etree

# ── Configuration ──────────────────────────────────────────────────────────
SUGGEST_URL = "https://id.loc.gov/authorities/subjects/suggest2"
SKOS_URL_TEMPLATE = "https://id.loc.gov/authorities/subjects/{}.skos.json"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
MAPPING_FILE = os.path.join(ROOT_DIR, "config", "lcsh_mapping.json")
INPUT_TAXONOMY = os.path.join(ROOT_DIR, "subject-taxonomy.xml")
OUTPUT_TAXONOMY = os.path.join(ROOT_DIR, "subject-taxonomy-lcsh.xml")

# Rate limiting: be polite to LOC servers
REQUEST_DELAY = 0.3  # seconds between requests


def query_lcsh_suggest(term):
    """Query the LOC suggest2 API for a subject term.
    Returns list of (label, uri) tuples."""
    params = urllib.parse.urlencode({"q": term})
    url = f"{SUGGEST_URL}?{params}"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        # suggest2 returns: {"hits": [{"uri": "...", "aLabel": "...", ...}, ...]}
        hits = data.get("hits", [])
        results = []
        for hit in hits:
            label = hit.get("aLabel", "")
            uri = hit.get("uri", "")
            if label and uri:
                results.append({"label": label, "uri": uri})
        return results
    except Exception as e:
        print(f"    API error for '{term}': {e}")
        return []


SKOS_BROADER = "http://www.w3.org/2004/02/skos/core#broader"
SKOS_PREFLABEL = "http://www.w3.org/2004/02/skos/core#prefLabel"


def fetch_skos_entry(uri, retries=3):
    """Fetch SKOS JSON-LD for a URI. Returns (label, broader_uris) tuple.
    broader_uris is a list of URI strings for broader terms."""
    lccn = uri.rstrip("/").split("/")[-1]
    skos_url = SKOS_URL_TEMPLATE.format(lccn)

    for attempt in range(retries):
        try:
            req = urllib.request.Request(skos_url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            # Find the main concept entry matching our URI
            for item in data:
                if item.get("@id", "").rstrip("/") == uri.rstrip("/"):
                    # Extract prefLabel
                    label = None
                    pref_labels = item.get(SKOS_PREFLABEL, [])
                    if isinstance(pref_labels, list):
                        for pl in pref_labels:
                            if isinstance(pl, dict):
                                label = pl.get("@value", "")
                            elif isinstance(pl, str):
                                label = pl
                            if label:
                                break
                    elif isinstance(pref_labels, dict):
                        label = pref_labels.get("@value", "")

                    # Extract broader term URIs
                    broader_uris = []
                    bt_list = item.get(SKOS_BROADER, [])
                    if not isinstance(bt_list, list):
                        bt_list = [bt_list]
                    for bt in bt_list:
                        if isinstance(bt, dict) and "@id" in bt:
                            bt_uri = bt["@id"]
                            # Only follow LCSH broader terms (not other schemes)
                            if "authorities/subjects" in bt_uri:
                                broader_uris.append(bt_uri)
                        elif isinstance(bt, str) and "authorities/subjects" in bt:
                            broader_uris.append(bt)

                    return label, broader_uris

            return None, []

        except Exception as e:
            if attempt < retries - 1:
                wait = (attempt + 1) * 2
                print(f"    Retry {attempt+1}/{retries} for {lccn}: {e} (waiting {wait}s)")
                time.sleep(wait)
            else:
                print(f"    SKOS error for {uri}: {e}")
                return None, []

    return None, []


def build_full_hierarchy(uri, cache, depth=0, max_depth=5):
    """Recursively fetch broader terms to build full hierarchy path.
    Returns list of {"label": ..., "uri": ...} from narrowest to broadest."""
    if depth >= max_depth:
        return []

    if uri in cache:
        return cache[uri]

    label, broader_uris = fetch_skos_entry(uri)
    time.sleep(REQUEST_DELAY)

    result = []
    for bt_uri in broader_uris:
        # Get the label for this broader term
        bt_label, _ = None, []
        if bt_uri in cache:
            # Already cached - just use the label from its own hierarchy chain
            pass
        else:
            bt_label, _ = fetch_skos_entry(bt_uri)
            time.sleep(REQUEST_DELAY)

        if bt_label is None and bt_uri in cache:
            # Try to get label from cached result
            bt_label = bt_uri.split("/")[-1]  # fallback to lccn

        if bt_label:
            # Get the hierarchy above this broader term
            ancestors = build_full_hierarchy(bt_uri, cache, depth + 1, max_depth)
            chain = [{"label": bt_label, "uri": bt_uri}] + ancestors
            if len(chain) > len(result):
                result = chain

    cache[uri] = result
    return result


def map_subjects_to_lcsh(subjects):
    """Map all subjects to LCSH, returning mapping dict."""
    mapping = {}
    total = len(subjects)
    matched = 0
    unmatched = 0

    # Load existing mapping if available (for resuming)
    if os.path.exists(MAPPING_FILE):
        with open(MAPPING_FILE, "r") as f:
            mapping = json.load(f)
        print(f"Loaded {len(mapping)} existing mappings from {MAPPING_FILE}")

    for i, (ref, name, stype, count, volumes, appears_in, resolved_from) in enumerate(subjects):
        if ref in mapping:
            if mapping[ref].get("lcsh_uri"):
                matched += 1
            else:
                unmatched += 1
            continue

        print(f"[{i+1}/{total}] Querying: {name}")
        results = query_lcsh_suggest(name)
        time.sleep(REQUEST_DELAY)

        if results:
            # Find best match - exact match preferred, then first result
            best = None
            for r in results:
                if r["label"].lower() == name.lower():
                    best = r
                    break
            if not best:
                # Check for close matches (case-insensitive, ignoring parenthetical qualifiers)
                for r in results:
                    clean_label = r["label"].split("(")[0].strip()
                    if clean_label.lower() == name.lower():
                        best = r
                        break
            if not best:
                best = results[0]  # Use first suggestion

            mapping[ref] = {
                "name": name,
                "type": stype,
                "count": int(count),
                "volumes": volumes,
                "appears_in": appears_in,
                "resolved_from": resolved_from,
                "lcsh_label": best["label"],
                "lcsh_uri": best["uri"],
                "exact_match": best["label"].lower() == name.lower(),
                "all_suggestions": [r["label"] for r in results[:5]],
            }
            matched += 1
            print(f"    ✓ Matched: {name} → {best['label']} ({best['uri']})")
        else:
            mapping[ref] = {
                "name": name,
                "type": stype,
                "count": int(count),
                "volumes": volumes,
                "appears_in": appears_in,
                "resolved_from": resolved_from,
                "lcsh_label": None,
                "lcsh_uri": None,
                "exact_match": False,
                "all_suggestions": [],
            }
            unmatched += 1
            print(f"    ✗ No match: {name}")

        # Save progress every 50 subjects
        if (i + 1) % 50 == 0:
            with open(MAPPING_FILE, "w") as f:
                json.dump(mapping, f, indent=2)
            print(f"  -- Progress saved: {i+1}/{total} --")

    # Final save
    with open(MAPPING_FILE, "w") as f:
        json.dump(mapping, f, indent=2)

    print(f"\nMapping complete: {matched} matched, {unmatched} unmatched out of {total}")
    return mapping


def fetch_hierarchies(mapping):
    """Fetch broader term hierarchies for all matched subjects."""
    hierarchy_cache = {}
    total_with_lcsh = sum(1 for v in mapping.values() if v.get("lcsh_uri"))
    done = 0

    for ref, data in mapping.items():
        if not data.get("lcsh_uri"):
            continue
        if "broader_terms" in data and data["broader_terms"]:
            done += 1
            # Pre-populate cache
            if data.get("lcsh_uri"):
                hierarchy_cache[data["lcsh_uri"]] = [
                    {"label": bt["label"], "uri": bt["uri"]}
                    for bt in data["broader_terms"]
                ]
            continue

        done += 1
        print(f"[{done}/{total_with_lcsh}] Hierarchy: {data['name']} ({data['lcsh_uri']})")

        broader = build_full_hierarchy(data["lcsh_uri"], hierarchy_cache)
        data["broader_terms"] = broader

        if broader:
            chain = " → ".join(bt["label"] for bt in broader)
            print(f"    BT: {chain}")
        else:
            print(f"    (no broader terms)")

        # Save progress every 50
        if done % 50 == 0:
            with open(MAPPING_FILE, "w") as f:
                json.dump(mapping, f, indent=2)
            print(f"  -- Progress saved: {done}/{total_with_lcsh} --")

    # Final save
    with open(MAPPING_FILE, "w") as f:
        json.dump(mapping, f, indent=2)

    return mapping


def build_taxonomy_xml(mapping):
    """Build hierarchical taxonomy XML based on LCSH broader terms."""

    # Determine top-level categories from LCSH broader terms
    # Strategy: use the highest-level (most general) broader term as category
    categories = {}  # top_label -> {subcategories: {sub_label -> [subjects]}}
    uncategorized = []

    for ref, data in mapping.items():
        broader = data.get("broader_terms", [])

        if broader and len(broader) >= 2:
            # Use the broadest term (last in chain) as category
            # and the next level as subcategory
            top = broader[-1]["label"]
            sub = broader[-2]["label"] if len(broader) >= 2 else broader[-1]["label"]

            if top not in categories:
                categories[top] = {"uri": broader[-1].get("uri", ""), "subcategories": {}}
            if sub not in categories[top]["subcategories"]:
                categories[top]["subcategories"][sub] = {
                    "uri": broader[-2].get("uri", "") if len(broader) >= 2 else "",
                    "subjects": []
                }
            categories[top]["subcategories"][sub]["subjects"].append((ref, data))

        elif broader and len(broader) == 1:
            # Only one broader term - use it as category, subject goes directly under it
            top = broader[0]["label"]
            if top not in categories:
                categories[top] = {"uri": broader[0].get("uri", ""), "subcategories": {}}
            # Use "General" as subcategory for direct children
            sub = "General"
            if sub not in categories[top]["subcategories"]:
                categories[top]["subcategories"][sub] = {"uri": "", "subjects": []}
            categories[top]["subcategories"][sub]["subjects"].append((ref, data))
        else:
            uncategorized.append((ref, data))

    # Merge very small categories (< 3 subjects) into nearest match or "Other"
    small_cats = [k for k, v in categories.items()
                  if sum(len(s["subjects"]) for s in v["subcategories"].values()) < 3]

    if small_cats:
        if "Other topics" not in categories:
            categories["Other topics"] = {"uri": "", "subcategories": {}}
        for cat_name in small_cats:
            cat = categories.pop(cat_name)
            for sub_name, sub_data in cat["subcategories"].items():
                merged_sub = f"{cat_name} -- {sub_name}" if sub_name != "General" else cat_name
                categories["Other topics"]["subcategories"][merged_sub] = sub_data

    # Build XML
    total_annotations = sum(int(d.get("count", 0)) for d in mapping.values())
    assigned = sum(
        int(d.get("count", 0)) for d in mapping.values()
        if d.get("broader_terms")
    )

    root = etree.Element("taxonomy", attrib={
        "source": "hsg-annotate-data",
        "authority": "Library of Congress Subject Headings (LCSH)",
        "authority-uri": "https://id.loc.gov/authorities/subjects",
        "generated": "2026-03-10",
        "total-subjects": str(len(mapping)),
        "total-annotations": str(total_annotations),
        "lcsh-matched": str(sum(1 for d in mapping.values() if d.get("lcsh_uri"))),
        "lcsh-unmatched": str(sum(1 for d in mapping.values() if not d.get("lcsh_uri"))),
        "categories": str(len(categories)),
        "uncategorized": str(len(uncategorized)),
    })

    # Sort categories by total annotation count (descending)
    sorted_cats = sorted(
        categories.items(),
        key=lambda x: sum(
            sum(int(s[1].get("count", 0)) for s in sub["subjects"])
            for sub in x[1]["subcategories"].values()
        ),
        reverse=True
    )

    for cat_name, cat_data in sorted_cats:
        cat_total = sum(
            sum(int(s[1].get("count", 0)) for s in sub["subjects"])
            for sub in cat_data["subcategories"].values()
        )
        cat_subjects = sum(len(sub["subjects"]) for sub in cat_data["subcategories"].values())

        cat_elem = etree.SubElement(root, "category", attrib={
            "label": cat_name,
            "total-annotations": str(cat_total),
            "total-subjects": str(cat_subjects),
        })
        if cat_data.get("uri"):
            cat_elem.set("lcsh-uri", cat_data["uri"])

        # Sort subcategories
        sorted_subs = sorted(
            cat_data["subcategories"].items(),
            key=lambda x: sum(int(s[1].get("count", 0)) for s in x[1]["subjects"]),
            reverse=True
        )

        for sub_name, sub_data in sorted_subs:
            sub_total = sum(int(s[1].get("count", 0)) for s in sub_data["subjects"])
            sub_elem = etree.SubElement(cat_elem, "subcategory", attrib={
                "label": sub_name,
                "total-annotations": str(sub_total),
                "total-subjects": str(len(sub_data["subjects"])),
            })
            if sub_data.get("uri"):
                sub_elem.set("lcsh-uri", sub_data["uri"])

            # Sort subjects by count
            sorted_subjects = sorted(sub_data["subjects"], key=lambda x: int(x[1].get("count", 0)), reverse=True)

            for ref, sdata in sorted_subjects:
                subj_elem = etree.SubElement(sub_elem, "subject", attrib={
                    "ref": ref,
                    "type": sdata.get("type", "topic"),
                    "count": str(sdata.get("count", 0)),
                    "volumes": str(sdata.get("volumes", "")),
                    "resolved-from": sdata.get("resolved_from", ""),
                })
                if sdata.get("lcsh_uri"):
                    subj_elem.set("lcsh-uri", sdata["lcsh_uri"])
                    subj_elem.set("lcsh-match", "exact" if sdata.get("exact_match") else "close")

                name_elem = etree.SubElement(subj_elem, "name")
                name_elem.text = sdata.get("name", "")

                if sdata.get("lcsh_label") and sdata["lcsh_label"] != sdata.get("name"):
                    lcsh_name = etree.SubElement(subj_elem, "lcsh-authorized-form")
                    lcsh_name.text = sdata["lcsh_label"]

                if sdata.get("broader_terms"):
                    bt_elem = etree.SubElement(subj_elem, "broader-terms")
                    for bt in sdata["broader_terms"]:
                        t = etree.SubElement(bt_elem, "term")
                        t.set("uri", bt.get("uri", ""))
                        t.text = bt.get("label", "")

                if sdata.get("appears_in"):
                    ai_elem = etree.SubElement(subj_elem, "appearsIn")
                    ai_elem.text = sdata["appears_in"]

    # Uncategorized
    if uncategorized:
        uncat_elem = etree.SubElement(root, "uncategorized", attrib={
            "total-annotations": str(sum(int(d.get("count", 0)) for _, d in uncategorized)),
            "total-subjects": str(len(uncategorized)),
        })
        sorted_uncat = sorted(uncategorized, key=lambda x: int(x[1].get("count", 0)), reverse=True)
        for ref, sdata in sorted_uncat:
            subj_elem = etree.SubElement(uncat_elem, "subject", attrib={
                "ref": ref,
                "type": sdata.get("type", "topic"),
                "count": str(sdata.get("count", 0)),
                "volumes": str(sdata.get("volumes", "")),
                "resolved-from": sdata.get("resolved_from", ""),
            })
            if sdata.get("lcsh_uri"):
                subj_elem.set("lcsh-uri", sdata["lcsh_uri"])

            name_elem = etree.SubElement(subj_elem, "name")
            name_elem.text = sdata.get("name", "")

            if sdata.get("appears_in"):
                ai_elem = etree.SubElement(subj_elem, "appearsIn")
                ai_elem.text = sdata["appears_in"]

    # Write XML
    tree = etree.ElementTree(root)
    etree.indent(tree, space="    ")
    tree.write(OUTPUT_TAXONOMY, xml_declaration=True, encoding="UTF-8", pretty_print=True)
    print(f"\nWrote taxonomy to {OUTPUT_TAXONOMY}")
    print(f"  Categories: {len(categories)}")
    print(f"  Uncategorized: {len(uncategorized)}")

    return OUTPUT_TAXONOMY


def extract_subjects_from_taxonomy(taxonomy_file):
    """Extract all subjects from the existing taxonomy XML."""
    tree = etree.parse(taxonomy_file)
    subjects = []
    for s in tree.xpath("//subject"):
        ref = s.get("ref", "")
        name_elem = s.find("name")
        name = name_elem.text if name_elem is not None and name_elem.text else ""
        stype = s.get("type", "topic")
        count = s.get("count", "0")
        volumes = s.get("volumes", "")
        appears_elem = s.find("appearsIn")
        appears_in = appears_elem.text if appears_elem is not None and appears_elem.text else ""
        resolved_from = s.get("resolved-from", "")
        subjects.append((ref, name, stype, count, volumes, appears_in, resolved_from))
    return subjects


if __name__ == "__main__":
    os.chdir(ROOT_DIR)

    step = sys.argv[1] if len(sys.argv) > 1 else "all"

    if step in ("map", "all"):
        print("=" * 60)
        print("STEP 1: Map subjects to LCSH")
        print("=" * 60)
        subjects = extract_subjects_from_taxonomy(INPUT_TAXONOMY)
        print(f"Extracted {len(subjects)} subjects from {INPUT_TAXONOMY}")
        mapping = map_subjects_to_lcsh(subjects)

    if step in ("hierarchy", "all"):
        print("\n" + "=" * 60)
        print("STEP 2: Fetch LCSH broader term hierarchies")
        print("=" * 60)
        if not os.path.exists(MAPPING_FILE):
            print(f"ERROR: {MAPPING_FILE} not found. Run 'map' step first.")
            sys.exit(1)
        with open(MAPPING_FILE, "r") as f:
            mapping = json.load(f)
        mapping = fetch_hierarchies(mapping)

    if step in ("xml", "all"):
        print("\n" + "=" * 60)
        print("STEP 3: Build taxonomy XML")
        print("=" * 60)
        if not os.path.exists(MAPPING_FILE):
            print(f"ERROR: {MAPPING_FILE} not found. Run 'map' step first.")
            sys.exit(1)
        with open(MAPPING_FILE, "r") as f:
            mapping = json.load(f)
        build_taxonomy_xml(mapping)
