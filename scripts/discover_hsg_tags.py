#!/usr/bin/env python3
"""
Discover subject tags from FRUS volume TEI headers and compare against
the existing taxonomy to identify gaps.

Reads <keywords scheme="https://history.state.gov/tags"> from FRUS volume
XML files and cross-references them with the current subject-taxonomy-lcsh.xml
to find tags not yet represented in the taxonomy.

Can process:
  - Local volumes in volumes/
  - Remote volumes from the public HistoryAtState/frus GitHub repo

Usage:
    python3 discover_hsg_tags.py                     # Scan local volumes
    python3 discover_hsg_tags.py --remote             # Scan all public FRUS volumes
    python3 discover_hsg_tags.py --remote --era 1940s # Scan 1940s volumes only
    python3 discover_hsg_tags.py --remote --era wwii  # Alias for 1939-1945
    python3 discover_hsg_tags.py --list-eras          # Show available eras
"""

import argparse
import glob
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from collections import defaultdict
from lxml import etree

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)

TAXONOMY_FILE = os.path.join(ROOT_DIR, "subject-taxonomy-lcsh.xml")
VOLUMES_DIR = os.path.join(ROOT_DIR, "volumes")
OUTPUT_FILE = os.path.join(ROOT_DIR, "hsg_tag_gaps.json")

TEI_NS = "http://www.tei-c.org/ns/1.0"
NSMAP = {"tei": TEI_NS}

GITHUB_RAW = "https://raw.githubusercontent.com/HistoryAtState/frus/master/volumes"
GITHUB_API = "https://api.github.com/repos/HistoryAtState/frus/contents/volumes"

# Known era aliases
ERA_ALIASES = {
    "wwi": ("1914", "1918"),
    "interwar": ("1919", "1938"),
    "wwii": ("1939", "1945"),
    "early-cold-war": ("1945", "1960"),
    "vietnam": ("1961", "1975"),
    "detente": ("1969", "1976"),
    "carter": ("1977", "1980"),
    "reagan": ("1981", "1988"),
    "post-cold-war": ("1989", "2000"),
}

# Known topic tags (the ones we care about for taxonomy gaps)
# These are HSG tags that represent subject topics, not countries or people.
# Sourced from https://history.state.gov/tags categorization.
KNOWN_TOPIC_TAGS = {
    # Governance & Policy
    "arms-control-and-disarmament", "arms-embargoes", "arms-transfers",
    "atomic-energy", "border-administration", "buildings-foreign",
    "chemical-and-bacteriological-warfare", "civil-rights",
    "civil-rights-domestic", "collective-security", "congressional-relations",
    "covert-actions", "decolonization", "department-of-state", "detainees",
    "diplomatic-recognition", "disability-rights",
    "east-asia-and-pacific", "economic-sanctions", "economic-summit-meetings",
    "elections", "energy-and-natural-resources",
    "financial-and-monetary-policy", "foreign-aid", "foreign-economic-policy",
    "foreign-investment", "foreign-protest-against-u-s-activity",
    "genocide", "global-issues", "hiv-aids", "human-rights",
    "information-programs", "intelligence", "international-law",
    "international-organizations", "law-of-the-sea", "lgbtq-rights",
    "military-bases", "military-intervention-presence-and-withdrawal",
    "national-security-council", "national-security-policy",
    "near-east", "new-international-economic-order",
    "non-governmental-organizations",
    "nuclear-nonproliferation", "nuclear-weapons",
    "organization-and-management", "outer-space",
    "peace", "personnel-civil-service", "personnel-demographics",
    "personnel-foreign-service", "political-prisoners",
    "politico-military-issues", "population-demographics",
    "prisoners-of-war", "property-claims",
    "protection-of-americans-abroad",
    "quarantine-blockade", "refugees",
    "science-and-technology", "self-determination",
    "south-and-central-asia", "sub-saharan-africa",
    "terrorism", "trade-and-commercial-policy-agreements",
    "vietnam-conflict", "war-crimes-and-war-criminals", "warfare",
    "western-hemisphere",
    # Historical conflicts
    "american-revolutionary-war", "war-of-1812",
    "antisemitism-post-world-war-ii",
    # International organizations
    "association-of-southeast-asian-nations",
    "conference-on-security-and-cooperation-in-europe",
    "european-advisory-commission", "european-economic-community",
    "far-eastern-commission",
    "general-agreement-on-tariffs-and-trade",
    "international-monetary-fund",
    "north-atlantic-treaty-organization",
    "organization-of-american-states",
    "organization-of-petroleum-exporting-countries",
    "southeast-asia-treaty-organization",
    "united-nations", "universal-postal-union",
    # Regions (not individual countries)
    "western-sahara",
}


def log(msg):
    print(msg, flush=True)


# ── Load existing taxonomy ───────────────────────────────────

def load_taxonomy_terms(path):
    """Load all terms, subcategories, and categories from subject-taxonomy-lcsh.xml.

    Returns:
        terms: dict of lowercase_name -> {name, ref, category, subcategory}
        subcategories: dict of slugified_label -> {label, category}
        categories: dict of slugified_label -> {label}
    """
    if not os.path.exists(path):
        log(f"WARNING: Taxonomy file not found: {path}")
        return {}, {}, {}

    tree = etree.parse(path)
    root = tree.getroot()
    terms = {}
    subcategories = {}
    categories = {}

    for cat in root.findall("category"):
        cat_label = cat.get("label", "Uncategorized")
        categories[slugify(cat_label)] = {"label": cat_label}

        for sub in cat.findall("subcategory"):
            sub_label = sub.get("label", "General")
            subcategories[slugify(sub_label)] = {
                "label": sub_label,
                "category": cat_label,
            }

            for subj in sub.findall("subject"):
                name_el = subj.find("name")
                if name_el is None or not name_el.text:
                    continue
                name = name_el.text.strip()
                terms[name.lower()] = {
                    "name": name,
                    "ref": subj.get("ref", ""),
                    "category": cat_label,
                    "subcategory": sub_label,
                }

    return terms, subcategories, categories


# ── Extract tags from volumes ────────────────────────────────

def extract_tags_from_xml(xml_content, source_name=""):
    """Extract HSG tags from a TEI XML volume.

    Returns: {
        "volume_id": str,
        "title": str,
        "tags": list of str,
        "administration": str,
    }
    """
    try:
        if isinstance(xml_content, bytes):
            # Raw XML bytes (from remote fetch) — use recover mode
            parser = etree.XMLParser(recover=True, huge_tree=True)
            root = etree.fromstring(xml_content, parser)
        elif isinstance(xml_content, str) and os.path.exists(xml_content):
            # File path
            tree = etree.parse(xml_content)
            root = tree.getroot()
        elif isinstance(xml_content, str):
            # Raw XML string
            parser = etree.XMLParser(recover=True, huge_tree=True)
            root = etree.fromstring(xml_content.encode("utf-8"), parser)
        else:
            tree = etree.parse(xml_content)
            root = tree.getroot()
        if root is None:
            log(f"  WARNING: Could not parse {source_name}: empty root")
            return None
    except Exception as e:
        log(f"  WARNING: Could not parse {source_name}: {e}")
        return None

    # Volume ID from xml:id or filename
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    vol_id = root.get(f"{{{xml_ns}}}id", "")
    if not vol_id:
        vol_id = source_name.replace(".xml", "")

    # Title
    title_el = root.find(f".//{{{TEI_NS}}}titleStmt/{{{TEI_NS}}}title[@type='complete']")
    if title_el is None:
        title_el = root.find(f".//{{{TEI_NS}}}titleStmt/{{{TEI_NS}}}title")
    title = ""
    if title_el is not None:
        title = "".join(title_el.itertext()).strip()
        # Truncate long titles
        if len(title) > 120:
            title = title[:117] + "..."

    # HSG subject tags
    tags = []
    for kw in root.xpath(
        '//tei:keywords[@scheme="https://history.state.gov/tags"]',
        namespaces=NSMAP,
    ):
        for term in kw.findall(f"{{{TEI_NS}}}term"):
            if term.text and term.text.strip():
                tags.append(term.text.strip())

    # Administration
    admin = ""
    for kw in root.xpath(
        '//tei:keywords[@scheme="#frus-administration-coverage"]',
        namespaces=NSMAP,
    ):
        admin_terms = [t.text.strip() for t in kw.findall(f"{{{TEI_NS}}}term")
                       if t.text]
        if admin_terms:
            admin = ", ".join(admin_terms)

    return {
        "volume_id": vol_id,
        "title": title,
        "tags": tags,
        "administration": admin,
    }


def scan_local_volumes():
    """Scan local volumes/ directory for HSG tags."""
    volume_files = sorted(glob.glob(os.path.join(VOLUMES_DIR, "*.xml")))

    # Exclude annotated files
    volume_files = [f for f in volume_files if "-annotated" not in f]

    if not volume_files:
        log("No volume files found in volumes/")
        return []

    log(f"Scanning {len(volume_files)} local volumes...")
    results = []

    for vf in volume_files:
        fname = os.path.basename(vf)
        info = extract_tags_from_xml(vf, fname)
        if info and info["tags"]:
            results.append(info)
            log(f"  {info['volume_id']}: {len(info['tags'])} tags")

    return results


def list_remote_volumes():
    """List all volume filenames from the GitHub API."""
    log("Fetching volume list from GitHub...")
    all_files = []
    page = 1

    while True:
        url = f"{GITHUB_API}?per_page=100&page={page}"
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "frus-subject-taxonomy")
        req.add_header("Accept", "application/vnd.github.v3+json")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 403:
                log("  GitHub API rate limit reached. Try again later or use a token.")
                break
            raise

        if not data:
            break

        for item in data:
            name = item.get("name", "")
            if name.endswith(".xml"):
                all_files.append(name)

        if len(data) < 100:
            break
        page += 1
        time.sleep(0.5)

    log(f"  Found {len(all_files)} volume files")
    return sorted(all_files)


def parse_volume_year_range(filename):
    """Extract the year range from a FRUS volume filename.

    Examples:
        frus1945Berlinv01.xml -> (1945, 1945)
        frus1969-76v19p2.xml -> (1969, 1976)
        frus1977-80v01.xml -> (1977, 1980)
        frus1993-00v48.xml -> (1993, 2000)
    """
    name = filename.replace(".xml", "")
    m = re.match(r"frus(\d{4})(?:-(\d{2,4}))?", name)
    if not m:
        return None, None

    start = int(m.group(1))
    if m.group(2):
        end_str = m.group(2)
        if len(end_str) == 2:
            # Two-digit year: infer century
            century = start // 100 * 100
            end = century + int(end_str)
            # Handle century crossing (e.g., 1993-00 -> 1993-2000)
            if end < start:
                end += 100
        else:
            end = int(end_str)
    else:
        end = start

    return start, end


def filter_volumes_by_era(filenames, era_start, era_end):
    """Filter volume filenames to those overlapping with [era_start, era_end]."""
    filtered = []
    for fn in filenames:
        start, end = parse_volume_year_range(fn)
        if start is None:
            continue
        # Check overlap: volume range [start, end] overlaps era [era_start, era_end]
        if start <= int(era_end) and end >= int(era_start):
            filtered.append(fn)
    return filtered


def fetch_remote_volume(filename, retries=3):
    """Fetch a volume's XML header from GitHub (just enough for keywords)."""
    url = f"{GITHUB_RAW}/{filename}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "frus-subject-taxonomy")

    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                # Read up to 100KB — the TEI header with keywords is in the first portion
                # For large volumes we need more since the header can be deep
                content = resp.read(150_000)
                # Try to close the tag if we truncated
                if not content.endswith(b"</TEI>"):
                    content += b"\n</TEI>"
                return content
        except urllib.error.HTTPError as e:
            if e.code == 404:
                log(f"  WARNING: {filename} not found (404)")
                return None
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                log(f"  WARNING: Failed to fetch {filename}: {e}")
                return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                log(f"  WARNING: Failed to fetch {filename}: {e}")
                return None


def scan_remote_volumes(era_filter=None):
    """Scan remote FRUS volumes from GitHub for HSG tags."""
    filenames = list_remote_volumes()
    if not filenames:
        return []

    if era_filter:
        if era_filter.lower() in ERA_ALIASES:
            era_start, era_end = ERA_ALIASES[era_filter.lower()]
        elif re.match(r"\d{4}s$", era_filter):
            # Decade: "1940s" -> 1940-1949
            decade = int(era_filter[:4])
            era_start, era_end = str(decade), str(decade + 9)
        elif re.match(r"\d{4}-\d{4}$", era_filter):
            era_start, era_end = era_filter.split("-")
        else:
            log(f"ERROR: Unrecognized era format: {era_filter}")
            log("  Use: decade (1940s), range (1939-1945), or alias (wwii)")
            sys.exit(1)

        filenames = filter_volumes_by_era(filenames, era_start, era_end)
        log(f"Filtered to {len(filenames)} volumes in era {era_start}-{era_end}")

    results = []
    for i, fn in enumerate(filenames):
        log(f"  [{i+1}/{len(filenames)}] Fetching {fn}...")
        content = fetch_remote_volume(fn)
        if content is None:
            continue

        info = extract_tags_from_xml(content, fn)
        if info and info["tags"]:
            results.append(info)
            log(f"    {info['volume_id']}: {len(info['tags'])} tags")
        else:
            log(f"    {fn}: no HSG tags found")

        # Rate limit
        if i < len(filenames) - 1:
            time.sleep(0.3)

    return results


# ── Analysis ─────────────────────────────────────────────────

def slugify(name):
    """Convert a taxonomy term name to an HSG-style slug for matching.

    'Human rights' -> 'human-rights'
    'Arms control and disarmament' -> 'arms-control-and-disarmament'
    """
    slug = name.lower().strip()
    # Treat slashes, colons, parens as word separators (e.g. "HIV/AIDS" -> "hiv-aids",
    # "Trade and Commercial Policy/Agreements" -> "...-policy-agreements",
    # "Personnel: Foreign Service" -> "personnel-foreign-service")
    slug = re.sub(r"[/:()\\.]+", " ", slug)
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"[\s-]+", "-", slug)
    return slug.strip("-")


def classify_tag(tag):
    """Classify a tag as 'topic' or 'other' (person/country).

    Only topic tags are relevant for taxonomy gap analysis.
    """
    if tag in KNOWN_TOPIC_TAGS:
        return "topic"
    return "other"


def analyze_gaps(volume_results, taxonomy_terms, taxonomy_subcats, taxonomy_cats):
    """Compare HSG tags against taxonomy to find gaps.

    Matches against:
      1. Subject term names (slugified)
      2. Subcategory labels (slugified)
      3. Category labels (slugified)

    Returns structured gap analysis.
    """
    # Build slug-to-term mapping from taxonomy subject names
    taxonomy_slugs = {}
    for lower_name, info in taxonomy_terms.items():
        slug = slugify(info["name"])
        taxonomy_slugs[slug] = info

    # Collect all tags across volumes
    tag_volumes = defaultdict(list)  # tag -> list of volume_ids
    for vol in volume_results:
        for tag in vol["tags"]:
            tag_volumes[tag].append(vol["volume_id"])

    # Classify each tag
    matched = {}       # tag -> taxonomy info
    unmatched = {}     # tag -> {volumes, type}
    skipped_tags = {}  # tag -> volumes (countries/persons)

    for tag, volumes in sorted(tag_volumes.items()):
        tag_type = classify_tag(tag)
        if tag_type != "topic":
            skipped_tags[tag] = volumes
            continue

        # 1. Try exact slug match against subject term names
        if tag in taxonomy_slugs:
            matched[tag] = {**taxonomy_slugs[tag], "volumes": volumes,
                            "match_level": "subject"}
            continue

        # 2. Try humanized form against lowercase term names
        humanized = tag.replace("-", " ")
        if humanized in taxonomy_terms:
            matched[tag] = {**taxonomy_terms[humanized], "volumes": volumes,
                            "match_level": "subject"}
            continue

        # 3. Try match against subcategory labels
        if tag in taxonomy_subcats:
            sc = taxonomy_subcats[tag]
            matched[tag] = {
                "name": sc["label"],
                "ref": "",
                "category": sc["category"],
                "subcategory": sc["label"],
                "volumes": volumes,
                "match_level": "subcategory",
            }
            continue

        # 4. Try match against category labels
        if tag in taxonomy_cats:
            cat = taxonomy_cats[tag]
            matched[tag] = {
                "name": cat["label"],
                "ref": "",
                "category": cat["label"],
                "subcategory": "",
                "volumes": volumes,
                "match_level": "category",
            }
            continue

        # 5. Try substring match: tag slug is a prefix of a taxonomy slug
        #    e.g. "western-sahara" matches "Western Sahara dispute"
        found = False
        for tax_slug, tax_info in taxonomy_slugs.items():
            if tax_slug.startswith(tag + "-") or tax_slug.endswith("-" + tag):
                matched[tag] = {**tax_info, "volumes": volumes,
                                "match_level": "subject (partial)"}
                found = True
                break
        if found:
            continue

        unmatched[tag] = {"volumes": volumes, "volume_count": len(volumes)}

    return matched, unmatched, skipped_tags


def print_report(volume_results, matched, unmatched, skipped_tags):
    """Print a human-readable gap analysis report."""
    log("")
    log("=" * 70)
    log("  HSG Tag Gap Analysis")
    log("=" * 70)

    log(f"\n  Volumes scanned:     {len(volume_results)}")
    total_tags = sum(len(v['tags']) for v in volume_results)
    unique_tags = len(matched) + len(unmatched) + len(skipped_tags)
    log(f"  Total tag instances: {total_tags}")
    log(f"  Unique tags:         {unique_tags}")
    log(f"    Matched taxonomy:  {len(matched)}")
    log(f"    Missing topics:    {len(unmatched)}")
    log(f"    Countries/persons: {len(skipped_tags)} (skipped)")

    if unmatched:
        log(f"\n{'─' * 70}")
        log("  MISSING TOPIC TAGS (not in taxonomy)")
        log(f"{'─' * 70}")

        # Sort by volume count (most common first)
        by_count = sorted(unmatched.items(),
                          key=lambda x: x[1]["volume_count"], reverse=True)

        for tag, info in by_count:
            humanized = tag.replace("-", " ").title()
            vol_count = info["volume_count"]
            vol_list = ", ".join(info["volumes"][:5])
            if len(info["volumes"]) > 5:
                vol_list += f" (+{len(info['volumes']) - 5} more)"
            log(f"\n  {humanized}")
            log(f"    HSG tag: {tag}")
            log(f"    Volumes: {vol_count} — {vol_list}")

    if matched:
        log(f"\n{'─' * 70}")
        log("  MATCHED TOPIC TAGS (already in taxonomy)")
        log(f"{'─' * 70}")
        for tag, info in sorted(matched.items()):
            level = info.get("match_level", "subject")
            level_label = f" [{level}]" if level != "subject" else ""
            log(f"  {tag:45s} -> {info['name']} ({info['category']}){level_label}")

    log("")


def save_results(volume_results, matched, unmatched, skipped_tags, output_path):
    """Save gap analysis to JSON for programmatic use."""
    output = {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "volumes_scanned": len(volume_results),
        "summary": {
            "matched": len(matched),
            "missing": len(unmatched),
            "persons_skipped": len(skipped_tags),
        },
        "missing_topics": [
            {
                "hsg_tag": tag,
                "humanized": tag.replace("-", " ").title(),
                "volume_count": info["volume_count"],
                "volumes": info["volumes"],
            }
            for tag, info in sorted(
                unmatched.items(),
                key=lambda x: x[1]["volume_count"],
                reverse=True,
            )
        ],
        "matched_topics": [
            {
                "hsg_tag": tag,
                "taxonomy_name": info["name"],
                "taxonomy_ref": info["ref"],
                "category": info["category"],
                "partial": info.get("partial_match", False),
            }
            for tag, info in sorted(matched.items())
        ],
        "skipped_tags": sorted(skipped_tags.keys()),
        "volumes": [
            {
                "volume_id": v["volume_id"],
                "title": v["title"],
                "administration": v["administration"],
                "tag_count": len(v["tags"]),
                "tags": v["tags"],
            }
            for v in volume_results
        ],
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    log(f"  Results saved to: {output_path}")


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Discover HSG tags from FRUS volumes and compare against taxonomy"
    )
    parser.add_argument(
        "--remote", action="store_true",
        help="Scan volumes from the public HistoryAtState/frus GitHub repo"
    )
    parser.add_argument(
        "--era", type=str, default=None,
        help="Filter by era: decade (1940s), range (1939-1945), or alias (wwii, carter, reagan)"
    )
    parser.add_argument(
        "--list-eras", action="store_true",
        help="Show available era aliases"
    )
    parser.add_argument(
        "--output", type=str, default=OUTPUT_FILE,
        help=f"Output JSON path (default: {OUTPUT_FILE})"
    )
    parser.add_argument(
        "--include-persons", action="store_true",
        help="Include person tags in the gap analysis"
    )
    args = parser.parse_args()

    if args.list_eras:
        log("Available era aliases:")
        for alias, (start, end) in sorted(ERA_ALIASES.items()):
            log(f"  {alias:20s} {start}-{end}")
        log("\nOr use: decade (1940s), year range (1939-1945)")
        return

    # Load taxonomy
    log("Loading taxonomy...")
    taxonomy_terms, taxonomy_subcats, taxonomy_cats = load_taxonomy_terms(TAXONOMY_FILE)
    log(f"  {len(taxonomy_terms)} terms, {len(taxonomy_subcats)} subcategories, {len(taxonomy_cats)} categories")

    # Scan volumes
    if args.remote:
        volume_results = scan_remote_volumes(era_filter=args.era)
    else:
        volume_results = scan_local_volumes()

    if not volume_results:
        log("\nNo volumes with HSG tags found.")
        return

    # Analyze gaps
    log("\nAnalyzing gaps...")
    matched, unmatched, skipped_tags = analyze_gaps(
        volume_results, taxonomy_terms, taxonomy_subcats, taxonomy_cats
    )

    # Print report
    print_report(volume_results, matched, unmatched, skipped_tags)

    # Save results
    save_results(volume_results, matched, unmatched, skipped_tags, args.output)


if __name__ == "__main__":
    main()
