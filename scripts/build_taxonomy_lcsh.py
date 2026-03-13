#!/usr/bin/env python3
"""
Build a subject taxonomy using LCSH broader terms (max 2 levels)
combined with domain-specific grouping for unmatched subjects.

Uses only exact and good_close LCSH matches. For subjects without
LCSH matches, applies foreign-policy-domain categorization.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

MAPPING_FILE = "../config/lcsh_mapping.json"
DOC_APPEARANCES_FILE = "../document_appearances.json"
OUTPUT_TAXONOMY = "../subject-taxonomy-lcsh.xml"

SKOS_URL_TEMPLATE = "https://id.loc.gov/authorities/subjects/{}.skos.json"
SKOS_BROADER = "http://www.w3.org/2004/02/skos/core#broader"
SKOS_PREFLABEL = "http://www.w3.org/2004/02/skos/core#prefLabel"

REQUEST_DELAY = 0.5
BT_CACHE_FILE = "../lcsh_broader_cache.json"


def fetch_label_and_broader(uri, retries=3):
    """Fetch label and immediate broader term URIs for an LCSH entry."""
    lccn = uri.rstrip("/").split("/")[-1]
    skos_url = SKOS_URL_TEMPLATE.format(lccn)

    for attempt in range(retries):
        try:
            req = urllib.request.Request(skos_url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))

            for item in data:
                if item.get("@id", "").rstrip("/") == uri.rstrip("/"):
                    # Extract label
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

                    # Extract broader URIs (only LCSH authorities)
                    broader_uris = []
                    bt_list = item.get(SKOS_BROADER, [])
                    if not isinstance(bt_list, list):
                        bt_list = [bt_list]
                    for bt in bt_list:
                        if isinstance(bt, dict) and "@id" in bt:
                            bt_uri = bt["@id"]
                            if "authorities/subjects" in bt_uri:
                                broader_uris.append(bt_uri)

                    return label, broader_uris

            return None, []

        except Exception as e:
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 2)
            else:
                print(f"  Error fetching {uri}: {e}")
                return None, []

    return None, []


def fetch_two_level_hierarchy(mapping):
    """For each subject with LCSH URI, fetch BT level 1 and BT level 2."""

    # Load cache
    bt_cache = {}
    if os.path.exists(BT_CACHE_FILE):
        with open(BT_CACHE_FILE) as f:
            bt_cache = json.load(f)

    # Collect all unique LCSH URIs that need hierarchy
    uris_to_fetch = set()
    for ref, data in mapping.items():
        quality = data.get("match_quality", "no_match")
        if quality in ("exact", "good_close") and data.get("lcsh_uri"):
            uri = data["lcsh_uri"]
            if uri not in bt_cache:
                uris_to_fetch.add(uri)

    print(f"Need to fetch hierarchies for {len(uris_to_fetch)} LCSH URIs")
    print(f"Already cached: {len(bt_cache)}")

    # Level 1: fetch broader terms for each subject
    level1_broader = {}  # uri -> [(bt_label, bt_uri), ...]
    level2_uris = set()

    done = 0
    total = len(uris_to_fetch)
    for uri in uris_to_fetch:
        done += 1
        if uri in bt_cache:
            continue

        label, broader_uris = fetch_label_and_broader(uri)
        time.sleep(REQUEST_DELAY)

        bt_entries = []
        for bt_uri in broader_uris:
            if bt_uri not in bt_cache:
                level2_uris.add(bt_uri)
            bt_entries.append(bt_uri)

        bt_cache[uri] = {
            "label": label,
            "broader_uris": bt_entries,
        }

        if done % 25 == 0:
            print(f"  Level 1: {done}/{total}")
            with open(BT_CACHE_FILE, "w") as f:
                json.dump(bt_cache, f, indent=2)

    print(f"Level 1 complete. Need to fetch {len(level2_uris)} level-2 broader terms.")

    # Level 2: fetch broader terms for the BT entries
    done = 0
    total = len(level2_uris)
    for uri in level2_uris:
        if uri in bt_cache:
            continue
        done += 1

        label, broader_uris = fetch_label_and_broader(uri)
        time.sleep(REQUEST_DELAY)

        bt_cache[uri] = {
            "label": label,
            "broader_uris": broader_uris,
        }

        if done % 25 == 0:
            print(f"  Level 2: {done}/{total}")
            with open(BT_CACHE_FILE, "w") as f:
                json.dump(bt_cache, f, indent=2)

    # Save cache
    with open(BT_CACHE_FILE, "w") as f:
        json.dump(bt_cache, f, indent=2)

    # Now build the hierarchy chain (max 2 levels) for each subject
    for ref, data in mapping.items():
        quality = data.get("match_quality", "no_match")
        if quality not in ("exact", "good_close") or not data.get("lcsh_uri"):
            continue

        uri = data["lcsh_uri"]
        entry = bt_cache.get(uri, {})
        broader_chain = []

        # Level 1
        bt1_uris = entry.get("broader_uris", [])
        if bt1_uris:
            bt1_uri = bt1_uris[0]  # Take first broader term
            bt1_entry = bt_cache.get(bt1_uri, {})
            bt1_label = bt1_entry.get("label") or bt1_uri.split("/")[-1]
            broader_chain.append({"label": bt1_label, "uri": bt1_uri})

            # Level 2
            bt2_uris = bt1_entry.get("broader_uris", [])
            if bt2_uris:
                bt2_uri = bt2_uris[0]
                bt2_entry = bt_cache.get(bt2_uri, {})
                bt2_label = bt2_entry.get("label") or bt2_uri.split("/")[-1]
                broader_chain.append({"label": bt2_label, "uri": bt2_uri})

        data["broader_chain_2lvl"] = broader_chain

    # Save updated mapping
    with open(MAPPING_FILE, "w") as f:
        json.dump(mapping, f, indent=2)

    return mapping


# ── HSG topic taxonomy (from history.state.gov/tags/all) ────────────────
# Official Office of the Historian subject taxonomy with subcategories.
# Each top-level topic maps to a dict with:
#   "keywords" – terms for matching subjects to this category
#   "subcategories" – ordered dict of subcategory name → keyword list
# Subcategory keywords are tried first (more specific); if none match,
# the subject falls into "General" within the parent category.

HSG_TAXONOMY = {
    "Arms Control and Disarmament": {
        "keywords": [
            "arms control", "disarmament", "nonproliferation",
            "SALT", "START", "ABM", "INF", "MBFR", "CFE", "ASAT",
            "anti-ballistic missile treaty",
            "verification", "test ban", "moratorium",
            "arms limitation", "arms reduction", "arms race",
            "non-proliferation", "nuclear non-proliferation",
            "strategic arms", "strategic offensive", "strategic defenses",
            "strategic force", "strategic nuclear",
            "warhead", "missile", "ballistic", "ICBM",
            "cruise missile", "submarine-launched", "air-launched",
            "bomber aircraft", "nuclear testing", "nuclear fuel",
            "mobile missile", "throw-weight", "reentry vehicle",
            "re-entry vehicle", "sublimit",
            "zero option", "global zero",
            "space-strike", "first strike",
            "counting rule", "on-site inspection",
            "backfire bomber", "gravity bomb",
            "SS-20", "SS\u201320", "Trident",
            "NTM", "national technical means",
            "force modernization",
            "SLBM", "ALCM",
            "delivery vehicle",
            "interim agreement",
            "safeguard", "overflight",
        ],
        "subcategories": {
            "Arms Embargoes": ["arms embargo"],
            "Arms Transfers": ["arms transfer", "arms sale"],
            "Chemical and Bacteriological Warfare": [
                "chemical weapon", "bacteriological warfare",
                "biological weapon", "CW", "BWC",
            ],
            "Collective Security": ["collective security"],
            "Confidence-Building Measures": ["confidence-building"],
            "Nuclear Nonproliferation": [
                "nonproliferation", "non-proliferation",
                "nuclear nonproliferation", "nuclear non-proliferation",
                "nuclear fuel", "safeguard", "IAEA",
            ],
            "Nuclear Weapons": [
                "nuclear weapon", "nuclear testing", "warhead",
                "missile", "ballistic", "ICBM",
                "strategic arms", "strategic offensive",
                "bomber aircraft", "mobile missile",
                "cruise missile", "submarine-launched", "air-launched",
                "throw-weight", "reentry vehicle", "re-entry vehicle",
                "SLBM", "ALCM", "delivery vehicle",
                "backfire bomber", "gravity bomb",
                "SS-20", "SS\u201320", "Trident",
                "first strike", "space-strike",
                "zero option", "global zero",
                "sublimit", "counting rule",
            ],
        },
    },
    "Department of State": {
        "keywords": [
            "department of state", "state department",
            "foreign service", "civil service",
            "ambassador", "embassy", "consulate", "consular",
            "congressional relations", "protocol",
            "protection of americans abroad",
            "locally employed staff", "visa",
        ],
        "subcategories": {
            "Buildings: Domestic": [],
            "Buildings: Foreign": ["embassy", "consulate"],
            "Congressional Relations": ["congressional relations"],
            "Organization and Management": ["organization", "management"],
            "Personnel: Civil Service": ["civil service"],
            "Personnel: Demographics": ["demographics"],
            "Personnel: Foreign Service": ["foreign service"],
            "Personnel: Locally Employed Staff": ["locally employed"],
            "Protection of Americans Abroad": ["protection of americans"],
            "Protocol": ["protocol"],
        },
    },
    "Foreign Economic Policy": {
        "keywords": [
            "agriculture", "economic sanction", "sanction",
            "economic summit", "energy", "natural resources",
            "financial", "monetary", "fiscal",
            "foreign aid", "foreign investment",
            "new international economic order",
            "trade", "commercial",
            "economic", "tariff", "embargo",
            "export", "import", "investment",
            "debt", "loan", "aid",
            "development aid", "development assistance",
            "food aid", "PL 480",
            "oil", "petroleum", "gas",
            "commodity", "market",
            "budget", "private sector",
            "hunger", "drought", "famine",
            "sea bed mining", "seabed mining",
            "north-south dialogue",
            "least developed countries", "developing countries",
        ],
        "subcategories": {
            "Agriculture": [
                "agriculture", "agricultural", "food aid",
                "grain", "sugar", "food", "hunger", "drought", "famine",
                "PL 480",
            ],
            "Economic Sanctions": ["economic sanction", "sanction"],
            "Economic Summit Meetings": ["economic summit"],
            "Energy and Natural Resources": [
                "energy", "oil", "petroleum", "gas",
                "natural resources", "mining", "minerals",
            ],
            "Financial and Monetary Policy": [
                "financial", "monetary", "fiscal",
                "debt", "loan", "budget",
            ],
            "Foreign Aid": [
                "foreign aid", "development aid", "development assistance",
                "PL 480", "donor", "least developed countries",
            ],
            "Foreign Investment": ["foreign investment", "investment"],
            "Labor": ["labor"],
            "New International Economic Order": [
                "new international economic order",
                "north-south dialogue",
            ],
            "Trade and Commercial Policy/Agreements": [
                "trade", "commercial", "tariff",
                "export", "import", "commodity", "market",
            ],
        },
    },
    "Global Issues": {
        "keywords": [
            "border administration", "decolonization",
            "election", "immigration", "narcotics", "drug",
            "outer space", "space program",
            "peace", "polar affairs",
            "population", "self-determination",
            "migration", "environment", "climate",
            "pollution", "conservation",
            "ozone", "whaling", "whale",
            "family planning", "abortion",
            "continental shelf", "seabed",
            "regional issue",
            "national independence",
            "scientific",
            "d\u00e9tente", "detente",
            "aviation security",
            "public health",
        ],
        "subcategories": {
            "Air Safety": ["aviation security"],
            "Border Administration": ["border administration", "border"],
            "Decolonization": ["decolonization", "national independence"],
            "Elections": ["election"],
            "Immigration": ["immigration", "migration"],
            "Narcotics": ["narcotics", "drug"],
            "Outer Space": ["outer space", "space program"],
            "Peace": ["peace", "d\u00e9tente", "detente"],
            "Polar Affairs": ["polar affairs", "polar"],
            "Population Demographics": [
                "population", "family planning", "abortion",
            ],
            "Public Health": ["public health"],
            "Self-Determination": ["self-determination"],
        },
    },
    "Human Rights": {
        "keywords": [
            "human rights", "antisemitism",
            "asylum", "civil rights",
            "detainee", "disability rights",
            "genocide", "HIV", "AIDS",
            "political prisoner", "refugee",
            "discrimination", "apartheid", "persecution",
            "emigration", "dissident", "freedom",
            "liberty", "humanitarian",
            "prisoner release", "emergency relief",
            "trial", "torture", "psychiatric abuse",
            "religious freedom",
        ],
        "subcategories": {
            "Antisemitism": [
                "antisemitism", "anti-jewish", "anti-semit",
                "pogrom", "jewish persecution",
            ],
            "Asylum": ["asylum"],
            "Chinese Exclusion Act (1882)": ["chinese exclusion"],
            "Civil Rights": ["civil rights"],
            "Detainees": ["detainee"],
            "Disability Rights": ["disability rights", "disability"],
            "Genocide": ["genocide"],
            "HIV/AIDS": ["HIV", "AIDS"],
            "Political Prisoners": [
                "political prisoner", "prisoner release",
                "dissident",
            ],
            "Refugees": ["refugee", "emergency relief"],
            "Religious Freedom": ["religious freedom"],
        },
    },
    "Information Programs": {
        "keywords": [
            "information program", "propaganda", "media", "press",
            "broadcast", "radio", "cultural exchange",
            "public diplomacy", "USIA", "Voice of America",
            "exchange program",
        ],
        "subcategories": {},
    },
    "International Law": {
        "keywords": [
            "international law", "law of the sea",
            "property claims", "jurisdiction", "sovereignty",
            "protest against U.S.", "treaty", "convention",
            "legal", "compliance", "regulation",
            "legislation", "judicial",
            "extradition", "claims tribunal",
            "international court",
        ],
        "subcategories": {
            "Domestic Protest against U.S. Activity": [
                "domestic protest",
            ],
            "Foreign Protest against U.S. Activity": [
                "foreign protest",
            ],
            "Law of the Sea": [
                "law of the sea", "territorial sea",
                "continental shelf", "seabed",
            ],
            "Property Claims": [
                "property claims", "claims tribunal",
                "iranian assets",
            ],
        },
    },
    "International Organizations": {
        "keywords": [
            "United Nations", "NATO",
            "Association of Southeast Asian Nations", "ASEAN",
            "Conference on Security and Cooperation in Europe", "CSCE",
            "European Advisory Commission",
            "European Economic Community", "European Community",
            "Far Eastern Commission",
            "General Agreement on Tariffs and Trade", "GATT",
            "International Monetary Fund", "IMF",
            "League of Nations",
            "International Atomic Energy Agency", "IAEA",
            "non-governmental organization", "NGO",
            "Organization of American States", "OAS",
            "Organization of Petroleum Exporting Countries", "OPEC",
            "Southeast Asia Treaty Organization", "SEATO",
            "Universal Postal Union",
            "World Trade Organization", "WTO",
            "OAU", "OECD", "World Bank", "G-7", "G-8",
            "international organization",
            "Non-aligned Movement", "NAM",
        ],
        "subcategories": {
            "Association of Southeast Asian Nations": ["ASEAN"],
            "Conference on Security and Cooperation in Europe": ["CSCE"],
            "European Advisory Commission": ["European Advisory Commission"],
            "European Economic Community": [
                "European Economic Community", "European Community",
            ],
            "Far Eastern Commission": ["Far Eastern Commission"],
            "General Agreement on Tariffs and Trade": ["GATT"],
            "International Monetary Fund": ["IMF"],
            "League of Nations": ["League of Nations"],
            "International Atomic Energy Agency": ["IAEA"],
            "Non-governmental Organizations": [
                "non-governmental organization", "NGO",
            ],
            "North Atlantic Treaty Organization": ["NATO"],
            "Organization of American States": ["OAS"],
            "Organization of Petroleum Exporting Countries": ["OPEC"],
            "Southeast Asia Treaty Organization": ["SEATO"],
            "United Nations": ["United Nations"],
            "Universal Postal Union": ["Universal Postal Union"],
            "World Trade Organization": ["WTO"],
        },
    },
    "Politico-Military Issues": {
        "keywords": [
            "alliance", "armistice", "covert action", "covert operation",
            "diplomatic recognition", "military base",
            "military intervention", "military presence",
            "military withdrawal",
            "national security council", "NSC",
            "national security policy",
            "quarantine", "blockade",
            "terrorism",
            "military", "defense", "armed forces",
            "army", "navy", "air force",
            "intelligence", "CIA", "KGB",
            "security", "deterrence",
            "reconnaissance", "espionage",
            "relations", "foreign policy",
            "diplomacy", "diplomatic",
            "bilateral", "multilateral",
            "negotiation", "summit",
            "normalization", "rapprochement",
            "brigade",
        ],
        "subcategories": {
            "Alliances": ["alliance"],
            "Armistices": ["armistice"],
            "Covert Action": ["covert action", "covert operation"],
            "Diplomatic Recognition": ["diplomatic recognition"],
            "Military Bases": ["military base"],
            "Military Intervention, Presence and Withdrawal": [
                "military intervention", "military presence",
                "military withdrawal",
            ],
            "National Security Council": [
                "national security council", "NSC",
            ],
            "National Security Policy": ["national security policy"],
            "Quarantine (Blockade)": ["quarantine", "blockade"],
            "Terrorism": ["terrorism"],
        },
    },
    "Science and Technology": {
        "keywords": [
            "science", "technology", "research",
            "atomic energy", "nuclear energy",
            "telecommunications", "computer", "satellite",
            "space science", "biodiversity", "fisheries",
            "ocean", "maritime",
        ],
        "subcategories": {
            "Atomic Energy": ["atomic energy", "nuclear energy"],
        },
    },
    "Warfare": {
        "keywords": [
            "war", "conflict", "ceasefire", "hostilities",
            "invasion", "occupation",
            "insurgency", "guerrilla",
            "hostage", "coup", "crisis",
            "peacekeeping", "mediation",
            "prisoners of war", "war crimes", "neutrality",
            "Korean War", "Vietnam", "World War",
            "Arab-Israeli", "Cuban Missile Crisis",
            "Geneva Convention",
            "dispute",
        ],
        "subcategories": {
            "Afghanistan Conflict (2001)": ["Afghanistan conflict", "Afghanistan war"],
            "American Revolutionary War": [
                "American Revolution", "Revolutionary War",
                "war of independence",
            ],
            "Arab-Israeli Dispute": ["Arab-Israeli"],
            "Civil War (U.S.)": ["Civil War", "Confedera"],
            "Cuban Missile Crisis": ["Cuban Missile Crisis"],
            "Geneva Convention": ["Geneva Convention"],
            "Iraq War (2003)": ["Iraq war", "Iraq conflict"],
            "Korean War": ["Korean War"],
            "Mexican-American War": ["Mexican-American War", "Mexican War"],
            "Neutrality": ["neutrality"],
            "Prisoners of War": ["prisoners of war"],
            "Spanish-American War": ["Spanish-American War"],
            "Suez Canal": ["Suez Canal", "Suez Crisis"],
            "Vietnam Conflict": ["Vietnam"],
            "War Crimes and War Criminals": ["war crimes", "war criminal"],
            "War of 1812": ["War of 1812"],
            "World War I": ["World War I"],
            "World War II": ["World War II"],
        },
    },
    "Bilateral Relations": {
        "keywords": [
            "bilateral relations", "bilateral issues",
            "east-west relations", "diplomatic relations",
        ],
        "subcategories": {
            "U.S.-Soviet/Russian Relations": [
                "soviet union relations", "soviet union bilateral",
                "soviet union trade relations", "soviet cultural exchanges",
            ],
            "NATO and European Relations": [
                "nato relations", "germany relations", "west germany relations",
                "spain relations", "poland relations", "norwegian relations",
            ],
            "East Asian Relations": [
                "china relations", "japan relations",
                "chinese relationship", "china normalization",
            ],
            "South Asian Relations": [
                "india relations", "indo-pak", "sino-indian",
            ],
            "Middle East and North African Relations": [
                "iran relations", "libya relations",
                "algeria relations", "morocco relations",
                "tunisia relations", "egypt relations",
                "israeli", "iraq relations",
                "saudi arabia relations", "turkey relations",
                "egyptian/libyan", "egyptian-libyan",
            ],
            "Western Hemisphere Relations": [
                "cuba relations", "mexico relations",
                "jamaica relations", "guyana relations",
                "haiti relations", "barbados relations",
                "dominican republic relations", "bahamas relations",
                "grenada relations", "trinidad relations",
                "dominica relations", "latin america",
            ],
            "Sub-Saharan African Relations": [
                "ethiopian relations",
            ],
        },
    },
}


def _keyword_score(keywords, texts):
    """Score how well a list of keywords matches against search texts."""
    score = 0
    for kw in keywords:
        kw_l = kw.lower()
        for text in texts:
            if kw_l in text:
                score += len(kw)
                break
    return score


def categorize_by_hsg(name, lcsh_label=None):
    """Categorize a subject into an HSG topic and subcategory.

    Returns (category, subcategory) or (None, None) if no match.
    """
    texts = [name.lower()]
    if lcsh_label:
        texts.append(lcsh_label.lower())

    # Step 1: find best top-level category
    # Include subcategory keywords in the category score so subjects
    # matching a specific subcategory also match the parent category.
    best_cat = None
    best_score = 0

    for cat, cat_data in HSG_TAXONOMY.items():
        all_kw = list(cat_data["keywords"])
        for sub_kw in cat_data.get("subcategories", {}).values():
            all_kw.extend(sub_kw)
        score = _keyword_score(all_kw, texts)
        if score > best_score:
            best_score = score
            best_cat = cat

    if not best_cat:
        return None, None

    # Step 2: find best subcategory within matched category
    subcats = HSG_TAXONOMY[best_cat].get("subcategories", {})
    best_sub = "General"
    best_sub_score = 0

    for sub_name, sub_keywords in subcats.items():
        score = _keyword_score(sub_keywords, texts)
        if score > best_sub_score:
            best_sub_score = score
            best_sub = sub_name

    return best_cat, best_sub


def _normalize_name(name):
    """Normalize a subject name for deduplication grouping."""
    import re as _re
    n = name.lower().strip()
    # Remove parenthetical qualifiers like (HUMINT)
    n = _re.sub(r'\s*\(.*?\)\s*', ' ', n)
    # Normalize hyphens and slashes to spaces
    n = _re.sub(r'[-/]', ' ', n)
    n = _re.sub(r'\s+', ' ', n).strip()
    # Basic plural → singular
    if n.endswith('ies'):
        n = n[:-3] + 'y'
    elif n.endswith('es') and not n.endswith('ses'):
        n = n[:-2]
    elif n.endswith('s') and not n.endswith('ss'):
        n = n[:-1]
    return n


DEDUP_DECISIONS_FILE = "../config/dedup_decisions.json"
CATEGORY_OVERRIDES_FILE = "../config/category_overrides.json"


def apply_dedup_decisions(mapping):
    """Apply reviewed dedup decisions globally to the mapping.

    Reads dedup_decisions.json and merges entries marked as 'merge'.
    For each merge group:
    - The primary_ref becomes the canonical entry
    - Counts are summed across all entries
    - Document appearances are merged (union)
    - All original rec IDs are preserved in merged_refs
    - Best LCSH match is kept
    - Secondary refs are removed from the mapping
    """
    if not os.path.exists(DEDUP_DECISIONS_FILE):
        print("  No dedup_decisions.json found, skipping global dedup")
        return mapping

    with open(DEDUP_DECISIONS_FILE) as f:
        decisions = json.load(f)

    merge_groups = decisions.get("merge", [])
    if not merge_groups:
        print("  No merge decisions to apply")
        return mapping

    merged_count = 0
    removed_refs = set()

    for group in merge_groups:
        primary_ref = group["primary_ref"]
        all_refs = group["all_refs"]
        secondary_refs = [r for r in all_refs if r != primary_ref]

        # Skip if primary not in mapping
        if primary_ref not in mapping:
            continue

        # Collect all entries that exist in mapping
        entries = []
        for ref in all_refs:
            if ref in mapping:
                entries.append((ref, mapping[ref]))

        if len(entries) <= 1:
            continue

        # Build combined entry from primary
        primary_data = dict(mapping[primary_ref])
        primary_data["merged_refs"] = [r for r, _ in entries]

        # Sum counts
        primary_data["count"] = sum(int(d.get("count", 0)) for _, d in entries)

        # Merge appears_in volumes
        all_vols = set()
        for _, d in entries:
            ai = d.get("appears_in", "")
            for v in ai.split(", "):
                v = v.strip()
                if v:
                    all_vols.add(v)
        primary_data["appears_in"] = ", ".join(sorted(all_vols))
        primary_data["volumes"] = len(all_vols)

        # Merge document_appearances
        merged_docs = {}
        for _, d in entries:
            for vol, docs in d.get("document_appearances", {}).items():
                existing = set(merged_docs.get(vol, []))
                existing.update(docs)
                merged_docs[vol] = sorted(existing)
        primary_data["document_appearances"] = merged_docs

        # Keep best LCSH match
        for _, d in entries:
            if d.get("match_quality") == "exact" and d.get("lcsh_uri"):
                primary_data["lcsh_uri"] = d["lcsh_uri"]
                primary_data["lcsh_label"] = d.get("lcsh_label", "")
                primary_data["match_quality"] = "exact"
                primary_data["exact_match"] = True
                break
            elif d.get("match_quality") == "good_close" and d.get("lcsh_uri"):
                primary_data["lcsh_uri"] = d["lcsh_uri"]
                primary_data["lcsh_label"] = d.get("lcsh_label", "")
                primary_data["match_quality"] = "good_close"

        # Update primary in mapping
        mapping[primary_ref] = primary_data

        # Remove secondary refs
        for ref in secondary_refs:
            if ref in mapping:
                del mapping[ref]
                removed_refs.add(ref)
                merged_count += 1

    print(f"  Global dedup: merged {merged_count} entries into {len(merge_groups)} primary entries")
    print(f"  Mapping now has {len(mapping)} subjects")
    return mapping


def deduplicate_subjects(categories):
    """Merge near-duplicate subjects within each subcategory.

    Subjects whose names normalize to the same string are combined:
    - The entry with the highest annotation count becomes the primary
    - Annotation counts are summed
    - Document appearances are merged (union of volumes/docs)
    - All original refs are recorded in merged_refs
    - LCSH data is kept from the best-quality match
    """
    merged_count = 0
    for cat_name, subcats in categories.items():
        for sub_name, subjects in subcats.items():
            # Group by normalized name
            groups = {}
            for ref, data in subjects:
                key = _normalize_name(data.get("name", ""))
                groups.setdefault(key, []).append((ref, data))

            merged = []
            for norm_key, entries in groups.items():
                if len(entries) == 1:
                    merged.append(entries[0])
                    continue

                # Sort: prefer the one with most annotations as primary
                entries.sort(key=lambda x: int(x[1].get("count", 0)), reverse=True)
                primary_ref, primary_data = entries[0]

                # Deep-copy primary to avoid mutating mapping
                combined = dict(primary_data)
                combined["merged_refs"] = [r for r, _ in entries]

                # Sum counts
                combined["count"] = sum(int(d.get("count", 0)) for _, d in entries)

                # Merge volumes set
                all_vols = set()
                for _, d in entries:
                    ai = d.get("appears_in", "")
                    for v in ai.split(", "):
                        v = v.strip()
                        if v:
                            all_vols.add(v)
                combined["appears_in"] = ", ".join(sorted(all_vols))
                combined["volumes"] = len(all_vols)

                # Merge document_appearances
                merged_docs = {}
                for _, d in entries:
                    for vol, docs in d.get("document_appearances", {}).items():
                        existing = set(merged_docs.get(vol, []))
                        existing.update(docs)
                        merged_docs[vol] = sorted(existing)
                combined["document_appearances"] = merged_docs

                # Keep LCSH from best match
                for _, d in entries:
                    if d.get("match_quality") == "exact" and d.get("lcsh_uri"):
                        combined["lcsh_uri"] = d["lcsh_uri"]
                        combined["lcsh_label"] = d.get("lcsh_label", "")
                        combined["match_quality"] = "exact"
                        combined["exact_match"] = True
                        break
                    elif d.get("match_quality") == "good_close" and d.get("lcsh_uri"):
                        combined["lcsh_uri"] = d["lcsh_uri"]
                        combined["lcsh_label"] = d.get("lcsh_label", "")
                        combined["match_quality"] = "good_close"

                merged.append((primary_ref, combined))
                merged_count += len(entries) - 1

            subcats[sub_name] = merged

    if merged_count:
        print(f"  Deduplicated: merged {merged_count} duplicate subjects")
    return categories


def build_taxonomy(mapping):
    """Build hierarchical taxonomy XML using HSG topic headings.

    All subjects are categorized into the 11 official Office of the
    Historian topic headings via keyword matching against the subject
    name and LCSH authorized form.  LCSH broader-term metadata is
    preserved on each subject element but no longer drives the
    top-level category structure.
    """

    # cat_name -> {sub_name -> [(ref, data), ...]}
    categories = {}
    uncategorized = []

    # Load category overrides (manual assignments from reviewed XML)
    cat_overrides = {}  # ref -> (to_category, to_subcategory)
    if os.path.exists(CATEGORY_OVERRIDES_FILE):
        with open(CATEGORY_OVERRIDES_FILE) as f:
            for entry in json.load(f):
                cat_overrides[entry["ref"]] = (entry["to_category"], entry["to_subcategory"])
        print(f"  Loaded {len(cat_overrides)} category overrides")

    for ref, data in mapping.items():
        name = data.get("name", "")
        lcsh_label = data.get("lcsh_label") if data.get("match_quality") in ("exact", "good_close") else None

        # Check for manual override first
        if ref in cat_overrides:
            cat_name, sub_name = cat_overrides[ref]
        else:
            cat_name, sub_name = categorize_by_hsg(name, lcsh_label)

        if cat_name and cat_name != "Uncategorized":
            categories.setdefault(cat_name, {}).setdefault(sub_name, []).append((ref, data))
        else:
            uncategorized.append((ref, data))

    overridden = sum(1 for ref in mapping if ref in cat_overrides)
    if cat_overrides:
        print(f"  Applied {overridden} category overrides")

    # Deduplicate near-identical subjects within each subcategory
    categories = deduplicate_subjects(categories)

    # Build XML
    from datetime import date
    root = etree.Element("taxonomy", attrib={
        "source": "hsg-annotate-data",
        "authority": "Office of the Historian (history.state.gov)",
        "authority-uri": "https://history.state.gov/tags/all",
        "generated": date.today().isoformat(),
        "total-subjects": str(len(mapping)),
    })

    # Sort categories by total annotation count
    sorted_cats = sorted(
        categories.items(),
        key=lambda x: sum(
            int(s[1].get("count", 0))
            for subs in x[1].values() for s in subs
        ),
        reverse=True,
    )

    for cat_name, subcats in sorted_cats:
        cat_total = sum(
            int(s[1].get("count", 0))
            for subs in subcats.values() for s in subs
        )
        cat_count = sum(len(subs) for subs in subcats.values())

        cat_elem = etree.SubElement(root, "category", attrib={
            "label": cat_name,
            "total-annotations": str(cat_total),
            "total-subjects": str(cat_count),
        })

        # Sort subcategories by annotation count
        sorted_subs = sorted(
            subcats.items(),
            key=lambda x: sum(int(s[1].get("count", 0)) for s in x[1]),
            reverse=True,
        )

        for sub_name, subjects in sorted_subs:
            sub_total = sum(int(s[1].get("count", 0)) for s in subjects)
            sub_elem = etree.SubElement(cat_elem, "subcategory", attrib={
                "label": sub_name,
                "total-annotations": str(sub_total),
                "total-subjects": str(len(subjects)),
            })

            # Sort subjects by count within each subcategory
            sorted_subjects = sorted(
                subjects,
                key=lambda x: int(x[1].get("count", 0)),
                reverse=True,
            )

            for ref, sdata in sorted_subjects:
                attribs = {
                    "ref": ref,
                    "type": sdata.get("type", "topic"),
                    "count": str(sdata.get("count", 0)),
                    "volumes": str(sdata.get("volumes", "")),
                }
                if sdata.get("lcsh_uri") and sdata.get("match_quality") in ("exact", "good_close"):
                    attribs["lcsh-uri"] = sdata["lcsh_uri"]
                    attribs["lcsh-match"] = sdata.get("match_quality", "exact")

                subj_elem = etree.SubElement(sub_elem, "subject", **attribs)

                name_elem = etree.SubElement(subj_elem, "name")
                name_elem.text = sdata.get("name", "")

                if sdata.get("lcsh_label") and sdata["lcsh_label"] != sdata.get("name") and sdata.get("match_quality") in ("exact", "good_close"):
                    lcsh_form = etree.SubElement(subj_elem, "lcsh-authorized-form")
                    lcsh_form.text = sdata["lcsh_label"]

                if sdata.get("appears_in"):
                    ai_elem = etree.SubElement(subj_elem, "appearsIn")
                    ai_elem.text = sdata["appears_in"]

                # Include document-level appearance data
                doc_apps = sdata.get("document_appearances", {})
                if doc_apps:
                    docs_elem = etree.SubElement(subj_elem, "documents")
                    for vol_id, doc_ids in sorted(doc_apps.items()):
                        vol_elem = etree.SubElement(docs_elem, "volume", id=vol_id)
                        vol_elem.text = ", ".join(doc_ids)

    # Uncategorized — same structure as other categories
    if uncategorized:
        uncat_total = sum(int(d.get("count", 0)) for _, d in uncategorized)
        uncat_cat = etree.SubElement(root, "category", attrib={
            "label": "Uncategorized",
            "total-annotations": str(uncat_total),
            "total-subjects": str(len(uncategorized)),
        })
        uncat_sub = etree.SubElement(uncat_cat, "subcategory", attrib={
            "label": "General",
            "total-annotations": str(uncat_total),
            "total-subjects": str(len(uncategorized)),
        })
        sorted_uncat = sorted(uncategorized, key=lambda x: int(x[1].get("count", 0)), reverse=True)
        for ref, sdata in sorted_uncat:
            attribs = {
                "ref": ref,
                "type": sdata.get("type", "topic"),
                "count": str(sdata.get("count", 0)),
                "volumes": str(sdata.get("volumes", "")),
            }
            if sdata.get("lcsh_uri") and sdata.get("match_quality") in ("exact", "good_close"):
                attribs["lcsh-uri"] = sdata["lcsh_uri"]
                attribs["lcsh-match"] = sdata.get("match_quality", "exact")

            subj_elem = etree.SubElement(uncat_sub, "subject", **attribs)

            name_elem = etree.SubElement(subj_elem, "name")
            name_elem.text = sdata.get("name", "")

            if sdata.get("lcsh_label") and sdata["lcsh_label"] != sdata.get("name") and sdata.get("match_quality") in ("exact", "good_close"):
                lcsh_form = etree.SubElement(subj_elem, "lcsh-authorized-form")
                lcsh_form.text = sdata["lcsh_label"]

            if sdata.get("appears_in"):
                ai_elem = etree.SubElement(subj_elem, "appearsIn")
                ai_elem.text = sdata["appears_in"]

            # Include document-level appearance data
            doc_apps = sdata.get("document_appearances", {})
            if doc_apps:
                docs_elem = etree.SubElement(subj_elem, "documents")
                for vol_id, doc_ids in sorted(doc_apps.items()):
                    vol_elem = etree.SubElement(docs_elem, "volume", id=vol_id)
                    vol_elem.text = ", ".join(doc_ids)

    # Write XML
    tree = etree.ElementTree(root)
    etree.indent(tree, space="    ")
    tree.write(OUTPUT_TAXONOMY, xml_declaration=True, encoding="UTF-8", pretty_print=True)

    print(f"\nTaxonomy written to: {OUTPUT_TAXONOMY}")
    print(f"  Categories: {len(categories)}")
    for cat_name, subcats in sorted_cats:
        cat_count = sum(len(subs) for subs in subcats.values())
        print(f"    {cat_name}: {cat_count} subjects")
        for sub_name, subjects in sorted(subcats.items(),
                key=lambda x: sum(int(s[1].get("count", 0)) for s in x[1]),
                reverse=True):
            print(f"      {sub_name}: {len(subjects)}")
    print(f"  Uncategorized: {len(uncategorized)}")

    return OUTPUT_TAXONOMY


if __name__ == "__main__":
    with open(MAPPING_FILE) as f:
        mapping = json.load(f)

    # Merge document-level appearances into the mapping
    if os.path.exists(DOC_APPEARANCES_FILE):
        with open(DOC_APPEARANCES_FILE) as f:
            doc_apps = json.load(f)
        for ref, volumes in doc_apps.items():
            if ref in mapping:
                mapping[ref]["document_appearances"] = volumes
        print(f"Merged document appearances for {len(doc_apps)} subjects")

    # Apply global dedup decisions before anything else
    print("=" * 60)
    print("Applying dedup decisions")
    print("=" * 60)
    mapping = apply_dedup_decisions(mapping)

    step = sys.argv[1] if len(sys.argv) > 1 else "all"

    if step in ("fetch", "all"):
        print("=" * 60)
        print("Fetching 2-level LCSH broader terms")
        print("=" * 60)
        mapping = fetch_two_level_hierarchy(mapping)

    if step in ("build", "all"):
        print("\n" + "=" * 60)
        print("Building taxonomy XML")
        print("=" * 60)
        build_taxonomy(mapping)
