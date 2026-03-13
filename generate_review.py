#!/usr/bin/env python3
"""
Generate a review TSV of LCSH mappings for human verification.
Also generates a cleaned mapping with only reliable matches.
"""

import json
import csv
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

MAPPING_FILE = "lcsh_mapping.json"
REVIEW_FILE = "lcsh_review.tsv"
CLEAN_MAPPING_FILE = "lcsh_mapping_clean.json"

with open(MAPPING_FILE) as f:
    mapping = json.load(f)

# Classify matches into quality tiers
exact_matches = {}      # Name matches LCSH label exactly
good_close = {}         # Close match but clearly correct (pluralization, qualifiers)
bad_close = {}          # Close match but wrong subject
no_match = {}           # No LCSH match

# Rules for auto-classifying close matches
def is_good_close_match(name, lcsh_label):
    """Heuristic: is this close match likely correct?"""
    name_l = name.lower().strip()
    lcsh_l = lcsh_label.lower().strip()

    # Pluralization: "Summit meeting" → "Summit meetings"
    if lcsh_l == name_l + "s" or lcsh_l == name_l + "es":
        return True
    if name_l == lcsh_l + "s" or name_l == lcsh_l + "es":
        return True

    # LCSH qualifier in parens: "Deterrence" → "Deterrence (Strategy)"
    if lcsh_l.startswith(name_l + " (") or lcsh_l.startswith(name_l + ","):
        return True

    # "National" added: "Self-determination" → "Self-determination, National"
    if name_l in lcsh_l and len(lcsh_l) - len(name_l) < 15:
        return True

    # Treaty/law forms: "Treaty ratification" → "Treaties--Ratification"
    name_words = set(name_l.split())
    lcsh_words = set(lcsh_l.replace("--", " ").split())
    overlap = name_words & lcsh_words
    if len(overlap) >= len(name_words) * 0.6:
        return True

    return False


def is_bad_close_match(name, lcsh_label):
    """Heuristic: is this close match clearly wrong?"""
    name_l = name.lower().strip()
    lcsh_l = lcsh_label.lower().strip()

    # LCSH label is about a totally different thing
    # If name is a single common word, be suspicious of compound matches
    if len(name_l.split()) == 1 and len(lcsh_l.split()) >= 3:
        return True

    # If LCSH label contains geographic qualifiers not in name
    if "(" in lcsh_l and ")" in lcsh_l:
        qualifier = lcsh_l[lcsh_l.index("("):lcsh_l.index(")")+1].lower()
        geo_words = ["conn.", "ohio", "turkey", "spain", "n.y.", "mass.", "calif."]
        if any(g in qualifier for g in geo_words):
            return True

    # Name words don't appear in LCSH label at all
    name_words = set(name_l.split())
    lcsh_words = set(lcsh_l.replace("--", " ").replace(",", "").split())
    overlap = name_words & lcsh_words
    if len(overlap) == 0:
        return True

    return False


# Classify all entries
stats = {"exact": 0, "good_close": 0, "bad_close": 0, "ambiguous_close": 0, "no_match": 0}

for ref, data in mapping.items():
    if not data.get("lcsh_uri"):
        no_match[ref] = data
        data["match_quality"] = "no_match"
        stats["no_match"] += 1
    elif data.get("exact_match"):
        exact_matches[ref] = data
        data["match_quality"] = "exact"
        stats["exact"] += 1
    elif is_bad_close_match(data["name"], data["lcsh_label"]):
        bad_close[ref] = data
        data["match_quality"] = "bad_close"
        stats["bad_close"] += 1
    elif is_good_close_match(data["name"], data["lcsh_label"]):
        good_close[ref] = data
        data["match_quality"] = "good_close"
        stats["good_close"] += 1
    else:
        data["match_quality"] = "ambiguous_close"
        stats["ambiguous_close"] += 1

print("Match quality breakdown:")
for k, v in stats.items():
    print(f"  {k}: {v}")

# Save match_quality back to main mapping
with open(MAPPING_FILE, "w") as f:
    json.dump(mapping, f, indent=2)
print(f"Updated {MAPPING_FILE} with match_quality tags")

# Write review TSV
with open(REVIEW_FILE, "w", newline="") as f:
    writer = csv.writer(f, delimiter="\t")
    writer.writerow([
        "ref", "annotation_name", "count", "volumes", "type",
        "lcsh_label", "lcsh_uri", "match_quality",
        "broader_terms", "all_suggestions"
    ])

    # Sort by: match_quality category, then by count descending
    quality_order = {"exact": 0, "good_close": 1, "ambiguous_close": 2, "bad_close": 3, "no_match": 4}
    sorted_entries = sorted(
        mapping.items(),
        key=lambda x: (quality_order.get(x[1].get("match_quality", "no_match"), 5), -x[1].get("count", 0))
    )

    for ref, data in sorted_entries:
        bt_chain = ""
        if data.get("broader_terms"):
            bt_chain = " → ".join(bt.get("label", "") for bt in data["broader_terms"])

        suggestions = "; ".join(data.get("all_suggestions", []))

        writer.writerow([
            ref,
            data.get("name", ""),
            data.get("count", 0),
            data.get("volumes", ""),
            data.get("type", ""),
            data.get("lcsh_label", ""),
            data.get("lcsh_uri", ""),
            data.get("match_quality", ""),
            bt_chain,
            suggestions,
        ])

print(f"\nWrote review file: {REVIEW_FILE}")

# Build clean mapping (only exact + good_close matches)
clean_mapping = {}
for ref, data in mapping.items():
    quality = data.get("match_quality", "no_match")
    if quality in ("exact", "good_close"):
        clean_mapping[ref] = data

with open(CLEAN_MAPPING_FILE, "w") as f:
    json.dump(clean_mapping, f, indent=2)

print(f"Clean mapping ({len(clean_mapping)} entries) saved to: {CLEAN_MAPPING_FILE}")
print(f"  Exact matches: {stats['exact']}")
print(f"  Good close matches: {stats['good_close']}")
print(f"  Rejected: {stats['bad_close'] + stats['ambiguous_close']} close + {stats['no_match']} unmatched")
