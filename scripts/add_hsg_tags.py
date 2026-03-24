#!/usr/bin/env python3
"""One-time script to add 30 HSG-only subject tags to the taxonomy XML."""

import re

TAXONOMY = "subject-taxonomy-lcsh.xml"

# tag -> (category, subcategory, display_name)
# subcategory=None means add to "General" subcat of that category
NEW_TAGS = [
    # Politico-Military Issues
    ("national-security-council", "Politico-Military Issues", "General", "National Security Council"),
    ("national-security-policy", "Politico-Military Issues", "General", "National Security Policy"),
    ("military-intervention-presence-and-withdrawal", "Politico-Military Issues", "General", "Military Intervention, Presence, and Withdrawal"),
    ("collective-security", "Politico-Military Issues", "Alliances", "Collective Security"),
    ("protection-of-americans-abroad", "Politico-Military Issues", "General", "Protection of Americans Abroad"),

    # International Organizations -- new subcategories
    ("united-nations", "International Organizations", "United Nations", "United Nations"),
    ("conference-on-security-and-cooperation-in-europe", "International Organizations", "Conference on Security and Cooperation in Europe", "Conference on Security and Cooperation in Europe"),
    ("european-economic-community", "International Organizations", "European Economic Community", "European Economic Community"),
    ("organization-of-petroleum-exporting-countries", "International Organizations", "Organization of Petroleum Exporting Countries", "Organization of Petroleum Exporting Countries"),
    ("southeast-asia-treaty-organization", "International Organizations", "Southeast Asia Treaty Organization", "Southeast Asia Treaty Organization"),
    ("association-of-southeast-asian-nations", "International Organizations", "Association of Southeast Asian Nations", "Association of Southeast Asian Nations"),
    ("universal-postal-union", "International Organizations", "Universal Postal Union", "Universal Postal Union"),

    # Human Rights
    ("civil-rights-domestic", "Human Rights", "Civil Rights", "Civil Rights (Domestic)"),
    ("antisemitism-post-world-war-ii", "Human Rights", "Antisemitism", "Antisemitism (Post-World War II)"),
    ("detainees", "Human Rights", "General", "Detainees"),
    ("disability-rights", "Human Rights", "General", "Disability Rights"),
    ("lgbtq-rights", "Human Rights", "General", "LGBTQ Rights"),

    # Foreign Economic Policy
    ("economic-summit-meetings", "Foreign Economic Policy", "General", "Economic Summit Meetings"),

    # Department of State / Government
    ("congressional-relations", "Department of State", "General", "Congressional Relations"),
    ("personnel-demographics", "Department of State", "General", "Personnel Demographics"),
    ("foreign-protest-against-u-s-activity", "Department of State", "General", "Foreign Protest Against U.S. Activity"),

    # Bilateral Relations -- add to existing subcats where names are close
    ("western-hemisphere", "Bilateral Relations", "Western Hemisphere Relations", "Western Hemisphere"),
    ("diplomatic-recognition", "Bilateral Relations", "General", "Diplomatic Recognition"),
    ("sub-saharan-africa", "Bilateral Relations", "General", "Sub-Saharan Africa"),
    ("east-asia-and-pacific", "Bilateral Relations", "East Asian Relations", "East Asia and Pacific"),
    ("near-east", "Bilateral Relations", "Middle East and North African Relations", "Near East"),
    ("south-and-central-asia", "Bilateral Relations", "South Asian Relations", "South and Central Asia"),

    # Warfare
    ("war-crimes-and-war-criminals", "Warfare", "General", "War Crimes and War Criminals"),
    ("american-revolutionary-war", "Warfare", "American Revolutionary War", "American Revolutionary War"),
    ("war-of-1812", "Warfare", "War of 1812", "War of 1812"),
]

def make_subject_xml(slug, name, indent="            "):
    """Create a minimal subject entry for an HSG-discovered tag."""
    return (
        f'{indent}<subject type="topic" count="0" volumes="0" source="hsg-tags">\n'
        f'{indent}    <name>{name}</name>\n'
        f'{indent}</subject>'
    )


def make_subcategory_xml(label, slug, name, indent="        "):
    """Create a new subcategory with one subject entry."""
    subj = make_subject_xml(slug, name, indent + "    ")
    return (
        f'{indent}<subcategory label="{label}" total-annotations="0" total-subjects="1">\n'
        f'{subj}\n'
        f'{indent}</subcategory>'
    )


def main():
    with open(TAXONOMY, "r", encoding="utf-8") as f:
        content = f.read()

    lines = content.split("\n")

    # Build index: (category, subcategory) -> line number of </subcategory>
    # and category -> line number of </category>
    subcat_ends = {}  # (cat_label, subcat_label) -> line_idx of </subcategory>
    cat_ends = {}     # cat_label -> line_idx of </category>
    current_cat = None
    current_subcat = None

    for i, line in enumerate(lines):
        m = re.match(r'\s*<category label="([^"]+)"', line)
        if m:
            current_cat = m.group(1)
        m = re.match(r'\s*<subcategory label="([^"]+)"', line)
        if m:
            current_subcat = m.group(1)
        if "</subcategory>" in line and current_cat and current_subcat:
            subcat_ends[(current_cat, current_subcat)] = i
        if "</category>" in line and current_cat:
            cat_ends[current_cat] = i

    # Group insertions by location
    # For existing subcategories: insert subject before </subcategory>
    # For new subcategories: insert before </category>
    insertions = {}  # line_idx -> list of xml strings to insert before that line

    for slug, cat, subcat, name in NEW_TAGS:
        key = (cat, subcat)
        if key in subcat_ends:
            # Existing subcategory - add subject before closing tag
            idx = subcat_ends[key]
            xml = make_subject_xml(slug, name)
            insertions.setdefault(idx, []).append(xml)
        elif cat in cat_ends:
            # New subcategory - add before </category>
            idx = cat_ends[cat]
            xml = make_subcategory_xml(subcat, slug, name)
            insertions.setdefault(idx, []).append(xml)
        else:
            print(f"WARNING: category '{cat}' not found for {slug}")

    # Apply insertions in reverse order so line numbers stay valid
    for idx in sorted(insertions.keys(), reverse=True):
        for xml in reversed(insertions[idx]):
            lines.insert(idx, xml)

    with open(TAXONOMY, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Added {len(NEW_TAGS)} subject entries to {TAXONOMY}")


if __name__ == "__main__":
    main()
