#!/usr/bin/env node
/**
 * Generate FRUS Subject Taxonomy process documentation as a Word document.
 */
const fs = require("fs");
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, AlignmentType, LevelFormat,
  TableOfContents, HeadingLevel, BorderStyle, WidthType, ShadingType,
  PageBreak, PageNumber,
} = require("docx");

// ── Shared constants ──────────────────────────────────────────
const PAGE_W = 12240; // US Letter
const PAGE_H = 15840;
const MARGIN = 1440;  // 1 inch
const CONTENT_W = PAGE_W - 2 * MARGIN; // 9360
const FONT = "Arial";
const FONT_SERIF = "Georgia";
const BLUE = "205493";
const DARK = "112E51";
const GRAY = "71767A";
const LIGHT_BLUE = "D5E8F0";
const LIGHT_GRAY = "F0F0F0";

const border = { style: BorderStyle.SINGLE, size: 1, color: "CCCCCC" };
const borders = { top: border, bottom: border, left: border, right: border };
const cellMargins = { top: 60, bottom: 60, left: 100, right: 100 };

// ── Helpers ───────────────────────────────────────────────────
function heading1(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(text)] });
}
function heading2(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(text)] });
}
function heading3(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun(text)] });
}
function para(text, opts = {}) {
  return new Paragraph({
    spacing: { after: 120 },
    ...opts,
    children: [new TextRun({ font: FONT, size: 22, ...opts.run, text })],
  });
}
function boldPara(label, text) {
  return new Paragraph({
    spacing: { after: 120 },
    children: [
      new TextRun({ font: FONT, size: 22, bold: true, text: label }),
      new TextRun({ font: FONT, size: 22, text }),
    ],
  });
}
function bullet(text, ref = "bullets", level = 0) {
  return new Paragraph({
    numbering: { reference: ref, level },
    children: [new TextRun({ font: FONT, size: 22, text })],
  });
}
function bulletBold(label, text, ref = "bullets", level = 0) {
  return new Paragraph({
    numbering: { reference: ref, level },
    children: [
      new TextRun({ font: FONT, size: 22, bold: true, text: label }),
      new TextRun({ font: FONT, size: 22, text }),
    ],
  });
}
function emptyPara() {
  return new Paragraph({ spacing: { after: 60 }, children: [] });
}

function headerCell(text, width) {
  return new TableCell({
    borders,
    width: { size: width, type: WidthType.DXA },
    shading: { fill: DARK, type: ShadingType.CLEAR },
    margins: cellMargins,
    children: [new Paragraph({ children: [new TextRun({ font: FONT, size: 20, bold: true, color: "FFFFFF", text })] })],
  });
}
function dataCell(text, width, opts = {}) {
  return new TableCell({
    borders,
    width: { size: width, type: WidthType.DXA },
    shading: opts.shading ? { fill: opts.shading, type: ShadingType.CLEAR } : undefined,
    margins: cellMargins,
    children: [new Paragraph({
      alignment: opts.align,
      children: [new TextRun({ font: FONT, size: 20, bold: opts.bold, text: String(text) })],
    })],
  });
}

// ── Category data (from the build output) ─────────────────────
const categories = [
  { name: "Arms Control and Disarmament", subjects: 166, annotations: 27405, subcats: ["Nuclear Weapons (104)", "General (20)", "Nuclear Nonproliferation (22)", "Confidence-Building Measures (6)", "Arms Transfers (5)", "Chemical and Bacteriological Warfare (5)", "Collective Security (3)", "Arms Embargoes (1)"] },
  { name: "Foreign Economic Policy", subjects: 179, annotations: 6883, subcats: ["Trade and Commercial Policy/Agreements (35)", "Energy and Natural Resources (44)", "General (25)", "Agriculture (24)", "Financial and Monetary Policy (19)", "Foreign Aid (11)", "Foreign Investment (10)", "Labor (5)", "Economic Sanctions (3)", "New International Economic Order (3)"] },
  { name: "Bilateral Relations", subjects: 54, annotations: 5713, subcats: ["Middle East and North African Relations (13)", "Western Hemisphere Relations (11)", "General (11)", "U.S.-Soviet/Russian Relations (10)", "NATO and European Relations (5)", "East Asian Relations (3)", "South Asian Relations (1)"] },
  { name: "Human Rights", subjects: 58, annotations: 5342, subcats: ["General (22)", "Refugees (11)", "Political Prisoners (9)", "Religious Freedom (7)", "HIV/AIDS (3)", "Antisemitism (2)", "Asylum (2)", "Civil Rights (1)", "Genocide (1)"] },
  { name: "Politico-Military Issues", subjects: 133, annotations: 4818, subcats: ["General (104)", "Terrorism (8)", "Military Intervention, Presence and Withdrawal (7)", "Covert Action (5)", "Alliances (3)", "Military Bases (3)", "Quarantine (Blockade) (2)", "Diplomatic Recognition (1)"] },
  { name: "Global Issues", subjects: 105, annotations: 4230, subcats: ["Public Health (23)", "General (21)", "Immigration (10)", "Narcotics (10)", "Population Demographics (9)", "Elections (7)", "Air Safety (6)", "Border Administration (5)", "Self-Determination (5)", "Peace (4)", "Outer Space (2)", "Decolonization (2)", "Polar Affairs (1)"] },
  { name: "International Law", subjects: 51, annotations: 3001, subcats: ["Law of the Sea (26)", "General (21)", "Property Claims (3)", "Foreign Protest against U.S. Activity (1)"] },
  { name: "Science and Technology", subjects: 43, annotations: 2359, subcats: ["General (31)", "Atomic Energy (12)"] },
  { name: "Information Programs", subjects: 26, annotations: 844, subcats: ["General (26)"] },
  { name: "Department of State", subjects: 13, annotations: 600, subcats: ["General (5)", "Organization and Management (3)", "Protocol (1)", "Personnel: Foreign Service (1)", "Congressional Relations (1)", "Buildings: Foreign (1)", "Personnel: Civil Service (1)"] },
  { name: "Warfare", subjects: 25, annotations: 304, subcats: ["General (18)", "Vietnam Conflict (3)", "Arab-Israeli Dispute (1)", "Prisoners of War (1)", "Iraq War (2003) (1)", "Neutrality (1)"] },
  { name: "International Organizations", subjects: 10, annotations: 208, subcats: ["General (3)", "Organization of American States (2)", "Non-governmental Organizations (2)", "International Monetary Fund (1)", "General Agreement on Tariffs and Trade (1)", "North Atlantic Treaty Organization (1)"] },
];

const volumes = [
  ["frus1969-76v19p2", "Japan, 1969-1976"],
  ["frus1977-80v11p1", "Iran: Hostage Crisis, November 1979-September 1980"],
  ["frus1977-80v15", "Central America, 1977-1980"],
  ["frus1977-80v17p3", "North Africa"],
  ["frus1977-80v19", "South Asia"],
  ["frus1977-80v22", "Southeast Asia and the Pacific"],
  ["frus1977-80v23", "Mexico, Cuba, and the Caribbean"],
  ["frus1977-80v24", "South America; Latin America Region"],
  ["frus1977-80v27", "Federal Republic of Germany"],
  ["frus1981-88v04", "Soviet Union, January 1983-March 1985"],
  ["frus1981-88v05", "Soviet Union, March 1985-October 1986"],
  ["frus1981-88v06", "Soviet Union, October 1986-January 1989"],
  ["frus1981-88v11", "START I"],
  ["frus1981-88v41", "Global Issues II"],
];

// ── Build document ────────────────────────────────────────────
async function main() {
  const children = [];

  // ── Title page ──────────────────────────────────────────────
  children.push(emptyPara(), emptyPara(), emptyPara(), emptyPara(), emptyPara(), emptyPara());
  children.push(new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 200 },
    children: [new TextRun({ font: FONT_SERIF, size: 52, bold: true, color: DARK, text: "FRUS Subject Taxonomy" })],
  }));
  children.push(new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 100 },
    children: [new TextRun({ font: FONT_SERIF, size: 36, color: DARK, text: "Process Documentation" })],
  }));
  children.push(new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 600 },
    border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: BLUE, space: 1 } },
    children: [new TextRun({ font: FONT, size: 24, color: GRAY, text: "Building a Controlled Subject Vocabulary for the" })],
  }));
  children.push(new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 200 },
    children: [new TextRun({ font: FONT, size: 24, color: GRAY, text: "Foreign Relations of the United States Series" })],
  }));
  children.push(emptyPara(), emptyPara(), emptyPara(), emptyPara());
  children.push(new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 100 },
    children: [new TextRun({ font: FONT, size: 24, color: GRAY, text: "Office of the Historian" })],
  }));
  children.push(new Paragraph({
    alignment: AlignmentType.CENTER,
    spacing: { after: 100 },
    children: [new TextRun({ font: FONT, size: 24, color: GRAY, text: "U.S. Department of State" })],
  }));
  children.push(new Paragraph({
    alignment: AlignmentType.CENTER,
    children: [new TextRun({ font: FONT, size: 24, color: GRAY, text: "March 2026" })],
  }));

  // ── Page break + TOC ────────────────────────────────────────
  children.push(new Paragraph({ children: [new PageBreak()] }));
  children.push(heading1("Table of Contents"));
  children.push(new TableOfContents("Table of Contents", { hyperlink: true, headingStyleRange: "1-3" }));
  children.push(new Paragraph({ children: [new PageBreak()] }));

  // ═══════════════════════════════════════════════════════════
  // 1. INTRODUCTION
  // ═══════════════════════════════════════════════════════════
  children.push(heading1("1. Introduction"));
  children.push(para(
    "This document describes the methodology and tooling used to build a hierarchical subject taxonomy for the Foreign Relations of the United States (FRUS) series. The taxonomy organizes 1,288 subject terms drawn from manual annotations of FRUS volumes into 12 thematic categories aligned with the Office of the Historian's topic headings."
  ));
  children.push(para(
    "The project addresses a core discoverability challenge: FRUS volumes contain thousands of subject annotations created during the editorial process, but these annotations exist as flat, unstructured lists. The taxonomy provides a structured, browsable hierarchy that enables researchers to find documents by topic across multiple volumes."
  ));

  children.push(heading2("1.1 Scope"));
  children.push(para("The current taxonomy covers:"));
  children.push(bullet("1,412 raw subject terms from 14 annotated FRUS volumes"));
  children.push(bullet("1,288 subjects after deduplication (124 entries merged)"));
  children.push(bullet("65,571 total document-level annotations"));
  children.push(bullet("4,509 individual documents across 14 volumes"));
  children.push(bullet("864 subjects categorized into 12 thematic categories"));
  children.push(bullet("424 subjects remaining uncategorized"));

  children.push(heading2("1.2 Relationship to history.state.gov"));
  children.push(para(
    "The taxonomy is designed to support the subject browsing interface at history.state.gov. The 12 top-level categories correspond to the official Office of the Historian topic headings used across the site. An interactive mockup demonstrates how the taxonomy could be integrated into the existing site design."
  ));

  // ═══════════════════════════════════════════════════════════
  // 2. SOURCE DATA
  // ═══════════════════════════════════════════════════════════
  children.push(new Paragraph({ children: [new PageBreak()] }));
  children.push(heading1("2. Source Data"));

  children.push(heading2("2.1 Annotated FRUS Volumes"));
  children.push(para(
    "Subject annotations originate from TEI XML-encoded FRUS volumes. Annotators tagged passages with subject terms using <rs> (referencing string) elements containing Airtable record IDs as unique identifiers. Each annotation links a passage within a specific document to a controlled subject term."
  ));

  children.push(heading3("Volumes Included"));
  // Volumes table
  const volRows = [
    new TableRow({
      children: [
        headerCell("Volume ID", 2800),
        headerCell("Coverage", 6560),
      ],
    }),
  ];
  for (const [vid, title] of volumes) {
    volRows.push(new TableRow({
      children: [
        dataCell(vid, 2800, { bold: true }),
        dataCell(title, 6560),
      ],
    }));
  }
  children.push(new Table({
    width: { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: [2800, 6560],
    rows: volRows,
  }));
  children.push(emptyPara());

  children.push(heading2("2.2 Subject Records"));
  children.push(para(
    "Each subject is identified by an Airtable record ID (e.g., recVSPQM8CicmOme2). The source mapping file (lcsh_mapping.json) contains 1,412 subject entries with the following metadata for each:"
  ));
  children.push(bulletBold("name: ", "The display name of the subject (e.g., \"Human rights\")"));
  children.push(bulletBold("type: ", "The annotation type (\"topic\" or \"compound-subject\")"));
  children.push(bulletBold("count: ", "Total number of document-level annotations"));
  children.push(bulletBold("volumes: ", "Number of FRUS volumes in which the subject appears"));
  children.push(bulletBold("appears_in: ", "Comma-separated list of volume IDs"));

  children.push(heading2("2.3 Document Metadata"));
  children.push(para(
    "Document-level metadata (titles, dates, sender/recipient information) was extracted from the TEI XML source files. This metadata enables the mockup to display full document details when browsing by subject, and links directly to the published documents on history.state.gov."
  ));

  // ═══════════════════════════════════════════════════════════
  // 3. LCSH MATCHING
  // ═══════════════════════════════════════════════════════════
  children.push(new Paragraph({ children: [new PageBreak()] }));
  children.push(heading1("3. LCSH Matching"));
  children.push(para(
    "Each subject term was matched against the Library of Congress Subject Headings (LCSH) to establish connections to a widely-used controlled vocabulary. This process serves two purposes: it validates subject terms against an authoritative source, and it provides access to the LCSH hierarchical structure for potential future use in categorization."
  ));

  children.push(heading2("3.1 Methodology"));
  children.push(para(
    "Subject names were queried against the id.loc.gov suggest2 API, which returns candidate LCSH headings ranked by relevance. Each result was classified into one of three match quality levels:"
  ));

  const matchRows = [
    new TableRow({ children: [headerCell("Match Quality", 2000), headerCell("Count", 1200), headerCell("Description", 6160)] }),
    new TableRow({ children: [dataCell("exact", 2000, { bold: true }), dataCell("308", 1200, { align: AlignmentType.RIGHT }), dataCell("Subject name matches an LCSH authorized heading exactly", 6160)] }),
    new TableRow({ children: [dataCell("good_close", 2000, { bold: true }), dataCell("134", 1200, { align: AlignmentType.RIGHT }), dataCell("Strong semantic match to an LCSH heading (e.g., minor phrasing differences)", 6160)] }),
    new TableRow({ children: [dataCell("no_match", 2000, { bold: true }), dataCell("970", 1200, { align: AlignmentType.RIGHT }), dataCell("No suitable LCSH heading found; subject is domain-specific to FRUS", 6160)] }),
  ];
  children.push(new Table({
    width: { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: [2000, 1200, 6160],
    rows: matchRows,
  }));
  children.push(emptyPara());

  children.push(para(
    "The high proportion of no-match results (69%) reflects the specialized nature of FRUS subject terms. Many terms refer to specific weapon systems (e.g., \"Air-launched cruise missiles\"), negotiation frameworks (e.g., \"Strategic offensive arms reductions\"), or policy-specific concepts that do not have corresponding LCSH headings."
  ));

  children.push(heading2("3.2 Broader-Term Hierarchy"));
  children.push(para(
    "For subjects with exact or good_close LCSH matches, the system fetched up to two levels of broader terms from the LCSH hierarchy using the SKOS (Simple Knowledge Organization System) JSON-LD endpoint at id.loc.gov. This broader-term data was explored as a potential basis for automatic categorization but was ultimately not used for the top-level category structure, which relies instead on domain-specific keyword matching aligned to the HSG topic headings."
  ));
  children.push(para(
    "Broader-term data is cached locally in lcsh_broader_cache.json to avoid repeated API calls. The cache currently contains entries for approximately 440 LCSH URIs."
  ));

  // ═══════════════════════════════════════════════════════════
  // 4. CATEGORIZATION FRAMEWORK
  // ═══════════════════════════════════════════════════════════
  children.push(new Paragraph({ children: [new PageBreak()] }));
  children.push(heading1("4. Categorization Framework"));
  children.push(para(
    "Subjects are organized into 12 top-level categories corresponding to the Office of the Historian's official topic headings. Within each category, subjects are further grouped into subcategories based on more specific keyword patterns."
  ));

  children.push(heading2("4.1 Keyword-Scoring Algorithm"));
  children.push(para(
    "Categorization uses a two-stage keyword-scoring algorithm. For each subject, the system tests the subject's name and its LCSH authorized form (if available) against keyword lists defined for each category and subcategory."
  ));
  children.push(para(
    "The scoring function performs case-insensitive substring matching. When a keyword is found within the subject text, the keyword's character length is added to the score. This length-weighting ensures that longer, more specific keywords outscore shorter, more generic ones. For example, \"intercontinental ballistic missiles\" (36 characters) would outscore a generic match on \"missile\" (7 characters)."
  ));

  children.push(heading3("Stage 1: Category Assignment"));
  children.push(para(
    "The system evaluates all category-level and subcategory-level keywords for each of the 12 categories. The category with the highest cumulative score is selected. If no category scores above zero, the subject is placed in \"Uncategorized.\""
  ));

  children.push(heading3("Stage 2: Subcategory Assignment"));
  children.push(para(
    "Within the selected category, the system scores only the subcategory keyword lists. The subcategory with the highest score is chosen; if none match, the subject is placed in the \"General\" subcategory."
  ));

  children.push(heading2("4.2 Manual Category Overrides"));
  children.push(para(
    "Keyword-based categorization correctly assigns the majority of subjects but produces incorrect results in cases where subject names lack distinguishing keywords or where keyword overlap causes assignment to the wrong category. To address this, 314 manual overrides were applied after editorial review:"
  ));
  children.push(bullet("195 subjects moved out of \"Uncategorized\" into appropriate categories"));
  children.push(bullet("119 subjects reassigned between categories to correct misclassifications"));
  children.push(emptyPara());
  children.push(para(
    "Notable override patterns include:"
  ));
  children.push(bullet("\"Verification and verification protocols\" moved from Department of State / Protocol to Arms Control and Disarmament / Nuclear Nonproliferation"));
  children.push(bullet("\"Hostage\" moved from Warfare / General to Politico-Military Issues / Terrorism"));
  children.push(bullet("Food and famine subjects (e.g., \"Hunger,\" \"Food shortages\") moved from Foreign Economic Policy / Agriculture to Global Issues / Public Health"));
  children.push(bullet("Whaling and maritime subjects moved from Global Issues or Science and Technology to International Law / Law of the Sea"));
  children.push(emptyPara());
  children.push(para(
    "Overrides are stored in category_overrides.json and are applied at build time, taking precedence over keyword-based assignment. This approach preserves the automated categorization logic while allowing editorial corrections without modifying the algorithm itself."
  ));

  // ═══════════════════════════════════════════════════════════
  // 5. DEDUPLICATION
  // ═══════════════════════════════════════════════════════════
  children.push(new Paragraph({ children: [new PageBreak()] }));
  children.push(heading1("5. Deduplication"));
  children.push(para(
    "The raw subject list contains numerous duplicates arising from inconsistent naming, variant forms, and overlapping concepts. Deduplication was performed in two phases, each involving automated detection followed by manual editorial review."
  ));

  children.push(heading2("5.1 Phase 1: Name Normalization"));
  children.push(para(
    "The first deduplication pass identifies subjects with effectively identical names after normalization. The normalization algorithm:"
  ));
  children.push(new Paragraph({
    numbering: { reference: "numbers", level: 0 },
    children: [new TextRun({ font: FONT, size: 22, text: "Converts to lowercase and strips whitespace" })],
  }));
  children.push(new Paragraph({
    numbering: { reference: "numbers", level: 0 },
    children: [new TextRun({ font: FONT, size: 22, text: "Removes parenthetical qualifiers (e.g., removing \"(HUMINT)\" from a subject name)" })],
  }));
  children.push(new Paragraph({
    numbering: { reference: "numbers", level: 0 },
    children: [new TextRun({ font: FONT, size: 22, text: "Normalizes hyphens and slashes to spaces" })],
  }));
  children.push(new Paragraph({
    numbering: { reference: "numbers", level: 0 },
    children: [new TextRun({ font: FONT, size: 22, text: "Reduces common plurals to singular form" })],
  }));
  children.push(emptyPara());
  children.push(para(
    "This phase detected 64 groups of near-duplicate subjects. An interactive review tool (dedup-review.html) was used to evaluate each group and decide whether to merge or keep entries separate. A total of 57 groups were marked for merge, 5 were skipped (kept separate), and 2 were resolved through the semantic review phase."
  ));

  children.push(heading2("5.2 Phase 2: Semantic Deduplication"));
  children.push(para(
    "The second pass identifies conceptual duplicates where subjects refer to the same concept but use different phrasing. Detection was performed using word-overlap analysis: after removing stop words, subject name pairs sharing at least two meaningful words and exhibiting 80% or more overlap in their word sets were flagged as potential duplicates."
  ));
  children.push(para("Detected groups were classified into four types:"));
  children.push(bulletBold("Near-identical: ", "Trivial spelling or formatting differences (e.g., \"Defense and Space\" vs. \"Defense and space\")"));
  children.push(bulletBold("Same concept: ", "Different phrasing of the same idea (e.g., \"Foreign aid\" and \"Development assistance\")"));
  children.push(bulletBold("Hierarchical: ", "One term is a narrower form of the other (e.g., \"INF\" and \"Intermediate Range Nuclear Forces\")"));
  children.push(bulletBold("Bilateral: ", "Country-pair relationship terms (e.g., \"U.S.-Soviet relations\" and \"Soviet Union bilateral issues\")"));
  children.push(emptyPara());
  children.push(para(
    "A second review tool (semantic-dedup-review.html) was used for evaluation. From 67 semantic groups, 40 were marked for merge, 23 were skipped, and 4 remain unreviewed."
  ));

  children.push(heading2("5.3 Merge Process"));
  children.push(para("When subjects are merged, the system:"));
  children.push(bullet("Designates one entry as the primary (either the entry with the highest annotation count, or a user-selected primary)"));
  children.push(bullet("Sums annotation counts across all entries in the group"));
  children.push(bullet("Takes the union of all volume appearances and document-level annotations"));
  children.push(bullet("Preserves the best LCSH match (preferring \"exact\" over \"good_close\")"));
  children.push(bullet("Removes all secondary entries from the mapping"));
  children.push(bullet("Records all original record IDs in a merged_refs field for traceability"));
  children.push(emptyPara());
  children.push(para(
    "Combined dedup decisions are stored in dedup_decisions.json (99 merge groups, 3 skip groups) and are applied at build time before categorization."
  ));

  // ═══════════════════════════════════════════════════════════
  // 6. OUTPUT FORMATS
  // ═══════════════════════════════════════════════════════════
  children.push(new Paragraph({ children: [new PageBreak()] }));
  children.push(heading1("6. Output Formats"));

  children.push(heading2("6.1 Taxonomy XML"));
  children.push(para(
    "The primary output is subject-taxonomy-lcsh.xml, a hierarchical XML file containing all categorized and uncategorized subjects with their metadata and document-level appearance data. The structure is:"
  ));
  children.push(emptyPara());

  // XML structure as a simple code-like block
  const xmlLines = [
    "<taxonomy total-subjects=\"1288\">",
    "  <category label=\"...\" total-annotations=\"...\" total-subjects=\"...\">",
    "    <subcategory label=\"...\" total-annotations=\"...\" total-subjects=\"...\">",
    "      <subject ref=\"rec...\" type=\"topic\" count=\"...\" volumes=\"...\">",
    "        <name>Subject Name</name>",
    "        <lcsh-authorized-form>LCSH Heading</lcsh-authorized-form>",
    "        <appearsIn>vol1, vol2, ...</appearsIn>",
    "        <documents>",
    "          <volume id=\"frus...\">d1, d2, d3, ...</volume>",
    "        </documents>",
    "      </subject>",
    "    </subcategory>",
    "  </category>",
    "</taxonomy>",
  ];
  for (const line of xmlLines) {
    children.push(new Paragraph({
      spacing: { after: 0 },
      indent: { left: 360 },
      children: [new TextRun({ font: "Courier New", size: 18, text: line })],
    }));
  }
  children.push(emptyPara());

  children.push(para("Key attributes on subject elements:"));
  children.push(bulletBold("ref: ", "Airtable record ID (unique identifier)"));
  children.push(bulletBold("type: ", "\"topic\" or \"compound-subject\""));
  children.push(bulletBold("count: ", "Total number of document-level annotations"));
  children.push(bulletBold("volumes: ", "Number of volumes where the subject appears"));
  children.push(bulletBold("lcsh-uri: ", "URI of the matching LCSH heading (if exact or good_close match)"));
  children.push(bulletBold("lcsh-match: ", "Match quality (\"exact\" or \"good_close\")"));

  children.push(heading2("6.2 Interactive Mockup"));
  children.push(para(
    "The mockup (hsg-subjects-mockup.html) is a self-contained HTML file that replicates the history.state.gov design system. It provides a browsable interface where users can navigate by category and subcategory, search across all subjects, and drill down to see every FRUS document associated with a subject, with direct links to the published documents online."
  ));
  children.push(para(
    "The mockup embeds all data directly in the HTML file (approximately 4.7 MB total), requiring no server-side infrastructure. It is generated from two intermediate JSON files:"
  ));
  children.push(bulletBold("mockup_sidebar_data.json: ", "Category/subcategory hierarchy with subject lists and counts"));
  children.push(bulletBold("mockup_subject_data.json: ", "Full subject details including document-level metadata (titles, dates, URLs)"));

  children.push(heading2("6.3 Taxonomy Review Tool"));
  children.push(para(
    "The review tool (taxonomy-review.html) is an editorial interface that displays all subjects organized by their assigned categories. It loads dedup decisions and category overrides at runtime, showing the current state of the taxonomy. Editors can review categorization assignments, search for subjects, and examine annotation counts and LCSH match data."
  ));

  // ═══════════════════════════════════════════════════════════
  // 7. PIPELINE REFERENCE
  // ═══════════════════════════════════════════════════════════
  children.push(new Paragraph({ children: [new PageBreak()] }));
  children.push(heading1("7. Pipeline Reference"));

  children.push(heading2("7.1 End-to-End Workflow"));
  children.push(para("The taxonomy is built through five phases:"));
  children.push(emptyPara());

  children.push(heading3("Phase 1: Data Extraction"));
  children.push(bullet("Extract subject annotations from annotated FRUS TEI XML volumes"));
  children.push(bullet("Extract document metadata (titles, dates) from TEI headers"));
  children.push(bullet("Query each subject against id.loc.gov for LCSH matching"));

  children.push(heading3("Phase 2: Editorial Review"));
  children.push(bullet("Review near-duplicate groups using dedup-review.html; export merge/skip decisions"));
  children.push(bullet("Review semantic duplicate groups using semantic-dedup-review.html; export decisions"));
  children.push(bullet("Review categorization; create manual overrides where keyword matching is incorrect"));

  children.push(heading3("Phase 3: Taxonomy Build"));
  children.push(bullet("Apply global dedup decisions (merge secondary entries into primaries)"));
  children.push(bullet("Fetch 2-level LCSH broader-term hierarchies (cached)"));
  children.push(bullet("Categorize all subjects using keyword scoring + manual overrides"));
  children.push(bullet("Deduplicate within subcategories"));
  children.push(bullet("Generate subject-taxonomy-lcsh.xml"));

  children.push(heading3("Phase 4: Mockup Generation"));
  children.push(bullet("Generate sidebar and subject data JSON files"));
  children.push(bullet("Build self-contained HTML mockup with embedded data"));

  children.push(heading3("Phase 5: Validation"));
  children.push(bullet("Review mockup for correct categorization and data integrity"));
  children.push(bullet("Verify uncategorized subjects for potential reassignment"));

  children.push(heading2("7.2 Scripts"));
  const scriptRows = [
    new TableRow({ children: [headerCell("Script", 3200), headerCell("Purpose", 6160)] }),
    new TableRow({ children: [dataCell("build_taxonomy_lcsh.py", 3200, { bold: true }), dataCell("Core build: LCSH fetching, categorization, dedup, XML generation", 6160)] }),
    new TableRow({ children: [dataCell("generate_mockup_data.py", 3200, { bold: true }), dataCell("Generates sidebar and subject JSON data for the mockup", 6160)] }),
    new TableRow({ children: [dataCell("build_mockup_html.py", 3200, { bold: true }), dataCell("Builds self-contained HTML mockup with embedded data", 6160)] }),
    new TableRow({ children: [dataCell("extract_doc_appearances.py", 3200, { bold: true }), dataCell("Extracts document-level annotations from TEI XML volumes", 6160)] }),
    new TableRow({ children: [dataCell("lcsh_mapper.py", 3200, { bold: true }), dataCell("Queries subjects against id.loc.gov suggest2 API", 6160)] }),
  ];
  children.push(new Table({
    width: { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: [3200, 6160],
    rows: scriptRows,
  }));
  children.push(emptyPara());

  children.push(heading2("7.3 Data Files"));
  const dataRows = [
    new TableRow({ children: [headerCell("File", 3600), headerCell("Description", 5760)] }),
    new TableRow({ children: [dataCell("lcsh_mapping.json", 3600, { bold: true }), dataCell("Master subject data with LCSH matches and metadata (814 KB)", 5760)] }),
    new TableRow({ children: [dataCell("document_appearances.json", 3600, { bold: true }), dataCell("Subject-to-document mapping: {rec_id: {volume: [doc_ids]}}", 5760)] }),
    new TableRow({ children: [dataCell("doc_metadata.json", 3600, { bold: true }), dataCell("Document titles, dates, and volume metadata (1 MB)", 5760)] }),
    new TableRow({ children: [dataCell("dedup_decisions.json", 3600, { bold: true }), dataCell("Reviewed merge/skip decisions for 99 duplicate groups", 5760)] }),
    new TableRow({ children: [dataCell("category_overrides.json", 3600, { bold: true }), dataCell("314 manual category reassignments from editorial review", 5760)] }),
    new TableRow({ children: [dataCell("lcsh_broader_cache.json", 3600, { bold: true }), dataCell("Cached LCSH broader-term hierarchies (~440 entries)", 5760)] }),
    new TableRow({ children: [dataCell("semantic_dedup_groups.json", 3600, { bold: true }), dataCell("67 semantic duplicate groups for review tool", 5760)] }),
    new TableRow({ children: [dataCell("semantic_dedup_decisions.json", 3600, { bold: true }), dataCell("Reviewed semantic merge/skip decisions", 5760)] }),
  ];
  children.push(new Table({
    width: { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: [3600, 5760],
    rows: dataRows,
  }));
  children.push(emptyPara());

  children.push(heading2("7.4 Regeneration Commands"));
  children.push(para("To regenerate the taxonomy and mockup from current data:"));
  children.push(emptyPara());
  const cmds = [
    "# Full build (fetch LCSH broader terms + build XML)",
    "python3 build_taxonomy_lcsh.py all",
    "",
    "# Build XML only (uses cached broader terms)",
    "python3 build_taxonomy_lcsh.py build",
    "",
    "# Generate mockup data + HTML",
    "python3 generate_mockup_data.py",
    "python3 build_mockup_html.py",
  ];
  for (const line of cmds) {
    children.push(new Paragraph({
      spacing: { after: 0 },
      indent: { left: 360 },
      children: [new TextRun({ font: "Courier New", size: 18, color: line.startsWith("#") ? GRAY : "000000", text: line || " " })],
    }));
  }
  children.push(emptyPara());

  // ═══════════════════════════════════════════════════════════
  // 8. CATEGORY REFERENCE (APPENDIX)
  // ═══════════════════════════════════════════════════════════
  children.push(new Paragraph({ children: [new PageBreak()] }));
  children.push(heading1("Appendix A: Category Reference"));
  children.push(para(
    "The following table lists all 12 categories with their subject counts, annotation counts, and subcategories. Categories are sorted by total annotations (descending)."
  ));
  children.push(emptyPara());

  for (const cat of categories) {
    children.push(new Paragraph({
      spacing: { before: 200, after: 80 },
      border: { bottom: { style: BorderStyle.SINGLE, size: 2, color: BLUE, space: 1 } },
      children: [
        new TextRun({ font: FONT, size: 24, bold: true, color: DARK, text: cat.name }),
        new TextRun({ font: FONT, size: 20, color: GRAY, text: `  (${cat.subjects} subjects, ${cat.annotations.toLocaleString()} annotations)` }),
      ],
    }));

    // Subcategories as a compact table
    const subRows = [
      new TableRow({
        children: [
          headerCell("Subcategory", 6560),
          headerCell("Subjects", 2800),
        ],
      }),
    ];
    for (const sub of cat.subcats) {
      const match = sub.match(/^(.+?)\s*\((\d+)\)$/);
      if (match) {
        subRows.push(new TableRow({
          children: [
            dataCell(match[1], 6560),
            dataCell(match[2], 2800, { align: AlignmentType.RIGHT }),
          ],
        }));
      }
    }
    children.push(new Table({
      width: { size: CONTENT_W, type: WidthType.DXA },
      columnWidths: [6560, 2800],
      rows: subRows,
    }));
    children.push(emptyPara());
  }

  // ── Appendix B: Summary Statistics ──────────────────────────
  children.push(new Paragraph({ children: [new PageBreak()] }));
  children.push(heading1("Appendix B: Summary Statistics"));

  const statsRows = [
    new TableRow({ children: [headerCell("Metric", 6000), headerCell("Value", 3360)] }),
    new TableRow({ children: [dataCell("Raw subject terms (before dedup)", 6000), dataCell("1,412", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Subjects after deduplication", 6000), dataCell("1,288", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Dedup merge groups", 6000), dataCell("99", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Entries merged (secondary refs removed)", 6000), dataCell("124", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("LCSH exact matches", 6000), dataCell("308", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("LCSH good_close matches", 6000), dataCell("134", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Subjects without LCSH match", 6000), dataCell("970", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Total document-level annotations", 6000), dataCell("65,571", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("FRUS volumes covered", 6000), dataCell("14", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Individual documents", 6000), dataCell("4,509", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Top-level categories", 6000), dataCell("12", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Categorized subjects", 6000), dataCell("864", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Uncategorized subjects", 6000), dataCell("424", 3360, { align: AlignmentType.RIGHT })] }),
    new TableRow({ children: [dataCell("Manual category overrides", 6000), dataCell("314", 3360, { align: AlignmentType.RIGHT })] }),
  ];
  children.push(new Table({
    width: { size: CONTENT_W, type: WidthType.DXA },
    columnWidths: [6000, 3360],
    rows: statsRows,
  }));

  // ── Build the document ──────────────────────────────────────
  const doc = new Document({
    styles: {
      default: {
        document: { run: { font: FONT, size: 22 } },
      },
      paragraphStyles: [
        {
          id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 32, bold: true, font: FONT, color: DARK },
          paragraph: { spacing: { before: 360, after: 200 }, outlineLevel: 0 },
        },
        {
          id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 26, bold: true, font: FONT, color: BLUE },
          paragraph: { spacing: { before: 240, after: 160 }, outlineLevel: 1 },
        },
        {
          id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { size: 23, bold: true, font: FONT, color: "333333" },
          paragraph: { spacing: { before: 200, after: 120 }, outlineLevel: 2 },
        },
      ],
    },
    numbering: {
      config: [
        {
          reference: "bullets",
          levels: [{
            level: 0, format: LevelFormat.BULLET, text: "\u2022", alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 720, hanging: 360 } } },
          }],
        },
        {
          reference: "numbers",
          levels: [{
            level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT,
            style: { paragraph: { indent: { left: 720, hanging: 360 } } },
          }],
        },
      ],
    },
    sections: [{
      properties: {
        page: {
          size: { width: PAGE_W, height: PAGE_H },
          margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN },
        },
      },
      headers: {
        default: new Header({
          children: [new Paragraph({
            alignment: AlignmentType.RIGHT,
            children: [new TextRun({ font: FONT, size: 18, color: GRAY, text: "FRUS Subject Taxonomy: Process Documentation" })],
          })],
        }),
      },
      footers: {
        default: new Footer({
          children: [new Paragraph({
            alignment: AlignmentType.CENTER,
            children: [
              new TextRun({ font: FONT, size: 18, color: GRAY, text: "Page " }),
              new TextRun({ font: FONT, size: 18, color: GRAY, children: [PageNumber.CURRENT] }),
            ],
          })],
        }),
      },
      children,
    }],
  });

  const buffer = await Packer.toBuffer(doc);
  const outPath = "FRUS-Subject-Taxonomy-Documentation.docx";
  fs.writeFileSync(outPath, buffer);
  console.log(`Wrote ${outPath} (${(buffer.length / 1024).toFixed(0)} KB)`);
}

main().catch(err => { console.error(err); process.exit(1); });
