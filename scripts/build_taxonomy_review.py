#!/usr/bin/env python3
"""
Build taxonomy-review.html — an interactive browser tool for reviewing
the LCSH subject taxonomy.

Reads:
  - subject-taxonomy-lcsh.xml   (the built taxonomy)
  - config/lcsh_mapping.json    (LCSH match details)

Writes:
  - taxonomy-review.html        (self-contained review tool)

The generated HTML lets editors:
  - Browse subjects by category/subcategory
  - Review and accept/reject LCSH mappings
  - Reassign subjects to different categories
  - Search across all subjects
  - Export decisions as lcsh_decisions.json
  - Save/load decisions via the dev server API

Usage:
    python3 build_taxonomy_review.py
"""

import json
import os
import sys
from lxml import etree

os.chdir(os.path.dirname(os.path.abspath(__file__)))

TAXONOMY_FILE = "../subject-taxonomy-lcsh.xml"
MAPPING_FILE = "../config/lcsh_mapping.json"
CATEGORY_OVERRIDES_FILE = "../config/category_overrides.json"
OUTPUT_FILE = "../taxonomy-review.html"


def parse_taxonomy(path):
    """Parse subject-taxonomy-lcsh.xml into a Python structure."""
    tree = etree.parse(path)
    root = tree.getroot()

    taxonomy = {
        "generated": root.get("generated", ""),
        "totalSubjects": root.get("total-subjects", "0"),
        "categories": [],
    }

    for cat_elem in root.findall("category"):
        cat = {
            "label": cat_elem.get("label", ""),
            "totalAnnotations": cat_elem.get("total-annotations", "0"),
            "totalSubjects": cat_elem.get("total-subjects", "0"),
            "subcategories": [],
        }

        for sub_elem in cat_elem.findall("subcategory"):
            sub = {
                "label": sub_elem.get("label", ""),
                "totalAnnotations": sub_elem.get("total-annotations", "0"),
                "totalSubjects": sub_elem.get("total-subjects", "0"),
                "subjects": [],
            }

            for subj_elem in sub_elem.findall("subject"):
                subj = {
                    "ref": subj_elem.get("ref", ""),
                    "type": subj_elem.get("type", "topic"),
                    "count": int(subj_elem.get("count", "0")),
                    "volumes": subj_elem.get("volumes", "0"),
                    "lcshUri": subj_elem.get("lcsh-uri", ""),
                    "lcshMatch": subj_elem.get("lcsh-match", ""),
                    "name": "",
                    "lcshForm": "",
                    "appearsIn": "",
                }

                name_elem = subj_elem.find("name")
                if name_elem is not None and name_elem.text:
                    subj["name"] = name_elem.text

                lcsh_form = subj_elem.find("lcsh-authorized-form")
                if lcsh_form is not None and lcsh_form.text:
                    subj["lcshForm"] = lcsh_form.text

                appears = subj_elem.find("appearsIn")
                if appears is not None and appears.text:
                    subj["appearsIn"] = appears.text

                sub["subjects"].append(subj)

            cat["subcategories"].append(sub)

        taxonomy["categories"].append(cat)

    return taxonomy


def load_mapping(path):
    """Load lcsh_mapping.json for supplementary match info."""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def load_category_overrides(path):
    """Load existing category overrides."""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def build_html(taxonomy, mapping, overrides):
    """Generate the self-contained review HTML."""

    # Build category list for the reassignment dropdown
    cat_labels = [c["label"] for c in taxonomy["categories"]]
    # Build subcategory map
    subcat_map = {}
    for cat in taxonomy["categories"]:
        subcat_map[cat["label"]] = [s["label"] for s in cat["subcategories"]]

    # Enrich subjects with mapping data where available
    for cat in taxonomy["categories"]:
        for sub in cat["subcategories"]:
            for subj in sub["subjects"]:
                ref = subj["ref"]
                if ref in mapping:
                    m = mapping[ref]
                    if not subj["lcshUri"] and m.get("lcsh_uri"):
                        subj["lcshUri"] = m["lcsh_uri"]
                    if not subj["lcshForm"] and m.get("lcsh_label"):
                        subj["lcshForm"] = m["lcsh_label"]
                    subj["matchQuality"] = m.get("match_quality", "")
                    # Add broader terms if available
                    chain = m.get("broader_chain_2lvl", [])
                    if chain:
                        subj["broaderTerms"] = [
                            {"label": bt.get("label", ""), "uri": bt.get("uri", "")}
                            for bt in chain
                        ]
                    # Add all suggestions
                    subj["allSuggestions"] = m.get("all_suggestions", [])

    taxonomy_json = json.dumps(taxonomy, ensure_ascii=False)
    subcat_map_json = json.dumps(subcat_map, ensure_ascii=False)
    overrides_json = json.dumps(overrides, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FRUS Subject Taxonomy — LCSH Review</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: #f5f5f5;
  color: #333;
  line-height: 1.5;
}}

/* ── Header ─────────────────────────────────── */
.header {{
  background: #1a365d;
  color: white;
  padding: 12px 20px;
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
  position: sticky;
  top: 0;
  z-index: 100;
}}
.header h1 {{
  font-size: 18px;
  font-weight: 600;
  white-space: nowrap;
}}
.header .meta {{
  font-size: 13px;
  opacity: 0.8;
}}
.header .spacer {{ flex: 1; }}
.search-box {{
  padding: 6px 10px;
  border: none;
  border-radius: 4px;
  width: 260px;
  font-size: 14px;
}}
.btn {{
  padding: 6px 14px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 13px;
  font-weight: 500;
  transition: background 0.15s;
}}
.btn-primary {{ background: #3182ce; color: white; }}
.btn-primary:hover {{ background: #2b6cb0; }}
.btn-success {{ background: #38a169; color: white; }}
.btn-success:hover {{ background: #2f855a; }}
.btn-danger {{ background: #e53e3e; color: white; }}
.btn-danger:hover {{ background: #c53030; }}
.btn-outline {{
  background: transparent;
  color: white;
  border: 1px solid rgba(255,255,255,0.4);
}}
.btn-outline:hover {{ background: rgba(255,255,255,0.1); }}

/* ── Stats bar ──────────────────────────────── */
.stats-bar {{
  background: #ebf4ff;
  border-bottom: 1px solid #bee3f8;
  padding: 8px 20px;
  display: flex;
  gap: 24px;
  font-size: 13px;
  color: #2a4365;
  flex-wrap: wrap;
}}
.stats-bar .stat {{ display: flex; gap: 4px; align-items: center; }}
.stats-bar .stat-val {{ font-weight: 600; }}

/* ── Layout ─────────────────────────────────── */
.layout {{
  display: flex;
  height: calc(100vh - 100px);
}}

/* Sidebar */
.sidebar {{
  width: 280px;
  min-width: 280px;
  background: white;
  border-right: 1px solid #e2e8f0;
  overflow-y: auto;
  padding: 8px 0;
}}
.cat-item {{
  padding: 8px 16px;
  cursor: pointer;
  border-left: 3px solid transparent;
  transition: all 0.1s;
  font-size: 14px;
}}
.cat-item:hover {{ background: #f7fafc; }}
.cat-item.active {{
  background: #ebf4ff;
  border-left-color: #3182ce;
  font-weight: 600;
}}
.cat-item .cat-count {{
  float: right;
  font-size: 12px;
  color: #718096;
  font-weight: 400;
}}
.subcat-item {{
  padding: 6px 16px 6px 32px;
  cursor: pointer;
  font-size: 13px;
  color: #4a5568;
  transition: background 0.1s;
}}
.subcat-item:hover {{ background: #f7fafc; }}
.subcat-item.active {{
  background: #ebf8ff;
  color: #2b6cb0;
  font-weight: 600;
}}
.subcat-item .subcat-count {{
  float: right;
  font-size: 11px;
  color: #a0aec0;
  font-weight: 400;
}}

/* ── Main content ───────────────────────────── */
.main {{
  flex: 1;
  overflow-y: auto;
  padding: 20px;
}}
.main h2 {{
  font-size: 20px;
  margin-bottom: 4px;
  color: #1a365d;
}}
.main .subtitle {{
  font-size: 13px;
  color: #718096;
  margin-bottom: 16px;
}}

/* Subject cards */
.subject-card {{
  background: white;
  border: 1px solid #e2e8f0;
  border-radius: 6px;
  padding: 14px 16px;
  margin-bottom: 10px;
  transition: border-color 0.15s;
}}
.subject-card:hover {{ border-color: #cbd5e0; }}
.subject-card.reviewed-accepted {{ border-left: 4px solid #38a169; }}
.subject-card.reviewed-rejected {{ border-left: 4px solid #e53e3e; }}
.subject-card.reassigned {{ border-left: 4px solid #d69e2e; }}

.subj-header {{
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}}
.subj-name {{
  font-size: 15px;
  font-weight: 600;
  color: #2d3748;
}}
.subj-badges {{
  display: flex;
  gap: 6px;
  flex-shrink: 0;
}}
.badge {{
  display: inline-block;
  padding: 2px 8px;
  border-radius: 10px;
  font-size: 11px;
  font-weight: 600;
}}
.badge-count {{ background: #edf2f7; color: #4a5568; }}
.badge-exact {{ background: #c6f6d5; color: #276749; }}
.badge-close {{ background: #fefcbf; color: #975a16; }}
.badge-none {{ background: #fed7d7; color: #9b2c2c; }}
.badge-rejected {{ background: #e53e3e; color: white; }}

.subj-details {{
  margin-top: 8px;
  font-size: 13px;
  color: #4a5568;
}}
.subj-details .detail-row {{
  display: flex;
  gap: 8px;
  margin-bottom: 3px;
}}
.subj-details .detail-label {{
  color: #718096;
  min-width: 100px;
  flex-shrink: 0;
}}
.subj-details a {{
  color: #3182ce;
  text-decoration: none;
}}
.subj-details a:hover {{ text-decoration: underline; }}

.subj-actions {{
  margin-top: 10px;
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
}}
.subj-actions .btn {{ font-size: 12px; padding: 4px 10px; }}

.reassign-controls {{
  display: none;
  margin-top: 10px;
  padding: 10px;
  background: #fffff0;
  border: 1px solid #fefcbf;
  border-radius: 4px;
}}
.reassign-controls select {{
  padding: 4px 8px;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 13px;
  margin-right: 8px;
}}
.reassign-controls label {{
  font-size: 13px;
  color: #4a5568;
  margin-right: 4px;
}}

/* ── Filter bar ─────────────────────────────── */
.filter-bar {{
  display: flex;
  gap: 12px;
  margin-bottom: 16px;
  align-items: center;
  flex-wrap: wrap;
}}
.filter-bar select, .filter-bar input {{
  padding: 5px 8px;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 13px;
}}
.filter-bar label {{
  font-size: 13px;
  color: #718096;
}}

/* ── Search results ─────────────────────────── */
.search-results {{
  display: none;
}}

/* ── Output panel (reused from annotation review) ── */
.output-panel {{
  display: none;
  position: fixed;
  bottom: 0;
  left: 0;
  right: 0;
  max-height: 40vh;
  background: #1a202c;
  color: #e2e8f0;
  font-family: "SF Mono", "Fira Code", monospace;
  font-size: 12px;
  z-index: 200;
  border-top: 2px solid #4a5568;
}}
.output-header {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 6px 12px;
  background: #2d3748;
}}
.output-header span {{ font-weight: 600; }}
.output-close {{
  background: none;
  border: none;
  color: #a0aec0;
  cursor: pointer;
  font-size: 16px;
}}
.output-body {{
  padding: 8px 12px;
  overflow-y: auto;
  max-height: calc(40vh - 32px);
  white-space: pre-wrap;
  word-break: break-word;
}}
.output-body .line-ok {{ color: #68d391; }}
.output-body .line-err {{ color: #fc8181; }}
.output-body .line-warn {{ color: #f6e05e; }}

/* ── Broader terms ──────────────────────────── */
.broader-terms {{
  margin-top: 4px;
  padding-left: 8px;
  border-left: 2px solid #e2e8f0;
  font-size: 12px;
  color: #718096;
}}
.broader-terms .bt-level {{
  margin-bottom: 2px;
}}

/* ── Toast ──────────────────────────────────── */
.toast {{
  position: fixed;
  bottom: 20px;
  right: 20px;
  padding: 10px 20px;
  background: #2d3748;
  color: white;
  border-radius: 6px;
  font-size: 14px;
  z-index: 300;
  display: none;
  animation: fadeIn 0.2s ease-in;
}}
@keyframes fadeIn {{ from {{ opacity: 0; }} to {{ opacity: 1; }} }}

/* ── Spinner ────────────────────────────────── */
@keyframes spin {{ to {{ transform: rotate(360deg); }} }}
.spinner {{
  display: inline-block;
  width: 14px; height: 14px;
  border: 2px solid rgba(255,255,255,0.3);
  border-top-color: white;
  border-radius: 50%;
  animation: spin 0.6s linear infinite;
  margin-right: 6px;
  vertical-align: middle;
}}
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <h1>FRUS Subject Taxonomy — LCSH Review</h1>
  <span class="meta" id="header-meta"></span>
  <span class="spacer"></span>
  <input type="text" class="search-box" id="search-box"
         placeholder="Search subjects…" oninput="handleSearch(this.value)">
  <button class="btn btn-outline" onclick="showStats()">Stats</button>
  <button class="btn btn-primary" onclick="exportDecisions()">Export Decisions</button>
  <button class="btn btn-success" onclick="saveToServer()">Save to Server</button>
</div>

<!-- Stats bar -->
<div class="stats-bar" id="stats-bar">
  <div class="stat">Subjects: <span class="stat-val" id="stat-total">0</span></div>
  <div class="stat">With LCSH: <span class="stat-val" id="stat-lcsh">0</span></div>
  <div class="stat">Exact: <span class="stat-val" id="stat-exact">0</span></div>
  <div class="stat">Close: <span class="stat-val" id="stat-close">0</span></div>
  <div class="stat">No match: <span class="stat-val" id="stat-nomatch">0</span></div>
  <div class="stat">|</div>
  <div class="stat">Reviewed: <span class="stat-val" id="stat-reviewed">0</span></div>
  <div class="stat">Accepted: <span class="stat-val" id="stat-accepted">0</span></div>
  <div class="stat">Rejected: <span class="stat-val" id="stat-rejected">0</span></div>
  <div class="stat">Reassigned: <span class="stat-val" id="stat-reassigned">0</span></div>
</div>

<!-- Layout -->
<div class="layout">
  <div class="sidebar" id="sidebar"></div>
  <div class="main" id="main-content">
    <div id="welcome">
      <h2>LCSH Taxonomy Review</h2>
      <p class="subtitle">Select a category from the sidebar to begin reviewing subjects.</p>
    </div>
    <div id="category-view" style="display:none;"></div>
    <div class="search-results" id="search-results"></div>
  </div>
</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<!-- Output panel -->
<div class="output-panel" id="output-panel">
  <div class="output-header">
    <span id="output-title">Output</span>
    <button class="output-close" onclick="closeOutputPanel()">&times;</button>
  </div>
  <div class="output-body" id="output-body"></div>
</div>

<script>
// ══════════════════════════════════════════════════════════
// DATA
// ══════════════════════════════════════════════════════════
const TAXONOMY = {taxonomy_json};
const SUBCAT_MAP = {subcat_map_json};
const EXISTING_OVERRIDES = {overrides_json};

// ── State ─────────────────────────────────────────────────
const lcshDecisions = {{}};      // ref -> "accepted" | "rejected"
const categoryOverrides = {{}};  // ref -> {{ toCategory, toSubcategory, name, count, fromCategory, fromSubcategory }}
let currentCat = null;
let currentSub = null;
let searchTimeout = null;

// Build flat subject index for search
const subjectIndex = [];
for (const cat of TAXONOMY.categories) {{
  for (const sub of cat.subcategories) {{
    for (const subj of sub.subjects) {{
      subjectIndex.push({{
        ...subj,
        category: cat.label,
        subcategory: sub.label,
      }});
    }}
  }}
}}

// ══════════════════════════════════════════════════════════
// INIT
// ══════════════════════════════════════════════════════════
function init() {{
  document.getElementById("header-meta").textContent =
    "Generated " + TAXONOMY.generated + " · " + TAXONOMY.totalSubjects + " subjects";

  buildSidebar();
  updateStats();
  loadFromServer();
}}

// ── Sidebar ───────────────────────────────────────────────
function buildSidebar() {{
  const sb = document.getElementById("sidebar");
  sb.innerHTML = "";

  for (const cat of TAXONOMY.categories) {{
    const catEl = document.createElement("div");
    catEl.className = "cat-item";
    catEl.innerHTML = cat.label +
      '<span class="cat-count">' + cat.totalSubjects + "</span>";
    catEl.onclick = () => selectCategory(cat.label);
    catEl.dataset.cat = cat.label;
    sb.appendChild(catEl);

    for (const sub of cat.subcategories) {{
      const subEl = document.createElement("div");
      subEl.className = "subcat-item";
      subEl.style.display = "none";
      subEl.innerHTML = sub.label +
        '<span class="subcat-count">' + sub.totalSubjects + "</span>";
      subEl.onclick = (e) => {{
        e.stopPropagation();
        selectSubcategory(cat.label, sub.label);
      }};
      subEl.dataset.cat = cat.label;
      subEl.dataset.sub = sub.label;
      sb.appendChild(subEl);
    }}
  }}
}}

function selectCategory(catLabel) {{
  // Toggle subcategories visibility
  const sb = document.getElementById("sidebar");
  const wasActive = currentCat === catLabel;

  // Hide all subcategories
  sb.querySelectorAll(".subcat-item").forEach(el => el.style.display = "none");
  sb.querySelectorAll(".cat-item").forEach(el => el.classList.remove("active"));

  if (wasActive) {{
    currentCat = null;
    currentSub = null;
    document.getElementById("category-view").style.display = "none";
    document.getElementById("welcome").style.display = "block";
    return;
  }}

  currentCat = catLabel;
  currentSub = null;

  // Show this category's subcategories
  sb.querySelectorAll('.subcat-item[data-cat="' + catLabel + '"]').forEach(
    el => el.style.display = "block"
  );
  sb.querySelector('.cat-item[data-cat="' + catLabel + '"]').classList.add("active");

  // Show all subjects in this category
  showCategory(catLabel);
}}

function selectSubcategory(catLabel, subLabel) {{
  currentCat = catLabel;
  currentSub = subLabel;

  const sb = document.getElementById("sidebar");
  sb.querySelectorAll(".subcat-item").forEach(el => el.classList.remove("active"));
  sb.querySelector('.subcat-item[data-cat="' + catLabel + '"][data-sub="' + subLabel + '"]')
    ?.classList.add("active");

  showCategory(catLabel, subLabel);
}}

// ── Main content ──────────────────────────────────────────
function showCategory(catLabel, subLabel) {{
  document.getElementById("welcome").style.display = "none";
  document.getElementById("search-results").style.display = "none";
  const cv = document.getElementById("category-view");
  cv.style.display = "block";

  const cat = TAXONOMY.categories.find(c => c.label === catLabel);
  if (!cat) return;

  let subjects = [];
  let title = catLabel;
  let subtitle = "";

  if (subLabel) {{
    const sub = cat.subcategories.find(s => s.label === subLabel);
    if (sub) {{
      subjects = sub.subjects;
      title = subLabel;
      subtitle = catLabel + " — " + sub.totalAnnotations + " annotations";
    }}
  }} else {{
    for (const sub of cat.subcategories) {{
      subjects = subjects.concat(sub.subjects);
    }}
    subtitle = cat.totalSubjects + " subjects · " + cat.totalAnnotations + " annotations";
  }}

  let filterHtml = '<div class="filter-bar">' +
    '<label>Filter:</label>' +
    '<select onchange="filterSubjects(this.value)">' +
    '<option value="all">All</option>' +
    '<option value="lcsh">Has LCSH</option>' +
    '<option value="exact">Exact match</option>' +
    '<option value="close">Close match</option>' +
    '<option value="none">No LCSH</option>' +
    '<option value="unreviewed">Unreviewed (with LCSH)</option>' +
    '<option value="accepted">Accepted</option>' +
    '<option value="rejected">Rejected</option>' +
    '<option value="reassigned">Reassigned</option>' +
    '</select>' +
    '<label style="margin-left:12px;">Sort:</label>' +
    '<select onchange="sortSubjects(this.value)">' +
    '<option value="count-desc">Count (high→low)</option>' +
    '<option value="count-asc">Count (low→high)</option>' +
    '<option value="alpha">Alphabetical</option>' +
    '</select>' +
    '</div>';

  cv.innerHTML = "<h2>" + title + "</h2>" +
    '<p class="subtitle">' + subtitle + "</p>" +
    filterHtml +
    '<div id="subjects-container">' +
    subjects.map(s => renderSubjectCard(s, catLabel, subLabel || findSubcategory(catLabel, s.ref))).join("") +
    "</div>";
}}

function findSubcategory(catLabel, ref) {{
  const cat = TAXONOMY.categories.find(c => c.label === catLabel);
  if (!cat) return "";
  for (const sub of cat.subcategories) {{
    if (sub.subjects.some(s => s.ref === ref)) return sub.label;
  }}
  return "";
}}

function renderSubjectCard(subj, catLabel, subLabel) {{
  const ref = subj.ref;
  const decision = lcshDecisions[ref] || "";
  const override = categoryOverrides[ref];
  const hasLcsh = !!subj.lcshUri;
  const matchType = subj.lcshMatch || subj.matchQuality || "";

  let cardClass = "subject-card";
  if (decision === "accepted") cardClass += " reviewed-accepted";
  else if (decision === "rejected") cardClass += " reviewed-rejected";
  if (override) cardClass += " reassigned";

  // Badges
  let badges = '<span class="badge badge-count">' + subj.count.toLocaleString() + " in " + subj.volumes + " vols</span>";
  if (matchType === "exact") badges += '<span class="badge badge-exact">Exact LCSH</span>';
  else if (matchType === "good_close") badges += '<span class="badge badge-close">Close LCSH</span>';
  else if (!hasLcsh) badges += '<span class="badge badge-none">No LCSH</span>';
  if (decision === "rejected") badges += '<span class="badge badge-rejected">LCSH Rejected</span>';

  // Details
  let details = "";
  if (hasLcsh) {{
    details += '<div class="detail-row"><span class="detail-label">LCSH form:</span><span>' +
      (subj.lcshForm || subj.name) + "</span></div>";
    details += '<div class="detail-row"><span class="detail-label">LCSH URI:</span><span>' +
      '<a href="' + subj.lcshUri + '" target="_blank">' + subj.lcshUri + "</a></span></div>";
  }}
  if (subj.appearsIn) {{
    details += '<div class="detail-row"><span class="detail-label">Appears in:</span><span>' +
      subj.appearsIn + "</span></div>";
  }}

  // Broader terms
  let broaderHtml = "";
  if (subj.broaderTerms && subj.broaderTerms.length > 0) {{
    broaderHtml = '<div class="broader-terms">';
    for (let i = 0; i < subj.broaderTerms.length; i++) {{
      const bt = subj.broaderTerms[i];
      const prefix = i === 0 ? "BT1" : "BT2";
      broaderHtml += '<div class="bt-level">' + prefix + ": " +
        '<a href="' + bt.uri + '" target="_blank">' + bt.label + "</a></div>";
    }}
    broaderHtml += "</div>";
  }}

  if (override) {{
    details += '<div class="detail-row" style="color:#d69e2e;font-weight:600;">' +
      '<span class="detail-label">Reassigned:</span>' +
      "<span>" + override.toCategory + " → " + override.toSubcategory + "</span></div>";
  }}

  // Actions — use data attributes to avoid quote-escaping issues
  let actions = "";
  if (hasLcsh) {{
    actions += '<button class="btn' + (decision === "accepted" ? " btn-success" : "") +
      '" data-action="accept" data-ref="' + ref + '">' +
      (decision === "accepted" ? "&#10003; Accepted" : "Accept LCSH") + "</button>";
    actions += '<button class="btn' + (decision === "rejected" ? " btn-danger" : "") +
      '" data-action="reject" data-ref="' + ref + '">' +
      (decision === "rejected" ? "&#10007; Rejected" : "Reject LCSH") + "</button>";
  }}
  actions += '<button class="btn" data-action="reassign" data-ref="' + ref + '">' +
    (override ? "Edit Category" : "Reassign") + "</button>";

  // Reassign controls
  const catOptions = TAXONOMY.categories.map(c =>
    '<option value="' + c.label + '"' +
    (c.label === (override ? override.toCategory : catLabel) ? " selected" : "") +
    ">" + c.label + "</option>"
  ).join("");

  const currentTargetCat = override ? override.toCategory : catLabel;
  const subcatOptions = (SUBCAT_MAP[currentTargetCat] || []).map(s =>
    '<option value="' + s + '"' +
    (s === (override ? override.toSubcategory : subLabel) ? " selected" : "") +
    ">" + s + "</option>"
  ).join("");

  const reassignHtml = '<div class="reassign-controls" id="reassign-' + ref + '">' +
    '<label>Category:</label>' +
    '<select id="reassign-cat-' + ref + '" data-action="update-subcats" data-ref="' + ref + '">' +
    catOptions + "</select>" +
    '<label>Subcategory:</label>' +
    '<select id="reassign-sub-' + ref + '">' + subcatOptions + "</select>" +
    '<button class="btn btn-primary" style="margin-left:8px;" data-action="apply-reassign" data-ref="' + ref +
    '" data-name="' + escHtml(subj.name) + '" data-count="' + subj.count +
    '" data-from-cat="' + escHtml(catLabel) + '" data-from-sub="' + escHtml(subLabel) + '">' +
    "Apply</button>" +
    (override ? '<button class="btn" style="margin-left:4px;" data-action="remove-reassign" data-ref="' + ref +
    '">Remove</button>' : "") +
    "</div>";

  return '<div class="' + cardClass + '" id="card-' + ref + '" ' +
    'data-ref="' + ref + '" ' +
    'data-match="' + matchType + '" ' +
    'data-decision="' + decision + '" ' +
    'data-reassigned="' + (override ? "1" : "0") + '">' +
    '<div class="subj-header">' +
    '<span class="subj-name">' + escHtml(subj.name) + "</span>" +
    '<div class="subj-badges">' + badges + "</div>" +
    "</div>" +
    '<div class="subj-details">' + details + broaderHtml + "</div>" +
    '<div class="subj-actions">' + actions + "</div>" +
    reassignHtml +
    "</div>";
}}

function escHtml(s) {{
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;")
    .replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}}

// ── Decisions ─────────────────────────────────────────────
function setDecision(ref, decision) {{
  if (lcshDecisions[ref] === decision) {{
    delete lcshDecisions[ref];
  }} else {{
    lcshDecisions[ref] = decision;
  }}
  refreshCurrentView();
  updateStats();
  autoSave();
}}

function toggleReassign(ref) {{
  const el = document.getElementById("reassign-" + ref);
  if (el) el.style.display = el.style.display === "none" ? "block" : "none";
}}

function updateSubcatOptions(ref) {{
  const catSel = document.getElementById("reassign-cat-" + ref);
  const subSel = document.getElementById("reassign-sub-" + ref);
  const cat = catSel.value;
  const subs = SUBCAT_MAP[cat] || [];
  subSel.innerHTML = subs.map(s =>
    '<option value="' + s + '">' + s + "</option>"
  ).join("");
}}

function applyReassign(ref, name, count, fromCat, fromSub) {{
  const catSel = document.getElementById("reassign-cat-" + ref);
  const subSel = document.getElementById("reassign-sub-" + ref);
  categoryOverrides[ref] = {{
    ref: ref,
    name: name,
    count: count,
    from_category: fromCat,
    from_subcategory: fromSub,
    to_category: catSel.value,
    to_subcategory: subSel.value,
  }};
  refreshCurrentView();
  updateStats();
  autoSave();
  showToast("Reassigned: " + name);
}}

function removeReassign(ref) {{
  delete categoryOverrides[ref];
  refreshCurrentView();
  updateStats();
  autoSave();
}}

// ── Filter / Sort ─────────────────────────────────────────
function filterSubjects(val) {{
  document.querySelectorAll("#subjects-container .subject-card").forEach(card => {{
    const match = card.dataset.match;
    const decision = card.dataset.decision;
    const reassigned = card.dataset.reassigned === "1";
    let show = true;

    switch (val) {{
      case "lcsh": show = !!match; break;
      case "exact": show = match === "exact"; break;
      case "close": show = match === "good_close"; break;
      case "none": show = !match; break;
      case "unreviewed": show = !!match && !decision; break;
      case "accepted": show = decision === "accepted"; break;
      case "rejected": show = decision === "rejected"; break;
      case "reassigned": show = reassigned; break;
    }}
    card.style.display = show ? "block" : "none";
  }});
}}

function sortSubjects(val) {{
  const container = document.getElementById("subjects-container");
  if (!container) return;
  const cards = Array.from(container.children);
  cards.sort((a, b) => {{
    if (val === "alpha") {{
      return a.querySelector(".subj-name").textContent
        .localeCompare(b.querySelector(".subj-name").textContent);
    }}
    const ca = parseInt(a.querySelector(".badge-count")?.textContent) || 0;
    const cb = parseInt(b.querySelector(".badge-count")?.textContent) || 0;
    return val === "count-asc" ? ca - cb : cb - ca;
  }});
  cards.forEach(c => container.appendChild(c));
}}

function refreshCurrentView() {{
  if (currentCat) {{
    showCategory(currentCat, currentSub);
  }}
}}

// ── Search ────────────────────────────────────────────────
function handleSearch(query) {{
  clearTimeout(searchTimeout);
  if (!query || query.length < 2) {{
    document.getElementById("search-results").style.display = "none";
    if (currentCat) {{
      document.getElementById("category-view").style.display = "block";
    }} else {{
      document.getElementById("welcome").style.display = "block";
    }}
    return;
  }}

  searchTimeout = setTimeout(() => {{
    const q = query.toLowerCase();
    const results = subjectIndex.filter(s =>
      s.name.toLowerCase().includes(q) ||
      (s.lcshForm && s.lcshForm.toLowerCase().includes(q)) ||
      s.ref.toLowerCase().includes(q)
    ).slice(0, 100);

    document.getElementById("welcome").style.display = "none";
    document.getElementById("category-view").style.display = "none";
    const sr = document.getElementById("search-results");
    sr.style.display = "block";
    sr.innerHTML = "<h2>Search: &ldquo;" + escHtml(query) + "&rdquo;</h2>" +
      '<p class="subtitle">' + results.length + " results</p>" +
      results.map(s =>
        renderSubjectCard(s, s.category, s.subcategory)
      ).join("");
  }}, 200);
}}

// ── Stats ─────────────────────────────────────────────────
function updateStats() {{
  let total = 0, withLcsh = 0, exact = 0, close = 0, noMatch = 0;
  for (const s of subjectIndex) {{
    total++;
    const m = s.lcshMatch || s.matchQuality || "";
    if (m === "exact") {{ withLcsh++; exact++; }}
    else if (m === "good_close") {{ withLcsh++; close++; }}
    else {{ noMatch++; }}
  }}

  const reviewed = Object.keys(lcshDecisions).length;
  const accepted = Object.values(lcshDecisions).filter(d => d === "accepted").length;
  const rejected = Object.values(lcshDecisions).filter(d => d === "rejected").length;
  const reassigned = Object.keys(categoryOverrides).length;

  document.getElementById("stat-total").textContent = total;
  document.getElementById("stat-lcsh").textContent = withLcsh;
  document.getElementById("stat-exact").textContent = exact;
  document.getElementById("stat-close").textContent = close;
  document.getElementById("stat-nomatch").textContent = noMatch;
  document.getElementById("stat-reviewed").textContent = reviewed;
  document.getElementById("stat-accepted").textContent = accepted;
  document.getElementById("stat-rejected").textContent = rejected;
  document.getElementById("stat-reassigned").textContent = reassigned;
}}

function showStats() {{
  let catStats = "";
  for (const cat of TAXONOMY.categories) {{
    catStats += cat.label + ": " + cat.totalSubjects + " subjects, " +
      parseInt(cat.totalAnnotations).toLocaleString() + " annotations\\n";
  }}
  alert("Taxonomy Stats\\n" +
    "Generated: " + TAXONOMY.generated + "\\n" +
    "Total subjects: " + TAXONOMY.totalSubjects + "\\n\\n" +
    catStats +
    "\\nDecisions: " + Object.keys(lcshDecisions).length + " LCSH reviews, " +
    Object.keys(categoryOverrides).length + " category reassignments"
  );
}}

// ── Export ─────────────────────────────────────────────────
function exportDecisions() {{
  const decisions = [];
  for (const [ref, decision] of Object.entries(lcshDecisions)) {{
    const subj = subjectIndex.find(s => s.ref === ref);
    decisions.push({{
      ref: ref,
      name: subj ? subj.name : "",
      decision: decision,
    }});
  }}

  const overridesList = Object.values(categoryOverrides);

  const output = {{
    exported: new Date().toISOString(),
    tool: "taxonomy-review.html",
    total_decisions: decisions.length,
    decisions: decisions.sort((a, b) => a.ref.localeCompare(b.ref)),
    total_category_overrides: overridesList.length,
    category_overrides: overridesList.sort((a, b) => a.ref.localeCompare(b.ref)),
  }};

  const blob = new Blob([JSON.stringify(output, null, 2)], {{ type: "application/json" }});
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "lcsh_decisions.json";
  a.click();
  URL.revokeObjectURL(url);
  showToast("Exported " + decisions.length + " LCSH decisions + " +
    overridesList.length + " category overrides");
}}

// ── Server save/load ──────────────────────────────────────
let autoSaveTimeout = null;

function autoSave() {{
  clearTimeout(autoSaveTimeout);
  autoSaveTimeout = setTimeout(saveToServer, 2000);
}}

async function saveToServer() {{
  try {{
    const payload = {{
      lcsh_decisions: lcshDecisions,
      category_overrides: categoryOverrides,
      saved: new Date().toISOString(),
    }};
    const resp = await fetch("/api/save-taxonomy-decisions", {{
      method: "POST",
      headers: {{ "Content-Type": "application/json" }},
      body: JSON.stringify(payload),
    }});
    if (resp.ok) {{
      const data = await resp.json();
      showToast("Saved " + (data.lcsh_count || 0) + " LCSH + " +
        (data.override_count || 0) + " category decisions");
    }} else {{
      // Server endpoint might not exist yet
      console.log("Server save not available, decisions stored in memory");
    }}
  }} catch (e) {{
    console.log("Server save not available:", e.message);
  }}
}}

async function loadFromServer() {{
  try {{
    const resp = await fetch("/api/load-taxonomy-decisions");
    if (resp.ok) {{
      const data = await resp.json();
      if (data.lcsh_decisions) {{
        Object.assign(lcshDecisions, data.lcsh_decisions);
      }}
      if (data.category_overrides) {{
        Object.assign(categoryOverrides, data.category_overrides);
      }}
      updateStats();
      if (Object.keys(lcshDecisions).length > 0 || Object.keys(categoryOverrides).length > 0) {{
        showToast("Loaded " + Object.keys(lcshDecisions).length + " LCSH + " +
          Object.keys(categoryOverrides).length + " category decisions from server");
      }}
    }}
  }} catch (e) {{
    console.log("Server load not available:", e.message);
  }}
}}

// ── Toast ─────────────────────────────────────────────────
function showToast(msg) {{
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.style.display = "block";
  setTimeout(() => el.style.display = "none", 3000);
}}

// ── Event delegation ──────────────────────────────────────
// Handle all button clicks via data-action attributes to avoid
// quote-escaping issues in inline onclick handlers.
document.addEventListener("click", function(e) {{
  const btn = e.target.closest("[data-action]");
  if (!btn) return;

  const action = btn.dataset.action;
  const ref = btn.dataset.ref;

  switch (action) {{
    case "accept":
      setDecision(ref, "accepted");
      break;
    case "reject":
      setDecision(ref, "rejected");
      break;
    case "reassign":
      toggleReassign(ref);
      break;
    case "apply-reassign":
      applyReassign(ref, btn.dataset.name, parseInt(btn.dataset.count),
        btn.dataset.fromCat, btn.dataset.fromSub);
      break;
    case "remove-reassign":
      removeReassign(ref);
      break;
  }}
}});

document.addEventListener("change", function(e) {{
  const sel = e.target.closest("[data-action='update-subcats']");
  if (sel) {{
    updateSubcatOptions(sel.dataset.ref);
  }}
}});

// ── Init ──────────────────────────────────────────────────
init();
</script>
</body>
</html>"""

    return html


def main():
    if not os.path.exists(TAXONOMY_FILE):
        print(f"Error: {TAXONOMY_FILE} not found.")
        print("Run 'python3 build_taxonomy_lcsh.py build' first.")
        sys.exit(1)

    print("Building taxonomy-review.html...")
    print(f"  Reading: {TAXONOMY_FILE}")
    taxonomy = parse_taxonomy(TAXONOMY_FILE)

    print(f"  Reading: {MAPPING_FILE}")
    mapping = load_mapping(MAPPING_FILE)

    print(f"  Reading: {CATEGORY_OVERRIDES_FILE}")
    overrides = load_category_overrides(CATEGORY_OVERRIDES_FILE)

    print(f"  Categories: {len(taxonomy['categories'])}")
    total_subjects = sum(
        len(s["subjects"])
        for c in taxonomy["categories"]
        for s in c["subcategories"]
    )
    print(f"  Subjects: {total_subjects}")

    html = build_html(taxonomy, mapping, overrides)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"\nWrote: {OUTPUT_FILE} ({size_kb:.0f} KB)")
    print(f"Serve locally:")
    print(f"  make serve")
    print(f"  Open http://localhost:9090/taxonomy-review.html")


if __name__ == "__main__":
    main()
