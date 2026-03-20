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
DOC_APPEARANCES_FILE = "../document_appearances.json"
DOC_METADATA_FILE = "../doc_metadata.json"
VARIANT_GROUPS_FILE = "../variant_groups.json"
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


def load_doc_appearances(path):
    """Load document_appearances.json — ref -> {vol_id: [doc_ids]}."""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def load_doc_metadata(path):
    """Load doc_metadata.json — {documents: {vol/doc: {t, d}}, volumes: {...}}."""
    if not os.path.exists(path):
        return {"documents": {}, "volumes": {}}
    with open(path) as f:
        return json.load(f)


def load_variant_groups(path):
    """Load variant_groups.json — groups and ref_to_canonical mapping."""
    if not os.path.exists(path):
        return {"groups": [], "ref_to_canonical": {}}
    with open(path) as f:
        return json.load(f)


def build_html(taxonomy, mapping, overrides, appearances, doc_meta, variant_groups):
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

    # Build compact appearances data: ref -> [[vol_id, doc_id, title, date], ...]
    # Only include refs actually in the taxonomy to keep payload manageable
    all_refs = set()
    for cat in taxonomy["categories"]:
        for sub in cat["subcategories"]:
            for subj in sub["subjects"]:
                all_refs.add(subj["ref"])

    doc_appearances = {}
    doc_meta_docs = doc_meta.get("documents", {})
    for ref in all_refs:
        if ref not in appearances:
            continue
        vol_docs = appearances[ref]
        entries = []
        for vol_id, doc_ids in sorted(vol_docs.items()):
            for doc_id in sorted(doc_ids, key=lambda d: int(d[1:]) if d[1:].isdigit() else 0):
                doc_key = f"{vol_id}/{doc_id}"
                meta = doc_meta_docs.get(doc_key, {})
                entries.append([vol_id, doc_id, meta.get("t", ""), meta.get("d", "")])
        if entries:
            doc_appearances[ref] = entries

    # Build variant groups data: ref -> {canonical, variants: [{ref, name}], source}
    vg_by_ref = {}  # ref -> group info (for both canonical and variant refs)
    for grp in variant_groups.get("groups", []):
        group_info = {
            "canonical_ref": grp["canonical_ref"],
            "canonical_name": grp["canonical_name"],
            "source": grp.get("source", ""),
            "search_names": grp.get("search_names", []),
            "variant_refs": grp.get("variant_refs", []),
        }
        vg_by_ref[grp["canonical_ref"]] = group_info
        for vref in grp.get("variant_refs", []):
            vg_by_ref[vref] = group_info

    taxonomy_json = json.dumps(taxonomy, ensure_ascii=False)
    subcat_map_json = json.dumps(subcat_map, ensure_ascii=False)
    overrides_json = json.dumps(overrides, ensure_ascii=False)
    appearances_json = json.dumps(doc_appearances, separators=(",", ":"), ensure_ascii=False)
    variant_groups_json = json.dumps(vg_by_ref, separators=(",", ":"), ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FRUS Subject Taxonomy — LCSH Review</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
html, body {{ height: 100%; overflow: hidden; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: #f5f5f5;
  color: #333;
  line-height: 1.5;
}}

/* ── Header ─────────────────────────────────── */
.header {{
  background: #312e81;
  color: white;
  padding: 10px 20px;
  display: flex;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
  z-index: 100;
  height: 48px;
}}
.header h1 {{
  font-size: 16px;
  font-weight: 600;
  white-space: nowrap;
}}
.header .meta {{
  font-size: 12px;
  opacity: 0.75;
}}
.header .spacer {{ flex: 1; }}
.btn {{
  padding: 6px 14px;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 13px;
  font-weight: 500;
  transition: background 0.15s;
}}
.btn-primary {{ background: #6b46c1; color: white; }}
.btn-primary:hover {{ background: #553c9a; }}
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

/* ── Toolbar ────────────────────────────────── */
.toolbar {{
  background: white;
  border-bottom: 1px solid #e2e8f0;
  padding: 6px 20px;
  display: flex;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
  height: 42px;
}}
.toolbar label {{
  font-size: 12px;
  color: #718096;
  font-weight: 500;
}}
.toolbar select, .toolbar input {{
  padding: 4px 8px;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 13px;
}}
.toolbar .search-box {{
  padding: 5px 10px;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  width: 220px;
  font-size: 13px;
}}
.toolbar .tb-spacer {{ flex: 1; }}
.toolbar .stat {{
  font-size: 12px;
  color: #4a5568;
  display: flex;
  gap: 3px;
  align-items: center;
}}
.toolbar .stat-val {{ font-weight: 700; color: #312e81; }}
.stat-merged {{ color: #6b46c1 !important; }}
.stat-excluded {{ color: #e53e3e !important; }}

/* ── Split layout ───────────────────────────── */
.split {{
  display: flex;
  height: calc(100vh - 90px);
}}

/* ── Left pane: subject list ────────────────── */
.list-pane {{
  width: 50%;
  border-right: 1px solid #e2e8f0;
  display: flex;
  flex-direction: column;
  background: white;
}}
.list-header {{
  display: flex;
  align-items: center;
  padding: 6px 12px;
  background: #f8f7ff;
  border-bottom: 1px solid #e2e8f0;
  font-size: 11px;
  font-weight: 700;
  color: #6b46c1;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  user-select: none;
  flex-shrink: 0;
}}
.list-header span {{ cursor: pointer; }}
.list-header span:hover {{ color: #312e81; }}
.lh-name {{ flex: 1; min-width: 0; padding-left: 40px; }}
.lh-cat  {{ width: 160px; text-align: left; }}
.lh-lcsh {{ width: 70px; text-align: center; }}
.lh-count {{ width: 50px; text-align: right; padding-right: 12px; }}
.list-scroll {{
  flex: 1;
  overflow-y: auto;
}}

/* ── List items ─────────────────────────────── */
.list-item {{
  display: flex;
  align-items: center;
  padding: 7px 12px;
  cursor: pointer;
  border-left: 3px solid transparent;
  border-bottom: 1px solid #f3f4f6;
  font-size: 13px;
  transition: background 0.1s;
}}
.list-item:hover {{ background: #f8f7ff; }}
.list-item.active {{
  background: #ede9fe;
  border-left-color: #312e81;
}}
.list-item.excluded {{ opacity: 0.45; }}
.list-item.excluded .li-name {{ text-decoration: line-through; }}
.list-item.merged {{ opacity: 0.6; }}

/* Status dot: review state */
.status-dot {{
  width: 8px; height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-right: 6px;
  background: #e2e8f0;
}}
.status-dot.accepted {{ background: #38a169; }}
.status-dot.rejected {{ background: #e53e3e; }}
.status-dot.reassigned {{ background: #d69e2e; }}
.status-dot.merged-dot {{ background: #805ad5; }}
.status-dot.excluded-dot {{ background: #e53e3e; border: 2px solid #feb2b2; }}

/* LCSH dot */
.lcsh-dot {{
  width: 8px; height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
  margin-right: 8px;
}}
.lcsh-dot.exact {{ background: #38a169; }}
.lcsh-dot.close {{ background: #d69e2e; }}
.lcsh-dot.none  {{ background: #e2e8f0; }}

.li-name {{
  flex: 1;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-weight: 500;
  color: #1f2937;
}}
.li-cat {{
  width: 160px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 12px;
  color: #6b7280;
}}
.match-badge {{
  width: 58px;
  text-align: center;
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  border-radius: 8px;
  padding: 1px 6px;
  flex-shrink: 0;
}}
.match-badge.exact {{ background: #c6f6d5; color: #276749; }}
.match-badge.close {{ background: #fefcbf; color: #975a16; }}
.match-badge.none  {{ color: transparent; }}
.li-count {{
  width: 50px;
  text-align: right;
  font-size: 12px;
  color: #6b7280;
  font-weight: 600;
  padding-right: 4px;
  flex-shrink: 0;
}}

/* ── Right pane: detail ─────────────────────── */
.detail-pane {{
  width: 50%;
  overflow-y: auto;
  padding: 24px 28px;
  background: white;
}}
.detail-empty {{
  color: #a0aec0;
  text-align: center;
  margin-top: 120px;
  font-size: 15px;
}}
.detail-name {{
  font-size: 22px;
  font-weight: 700;
  color: #1f2937;
  margin-bottom: 6px;
}}
.detail-divider {{
  height: 3px;
  background: #6b46c1;
  width: 60px;
  border-radius: 2px;
  margin-bottom: 16px;
}}
.detail-section {{
  margin-bottom: 18px;
}}
.detail-section-title {{
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: #6b46c1;
  margin-bottom: 6px;
}}
.detail-row {{
  display: flex;
  gap: 8px;
  margin-bottom: 4px;
  font-size: 13px;
  color: #4a5568;
}}
.detail-label {{
  color: #718096;
  min-width: 110px;
  flex-shrink: 0;
  font-weight: 500;
}}
.detail-pane a {{ color: #6b46c1; text-decoration: none; }}
.detail-pane a:hover {{ text-decoration: underline; }}

/* Match pill */
.match-pill {{
  display: inline-block;
  padding: 2px 10px;
  border-radius: 10px;
  font-size: 12px;
  font-weight: 600;
}}
.match-pill.exact {{ background: #c6f6d5; color: #276749; }}
.match-pill.close {{ background: #fefcbf; color: #975a16; }}
.match-pill.none  {{ background: #fed7d7; color: #9b2c2c; }}

/* Broader terms */
.broader-terms {{
  margin-top: 4px;
  padding-left: 10px;
  border-left: 2px solid #e2e8f0;
  font-size: 12px;
  color: #718096;
}}
.broader-terms .bt-level {{ margin-bottom: 2px; }}

/* Variant tags */
.variant-section {{ margin-bottom: 14px; }}
.variant-tag {{
  display: inline-block;
  background: #ede9fe;
  color: #5b21b6;
  padding: 2px 9px;
  border-radius: 10px;
  margin: 2px 3px;
  font-size: 12px;
}}
.variant-tag.not-in-tax {{ opacity: 0.55; font-style: italic; }}

/* Document appearances */
.doc-row {{
  display: flex;
  gap: 6px;
  padding: 2px 0;
  font-size: 12px;
  color: #4a5568;
}}
.doc-row .doc-id {{ font-weight: 500; min-width: 48px; color: #6b46c1; }}
.doc-row .doc-date {{ color: #a0aec0; font-size: 11px; min-width: 90px; }}
.vol-label {{
  font-weight: 600;
  color: #312e81;
  font-size: 12px;
  margin: 8px 0 2px 0;
}}
.vol-group {{
  margin-left: 12px;
  padding-left: 8px;
  border-left: 2px solid #ede9fe;
}}

/* Detail actions */
.detail-actions {{
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 16px;
  padding-top: 14px;
  border-top: 1px solid #e2e8f0;
}}
.detail-actions .btn {{ font-size: 13px; padding: 6px 14px; }}

/* Reassign controls (in detail pane) */
.reassign-controls {{
  display: none;
  margin-top: 12px;
  padding: 10px;
  background: #faf5ff;
  border: 1px solid #e9d8fd;
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

/* ── Merge modal ───────────────────────────── */
.merge-overlay {{
  display: none;
  position: fixed;
  top: 0; left: 0; right: 0; bottom: 0;
  background: rgba(0,0,0,0.5);
  z-index: 400;
  justify-content: center;
  align-items: center;
}}
.merge-overlay.open {{ display: flex; }}
.merge-modal {{
  background: white;
  border-radius: 8px;
  width: 560px;
  max-height: 70vh;
  display: flex;
  flex-direction: column;
  box-shadow: 0 8px 30px rgba(0,0,0,0.3);
}}
.merge-modal-header {{
  padding: 16px 20px;
  border-bottom: 1px solid #e2e8f0;
}}
.merge-modal-header h3 {{
  font-size: 16px;
  color: #6b46c1;
  margin-bottom: 8px;
}}
.merge-search {{
  width: 100%;
  padding: 8px 10px;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 14px;
}}
.merge-list {{
  flex: 1;
  overflow-y: auto;
  max-height: 50vh;
}}
.merge-cat-header {{
  padding: 6px 20px;
  background: #f8f7ff;
  font-size: 11px;
  font-weight: 700;
  color: #6b46c1;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}}
.merge-item {{
  padding: 8px 20px;
  cursor: pointer;
  border-bottom: 1px solid #f5f5f5;
  display: flex;
  justify-content: space-between;
  align-items: center;
}}
.merge-item:hover {{ background: #f8f7ff; }}
.merge-item .m-name {{ font-weight: 600; font-size: 14px; }}
.merge-item .m-info {{ font-size: 12px; color: #718096; }}
.merge-item .m-count {{ font-size: 12px; font-weight: 600; color: #6b46c1; }}
.merge-modal-footer {{
  padding: 12px 20px;
  border-top: 1px solid #e2e8f0;
  text-align: right;
}}
.merge-modal-footer button {{
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  padding: 6px 16px;
  font-size: 13px;
  cursor: pointer;
  background: white;
}}
.merge-modal-footer button:hover {{ background: #f5f5f5; }}

/* Merge decision display */
.merge-decision {{
  margin-top: 10px;
  padding: 8px 12px;
  background: #faf5ff;
  border: 1px solid #d6bcfa;
  border-radius: 4px;
  font-size: 13px;
}}
.merge-decision.is-source {{ background: #faf5ff; border-color: #d6bcfa; }}
.merge-decision.is-target {{ background: #f0fff4; border-color: #9ae6b4; }}
.merge-target-name {{ font-weight: 700; color: #6b46c1; cursor: pointer; }}
.merge-target-name:hover {{ text-decoration: underline; }}
.merge-source-tag {{
  display: inline-block;
  background: #e9d8fd;
  padding: 1px 7px;
  border-radius: 10px;
  margin: 1px 3px;
  font-size: 11px;
}}
.btn-merge {{ background: #6b46c1; color: white; }}
.btn-merge:hover {{ background: #553c9a; }}
.btn-merge-undo {{
  border: none;
  border-radius: 4px;
  padding: 3px 8px;
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
  background: #faf5ff;
  color: #6b46c1;
  margin-left: 8px;
}}
.btn-merge-undo:hover {{ background: #e9d8fd; }}
.btn-exclude {{ color: #e53e3e; border-color: #fed7d7; background: #fff5f5; }}
.btn-exclude:hover {{ background: #fed7d7; }}
.btn-exclude-undo {{ color: #e53e3e; font-size: 12px; margin-left: 8px; cursor: pointer; text-decoration: underline; border: none; background: none; }}
.exclude-label {{ color: #e53e3e; font-weight: 600; font-size: 13px; }}

/* ── Output panel ───────────────────────────── */
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
  border-top: 2px solid #6b46c1;
}}
.output-header {{
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 6px 12px;
  background: #312e81;
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

/* ── Toast ──────────────────────────────────── */
.toast {{
  position: fixed;
  bottom: 20px;
  right: 20px;
  padding: 10px 20px;
  background: #312e81;
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
  <button class="btn btn-outline" onclick="showStats()">Stats</button>
  <button class="btn btn-primary" onclick="exportDecisions()">Export Decisions</button>
  <button class="btn btn-success" onclick="saveToServer()">Save to Server</button>
</div>

<!-- Toolbar -->
<div class="toolbar">
  <label>Category</label>
  <select id="filter-category" onchange="applyFilters()">
    <option value="all">All Categories</option>
  </select>
  <label>LCSH</label>
  <select id="filter-lcsh" onchange="applyFilters()">
    <option value="all">All</option>
    <option value="exact">Exact</option>
    <option value="close">Close</option>
    <option value="none">No LCSH</option>
  </select>
  <label>Status</label>
  <select id="filter-status" onchange="applyFilters()">
    <option value="all">All</option>
    <option value="unreviewed">Unreviewed</option>
    <option value="accepted">Accepted</option>
    <option value="rejected">Rejected</option>
    <option value="reassigned">Reassigned</option>
    <option value="merged">Merged</option>
    <option value="excluded">Excluded</option>
  </select>
  <input type="text" class="search-box" id="search-box"
         placeholder="Search subjects\u2026" oninput="handleSearch(this.value)">
  <span class="tb-spacer"></span>
  <span class="stat"><span class="stat-val" id="stat-total">0</span> subjects</span>
  <span class="stat"><span class="stat-val" id="stat-lcsh">0</span> LCSH</span>
  <span class="stat"><span class="stat-val" id="stat-exact">0</span> exact</span>
  <span class="stat"><span class="stat-val" id="stat-close">0</span> close</span>
  <span class="stat"><span class="stat-val" id="stat-nomatch">0</span> none</span>
  <span class="stat">|</span>
  <span class="stat"><span class="stat-val" id="stat-reviewed">0</span> rev</span>
  <span class="stat"><span class="stat-val" id="stat-accepted">0</span> acc</span>
  <span class="stat"><span class="stat-val" id="stat-rejected">0</span> rej</span>
  <span class="stat"><span class="stat-val" id="stat-reassigned">0</span> rsg</span>
  <span class="stat"><span class="stat-val stat-merged" id="stat-merged">0</span> mrg</span>
  <span class="stat"><span class="stat-val stat-excluded" id="stat-excluded">0</span> exc</span>
</div>

<!-- Split layout -->
<div class="split">
  <div class="list-pane">
    <div class="list-header">
      <span class="lh-name" onclick="sortList('alpha')">Subject</span>
      <span class="lh-cat" onclick="sortList('cat')">Category</span>
      <span class="lh-lcsh">LCSH</span>
      <span class="lh-count" onclick="sortList('count')">#</span>
    </div>
    <div class="list-scroll" id="list-scroll"></div>
  </div>
  <div class="detail-pane" id="detail-pane">
    <div class="detail-empty" id="detail-empty">Select a subject from the list to view details.</div>
    <div id="detail-content" style="display:none;"></div>
  </div>
</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<!-- Merge modal -->
<div class="merge-overlay" id="merge-overlay" onclick="if(event.target===this)closeMergeModal()">
  <div class="merge-modal">
    <div class="merge-modal-header">
      <h3 id="merge-modal-title">Merge into another term</h3>
      <input class="merge-search" id="merge-search" type="text" placeholder="Search terms..." oninput="filterMergeModal()">
    </div>
    <div class="merge-list" id="merge-list"></div>
    <div class="merge-modal-footer">
      <button onclick="closeMergeModal()">Cancel</button>
    </div>
  </div>
</div>

<!-- Output panel -->
<div class="output-panel" id="output-panel">
  <div class="output-header">
    <span id="output-title">Output</span>
    <button class="output-close" onclick="closeOutputPanel()">&times;</button>
  </div>
  <div class="output-body" id="output-body"></div>
</div>

<script id="doc-appearances-data" type="application/json">
{appearances_json}
</script>
<script id="variant-groups-data" type="application/json">
{variant_groups_json}
</script>

<script>
// ══════════════════════════════════════════════════════════
// DATA
// ══════════════════════════════════════════════════════════
const TAXONOMY = {taxonomy_json};
const SUBCAT_MAP = {subcat_map_json};
const EXISTING_OVERRIDES = {overrides_json};
const DOC_APPEARANCES = JSON.parse(document.getElementById('doc-appearances-data').textContent);
const VARIANT_GROUPS = JSON.parse(document.getElementById('variant-groups-data').textContent);

// ── State ─────────────────────────────────────────────────
const lcshDecisions = {{}};      // ref -> "accepted" | "rejected"
const categoryOverrides = {{}};  // ref -> {{ toCategory, toSubcategory, name, count, fromCategory, fromSubcategory }}
const mergeDecisions = {{}};     // sourceRef -> {{ targetRef, targetName }}
const exclusions = {{}};         // ref -> {{ name, reason }}
let currentCat = null;
let currentSub = null;
let selectedRef = null;
let currentSort = "count-desc";
let searchTimeout = null;
let mergeModalSourceRef = null;

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
    "Generated " + TAXONOMY.generated + " \u00b7 " + TAXONOMY.totalSubjects + " subjects";

  // Load existing category overrides baked in from category_overrides.json
  for (const ov of EXISTING_OVERRIDES) {{
    categoryOverrides[ov.ref] = ov;
  }}

  // Populate category filter dropdown
  const catSel = document.getElementById("filter-category");
  for (const cat of TAXONOMY.categories) {{
    const opt = document.createElement("option");
    opt.value = cat.label;
    opt.textContent = cat.label;
    catSel.appendChild(opt);
  }}

  buildSubjectList();
  updateStats();
  loadFromServer();
}}

// ── Effective category for a subject ─────────────────────
function getEffectiveCategory(subj) {{
  const ov = categoryOverrides[subj.ref];
  if (ov) return {{ category: ov.to_category, subcategory: ov.to_subcategory }};
  return {{ category: subj.category, subcategory: subj.subcategory }};
}}

// ── Build flat subject list (left pane) ──────────────────
function buildSubjectList() {{
  const scroll = document.getElementById("list-scroll");
  let items = getFilteredSubjects();
  items = applySortToList(items);

  let html = "";
  for (const s of items) {{
    const ref = s.ref;
    const decision = lcshDecisions[ref] || "";
    const override = categoryOverrides[ref];
    const merge = mergeDecisions[ref];
    const excluded = !!exclusions[ref];
    const matchType = s.lcshMatch || s.matchQuality || "";
    const eff = getEffectiveCategory(s);

    // Status dot class
    let dotCls = "status-dot";
    if (excluded) dotCls += " excluded-dot";
    else if (merge) dotCls += " merged-dot";
    else if (decision === "accepted") dotCls += " accepted";
    else if (decision === "rejected") dotCls += " rejected";
    else if (override) dotCls += " reassigned";

    // LCSH dot class
    let lcshCls = "lcsh-dot";
    if (matchType === "exact") lcshCls += " exact";
    else if (matchType === "good_close") lcshCls += " close";
    else lcshCls += " none";

    // Match badge
    let badgeText = "";
    let badgeCls = "match-badge none";
    if (matchType === "exact") {{ badgeText = "EXACT"; badgeCls = "match-badge exact"; }}
    else if (matchType === "good_close") {{ badgeText = "CLOSE"; badgeCls = "match-badge close"; }}

    // Category path
    const catPath = eff.subcategory ? eff.category + " \u203a " + eff.subcategory : eff.category;

    // Item classes
    let itemCls = "list-item";
    if (ref === selectedRef) itemCls += " active";
    if (excluded) itemCls += " excluded";
    if (merge) itemCls += " merged";

    html += '<div class="' + itemCls + '" data-action="select-subject" data-ref="' + ref + '">' +
      '<span class="' + dotCls + '"></span>' +
      '<span class="' + lcshCls + '"></span>' +
      '<span class="li-name">' + escHtml(s.name) + '</span>' +
      '<span class="li-cat">' + escHtml(catPath) + '</span>' +
      '<span class="' + badgeCls + '">' + badgeText + '</span>' +
      '<span class="li-count">' + s.count.toLocaleString() + '</span>' +
      '</div>';
  }}

  if (!html) html = '<div style="padding:20px;color:#a0aec0;text-align:center;">No subjects match the current filters.</div>';
  scroll.innerHTML = html;
}}

function getFilteredSubjects() {{
  const catFilter = document.getElementById("filter-category").value;
  const lcshFilter = document.getElementById("filter-lcsh").value;
  const statusFilter = document.getElementById("filter-status").value;
  const searchQuery = (document.getElementById("search-box").value || "").toLowerCase();

  return subjectIndex.filter(s => {{
    const ref = s.ref;
    const eff = getEffectiveCategory(s);
    const matchType = s.lcshMatch || s.matchQuality || "";
    const decision = lcshDecisions[ref] || "";
    const override = categoryOverrides[ref];
    const merge = mergeDecisions[ref];
    const excluded = !!exclusions[ref];

    // Category filter
    if (catFilter !== "all" && eff.category !== catFilter) return false;

    // LCSH filter
    if (lcshFilter === "exact" && matchType !== "exact") return false;
    if (lcshFilter === "close" && matchType !== "good_close") return false;
    if (lcshFilter === "none" && (matchType === "exact" || matchType === "good_close")) return false;

    // Status filter
    if (statusFilter === "unreviewed" && (decision || excluded || merge)) return false;
    if (statusFilter === "accepted" && decision !== "accepted") return false;
    if (statusFilter === "rejected" && decision !== "rejected") return false;
    if (statusFilter === "reassigned" && !override) return false;
    if (statusFilter === "merged" && !merge) return false;
    if (statusFilter === "excluded" && !excluded) return false;

    // Search
    if (searchQuery && searchQuery.length >= 2) {{
      if (!s.name.toLowerCase().includes(searchQuery) &&
          !(s.lcshForm && s.lcshForm.toLowerCase().includes(searchQuery)) &&
          !s.ref.toLowerCase().includes(searchQuery)) return false;
    }}

    return true;
  }});
}}

function applySortToList(items) {{
  const sorted = [...items];
  switch (currentSort) {{
    case "count-desc":
      sorted.sort((a, b) => b.count - a.count);
      break;
    case "count-asc":
      sorted.sort((a, b) => a.count - b.count);
      break;
    case "alpha":
      sorted.sort((a, b) => a.name.localeCompare(b.name));
      break;
    case "cat":
      sorted.sort((a, b) => {{
        const ca = getEffectiveCategory(a);
        const cb = getEffectiveCategory(b);
        const cmp = ca.category.localeCompare(cb.category);
        if (cmp !== 0) return cmp;
        return ca.subcategory.localeCompare(cb.subcategory);
      }});
      break;
  }}
  return sorted;
}}

function sortList(field) {{
  if (field === "count") {{
    currentSort = currentSort === "count-desc" ? "count-asc" : "count-desc";
  }} else if (field === "alpha") {{
    currentSort = "alpha";
  }} else if (field === "cat") {{
    currentSort = "cat";
  }}
  buildSubjectList();
}}

function applyFilters() {{
  buildSubjectList();
}}

// ── Select subject (populate detail pane) ────────────────
function selectSubject(ref) {{
  selectedRef = ref;

  // Highlight active item in the list
  document.querySelectorAll(".list-item").forEach(el => {{
    el.classList.toggle("active", el.dataset.ref === ref);
  }});

  const subj = subjectIndex.find(s => s.ref === ref);
  if (!subj) return;

  document.getElementById("detail-empty").style.display = "none";
  const dc = document.getElementById("detail-content");
  dc.style.display = "block";
  dc.innerHTML = renderDetailContent(subj);
}}

function renderDetailContent(subj) {{
  const ref = subj.ref;
  const decision = lcshDecisions[ref] || "";
  const override = categoryOverrides[ref];
  const merge = mergeDecisions[ref];
  const excluded = !!exclusions[ref];
  const hasLcsh = !!subj.lcshUri;
  const matchType = subj.lcshMatch || subj.matchQuality || "";
  const eff = getEffectiveCategory(subj);

  let html = '<div class="detail-name">' + escHtml(subj.name) + '</div>';
  html += '<div class="detail-divider"></div>';

  // Overview section
  html += '<div class="detail-section">';
  html += '<div class="detail-section-title">Overview</div>';
  html += '<div class="detail-row"><span class="detail-label">Category:</span><span>' +
    escHtml(eff.category) + ' \u203a ' + escHtml(eff.subcategory) + '</span></div>';
  html += '<div class="detail-row"><span class="detail-label">Type:</span><span>' + escHtml(subj.type) + '</span></div>';
  html += '<div class="detail-row"><span class="detail-label">Annotations:</span><span>' + subj.count.toLocaleString() + '</span></div>';
  html += '<div class="detail-row"><span class="detail-label">Volumes:</span><span>' + subj.volumes + '</span></div>';

  if (override) {{
    html += '<div class="detail-row" style="color:#d69e2e;font-weight:600;">' +
      '<span class="detail-label">Reassigned:</span>' +
      '<span>from ' + escHtml(subj.category) + ' \u203a ' + escHtml(subj.subcategory) +
      ' to ' + escHtml(override.to_category) + ' \u203a ' + escHtml(override.to_subcategory) + '</span></div>';
  }}

  if (excluded) {{
    html += '<div class="detail-row"><span class="exclude-label">Excluded from taxonomy</span>' +
      '<button class="btn-exclude-undo" data-action="restore-exclude" data-ref="' + ref + '">Restore</button></div>';
  }}
  html += '</div>';

  // Merge info section (before action buttons)
  if (merge) {{
    html += '<div class="merge-decision is-source">' +
      '<span style="color:#6b46c1;font-weight:600;">Merged into:</span> ' +
      '<span class="merge-target-name" data-action="goto-term" data-ref="' + merge.targetRef + '">' +
      escHtml(merge.targetName) + '</span>' +
      '<button class="btn-merge-undo" data-action="undo-merge" data-ref="' + ref + '">&#x2717; Undo</button>' +
      '</div>';
  }} else {{
    const sources = getMergeSources(ref);
    if (sources.length > 0) {{
      html += '<div class="merge-decision is-target">' +
        '<span style="color:#276749;font-weight:600;">Receiving merges from:</span> ';
      for (const s of sources) {{
        html += '<span class="merge-source-tag">' + escHtml(s.name) + '</span>';
      }}
      html += '</div>';
    }}
  }}

  // Action buttons (at top for easy access)
  html += '<div class="detail-actions">';
  if (hasLcsh) {{
    html += '<button class="btn' + (decision === "accepted" ? " btn-success" : "") +
      '" data-action="accept" data-ref="' + ref + '">' +
      (decision === "accepted" ? "&#10003; Accepted" : "Accept LCSH") + '</button>';
    html += '<button class="btn' + (decision === "rejected" ? " btn-danger" : "") +
      '" data-action="reject" data-ref="' + ref + '">' +
      (decision === "rejected" ? "&#10007; Rejected" : "Reject LCSH") + '</button>';
  }}
  html += '<button class="btn" data-action="reassign" data-ref="' + ref + '">' +
    (override ? "Edit Category" : "Reassign") + '</button>';
  if (!merge) {{
    html += '<button class="btn btn-merge" data-action="open-merge" data-ref="' + ref + '">Merge\u2026</button>';
  }}
  if (!excluded) {{
    html += '<button class="btn btn-exclude" data-action="exclude" data-ref="' + ref +
      '" data-name="' + escHtml(subj.name) + '">Exclude</button>';
  }}
  html += '</div>';

  // Reassign controls (hidden by default, shown when Reassign is clicked)
  const catLabel = subj.category;
  const subLabel = subj.subcategory;
  const catOptions = TAXONOMY.categories.map(c =>
    '<option value="' + c.label + '"' +
    (c.label === (override ? override.to_category : catLabel) ? " selected" : "") +
    '>' + c.label + '</option>'
  ).join("");

  const currentTargetCat = override ? override.to_category : catLabel;
  const subcatOptions = (SUBCAT_MAP[currentTargetCat] || []).map(s =>
    '<option value="' + s + '"' +
    (s === (override ? override.to_subcategory : subLabel) ? " selected" : "") +
    '>' + s + '</option>'
  ).join("");

  html += '<div class="reassign-controls" id="reassign-' + ref + '">' +
    '<label>Category:</label>' +
    '<select id="reassign-cat-' + ref + '" data-action="update-subcats" data-ref="' + ref + '">' +
    catOptions + '</select>' +
    '<label>Subcategory:</label>' +
    '<select id="reassign-sub-' + ref + '">' + subcatOptions + '</select>' +
    '<button class="btn btn-primary" style="margin-left:8px;" data-action="apply-reassign" data-ref="' + ref +
    '" data-name="' + escHtml(subj.name) + '" data-count="' + subj.count +
    '" data-from-cat="' + escHtml(catLabel) + '" data-from-sub="' + escHtml(subLabel) + '">' +
    'Apply</button>' +
    (override ? '<button class="btn" style="margin-left:4px;" data-action="remove-reassign" data-ref="' + ref +
    '">Remove</button>' : '') +
    '</div>';

  // LCSH Mapping section
  html += '<div class="detail-section">';
  html += '<div class="detail-section-title">LCSH Mapping</div>';
  if (hasLcsh) {{
    let pillCls = "match-pill ";
    let pillText = "";
    if (matchType === "exact") {{ pillCls += "exact"; pillText = "Exact match"; }}
    else if (matchType === "good_close") {{ pillCls += "close"; pillText = "Close match"; }}
    else {{ pillCls += "none"; pillText = "Unknown"; }}

    html += '<div class="detail-row"><span class="detail-label">Match:</span><span class="' + pillCls + '">' + pillText + '</span></div>';
    html += '<div class="detail-row"><span class="detail-label">Auth. form:</span><span>' +
      escHtml(subj.lcshForm || subj.name) + '</span></div>';
    html += '<div class="detail-row"><span class="detail-label">URI:</span><span>' +
      '<a href="' + subj.lcshUri + '" target="_blank">' + escHtml(subj.lcshUri) + '</a></span></div>';

    // Broader terms
    if (subj.broaderTerms && subj.broaderTerms.length > 0) {{
      html += '<div class="broader-terms" style="margin-top:6px;">';
      for (let i = 0; i < subj.broaderTerms.length; i++) {{
        const bt = subj.broaderTerms[i];
        const prefix = i === 0 ? "BT1" : "BT2";
        html += '<div class="bt-level">' + prefix + ': <a href="' + bt.uri + '" target="_blank">' + escHtml(bt.label) + '</a></div>';
      }}
      html += '</div>';
    }}

    if (decision) {{
      const decLabel = decision === "accepted" ? "Accepted" : "Rejected";
      const decColor = decision === "accepted" ? "#38a169" : "#e53e3e";
      html += '<div class="detail-row" style="margin-top:6px;"><span class="detail-label">Decision:</span>' +
        '<span style="font-weight:700;color:' + decColor + ';">' + decLabel + '</span></div>';
    }}
  }} else {{
    html += '<div class="detail-row"><span class="match-pill none">No LCSH match</span></div>';
  }}
  html += '</div>';

  // Variant Group section
  const vg = VARIANT_GROUPS[ref];
  if (vg && vg.search_names && vg.search_names.length > 1) {{
    html += '<div class="detail-section variant-section">';
    html += '<div class="detail-section-title">Variant Group</div>';
    for (const sn of vg.search_names) {{
      const cls = sn.in_taxonomy ? "variant-tag" : "variant-tag not-in-tax";
      html += '<span class="' + cls + '">' + escHtml(sn.name) + '</span>';
    }}
    html += '</div>';
  }}

  // Appears In section
  const docEntries = DOC_APPEARANCES[ref];
  if (docEntries && docEntries.length > 0) {{
    const byVol = {{}};
    for (const [vol, docId, title, date] of docEntries) {{
      if (!byVol[vol]) byVol[vol] = [];
      byVol[vol].push({{ docId, title, date }});
    }}
    const totalDocs = docEntries.length;
    const volCount = Object.keys(byVol).length;

    html += '<div class="detail-section">';
    html += '<div class="detail-section-title">Appears In (' + totalDocs + ' docs, ' + volCount + ' vols)</div>';
    for (const vol of Object.keys(byVol).sort()) {{
      html += '<div class="vol-label">' + escHtml(vol) + ' (' + byVol[vol].length + ')</div>';
      html += '<div class="vol-group">';
      for (const d of byVol[vol]) {{
        html += '<div class="doc-row"><span class="doc-id">' + escHtml(d.docId) +
          '</span><span class="doc-date">' + escHtml(d.date) +
          '</span><span>' + escHtml(d.title.substring(0, 80)) + '</span></div>';
      }}
      html += '</div>';
    }}
    html += '</div>';
  }}

  return html;
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

// ── Merge decisions ───────────────────────────────────────
function getMergeSources(targetRef) {{
  const sources = [];
  for (const [sourceRef, d] of Object.entries(mergeDecisions)) {{
    if (d.targetRef === targetRef) {{
      const subj = subjectIndex.find(s => s.ref === sourceRef);
      sources.push({{ ref: sourceRef, name: subj ? subj.name : sourceRef }});
    }}
  }}
  return sources;
}}

function openMergeModal(sourceRef) {{
  mergeModalSourceRef = sourceRef;
  const subj = subjectIndex.find(s => s.ref === sourceRef);
  document.getElementById("merge-modal-title").textContent =
    'Merge "' + (subj ? subj.name : sourceRef) + '" into\u2026';
  document.getElementById("merge-search").value = "";
  populateMergeModal("");
  document.getElementById("merge-overlay").classList.add("open");
  document.getElementById("merge-search").focus();
}}

function closeMergeModal() {{
  document.getElementById("merge-overlay").classList.remove("open");
  mergeModalSourceRef = null;
}}

function filterMergeModal() {{
  const q = document.getElementById("merge-search").value.toLowerCase();
  populateMergeModal(q);
}}

function populateMergeModal(query) {{
  const list = document.getElementById("merge-list");
  const byCat = {{}};

  for (const s of subjectIndex) {{
    if (s.ref === mergeModalSourceRef) continue;
    if (query && !s.name.toLowerCase().includes(query)) continue;
    if (!byCat[s.category]) byCat[s.category] = [];
    byCat[s.category].push(s);
  }}

  let html = "";
  for (const cat of Object.keys(byCat).sort()) {{
    const terms = byCat[cat].sort((a, b) => b.count - a.count);
    html += '<div class="merge-cat-header">' + escHtml(cat) + ' (' + terms.length + ')</div>';
    for (const t of terms) {{
      html += '<div class="merge-item" data-action="confirm-merge" data-source="' +
        mergeModalSourceRef + '" data-target="' + t.ref + '" data-target-name="' + escHtml(t.name) + '">' +
        '<div><div class="m-name">' + escHtml(t.name) + '</div>' +
        '<div class="m-info">' + escHtml(t.subcategory) + '</div></div>' +
        '<span class="m-count">' + t.count.toLocaleString() + ' occ</span>' +
        '</div>';
    }}
  }}
  if (!html) html = '<div style="padding:20px;color:#718096;text-align:center;">No matching terms.</div>';
  list.innerHTML = html;
}}

function confirmMerge(sourceRef, targetRef, targetName) {{
  mergeDecisions[sourceRef] = {{
    targetRef: targetRef,
    targetName: targetName,
  }};

  // Auto-inherit category if source is Uncategorized and target has a real category
  const sourceSubj = subjectIndex.find(s => s.ref === sourceRef);
  const targetSubj = subjectIndex.find(s => s.ref === targetRef);
  if (sourceSubj && targetSubj) {{
    const srcCat = categoryOverrides[sourceRef] ? categoryOverrides[sourceRef].to_category : sourceSubj.category;
    const tgtCat = categoryOverrides[targetRef] ? categoryOverrides[targetRef].to_category : targetSubj.category;
    const tgtSub = categoryOverrides[targetRef] ? categoryOverrides[targetRef].to_subcategory : targetSubj.subcategory;
    if (srcCat === "Uncategorized" && tgtCat !== "Uncategorized") {{
      categoryOverrides[sourceRef] = {{
        ref: sourceRef,
        name: sourceSubj.name,
        count: sourceSubj.count,
        from_category: sourceSubj.category,
        from_subcategory: sourceSubj.subcategory,
        to_category: tgtCat,
        to_subcategory: tgtSub,
        auto_from_merge: true,
      }};
    }}
  }}

  closeMergeModal();
  refreshCurrentView();
  updateStats();
  autoSave();
  showToast("Merged into: " + targetName);
}}

function undoMerge(ref) {{
  delete mergeDecisions[ref];
  // Also remove auto-inherited category override if it was created by the merge
  if (categoryOverrides[ref] && categoryOverrides[ref].auto_from_merge) {{
    delete categoryOverrides[ref];
  }}
  refreshCurrentView();
  updateStats();
  autoSave();
}}

// ── Exclusions ───────────────────────────────────────────
function excludeTerm(ref, name) {{
  exclusions[ref] = {{ name: name }};
  refreshCurrentView();
  updateStats();
  autoSave();
  showToast("Excluded: " + name);
}}

function restoreExcluded(ref) {{
  const name = exclusions[ref] ? exclusions[ref].name : ref;
  delete exclusions[ref];
  refreshCurrentView();
  updateStats();
  autoSave();
  showToast("Restored: " + name);
}}

// ── Refresh ───────────────────────────────────────────────
function refreshCurrentView() {{
  buildSubjectList();
  if (selectedRef) {{
    selectSubject(selectedRef);
  }}
}}

// ── Search ────────────────────────────────────────────────
function handleSearch(query) {{
  clearTimeout(searchTimeout);
  searchTimeout = setTimeout(() => {{
    buildSubjectList();
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
  const merged = Object.keys(mergeDecisions).length;
  const excluded = Object.keys(exclusions).length;

  document.getElementById("stat-total").textContent = total;
  document.getElementById("stat-lcsh").textContent = withLcsh;
  document.getElementById("stat-exact").textContent = exact;
  document.getElementById("stat-close").textContent = close;
  document.getElementById("stat-nomatch").textContent = noMatch;
  document.getElementById("stat-reviewed").textContent = reviewed;
  document.getElementById("stat-accepted").textContent = accepted;
  document.getElementById("stat-rejected").textContent = rejected;
  document.getElementById("stat-reassigned").textContent = reassigned;
  document.getElementById("stat-merged").textContent = merged;
  document.getElementById("stat-excluded").textContent = excluded;
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

  const mergeList = [];
  for (const [sourceRef, d] of Object.entries(mergeDecisions)) {{
    const subj = subjectIndex.find(s => s.ref === sourceRef);
    mergeList.push({{
      action: "merge",
      canonical_ref: d.targetRef,
      variant_refs: [sourceRef],
      reason: "Fold \u2018" + (subj ? subj.name : sourceRef) + "\u2019 into \u2018" + d.targetName + "\u2019",
    }});
  }}

  const exclusionList = Object.entries(exclusions).map(([ref, info]) => ({{
    ref: ref,
    name: info.name || ref,
  }}));

  const output = {{
    exported: new Date().toISOString(),
    tool: "taxonomy-review.html",
    total_decisions: decisions.length,
    decisions: decisions.sort((a, b) => a.ref.localeCompare(b.ref)),
    total_category_overrides: overridesList.length,
    category_overrides: overridesList.sort((a, b) => a.ref.localeCompare(b.ref)),
    total_merge_decisions: mergeList.length,
    merge_decisions: mergeList,
    total_exclusions: exclusionList.length,
    exclusions: exclusionList.sort((a, b) => a.ref.localeCompare(b.ref)),
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
    // Read-modify-write to preserve fields set by other tools (e.g., global_rejections)
    let existing = {{}};
    try {{
      const loadResp = await fetch("/api/load-taxonomy-decisions");
      if (loadResp.ok) existing = await loadResp.json();
    }} catch(e) {{}}

    const payload = {{
      ...existing,
      lcsh_decisions: lcshDecisions,
      category_overrides: categoryOverrides,
      merge_decisions: mergeDecisions,
      exclusions: exclusions,
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
      if (data.merge_decisions) {{
        Object.assign(mergeDecisions, data.merge_decisions);
      }}
      if (data.exclusions) {{
        Object.assign(exclusions, data.exclusions);
      }}
      updateStats();
      refreshCurrentView();
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
    case "select-subject":
      selectSubject(ref);
      break;
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
    case "open-merge":
      openMergeModal(ref);
      break;
    case "confirm-merge":
      confirmMerge(btn.dataset.source, btn.dataset.target, btn.dataset.targetName);
      break;
    case "undo-merge":
      undoMerge(ref);
      break;
    case "exclude":
      excludeTerm(ref, btn.dataset.name);
      break;
    case "restore-exclude":
      restoreExcluded(ref);
      break;
    case "goto-term":
      // Navigate to the target term
      selectSubject(ref);
      // Scroll the list item into view
      setTimeout(() => {{
        const item = document.querySelector('.list-item[data-ref="' + ref + '"]');
        if (item) item.scrollIntoView({{ behavior: "smooth", block: "center" }});
      }}, 100);
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

    print(f"  Reading: {DOC_APPEARANCES_FILE}")
    appearances = load_doc_appearances(DOC_APPEARANCES_FILE)
    print(f"    {len(appearances)} subjects with document appearances")

    print(f"  Reading: {DOC_METADATA_FILE}")
    doc_meta = load_doc_metadata(DOC_METADATA_FILE)
    print(f"    {len(doc_meta.get('documents', {}))} documents, {len(doc_meta.get('volumes', {}))} volumes")

    print(f"  Reading: {VARIANT_GROUPS_FILE}")
    variant_groups = load_variant_groups(VARIANT_GROUPS_FILE)
    print(f"    {len(variant_groups.get('groups', []))} variant groups")

    print(f"  Categories: {len(taxonomy['categories'])}")
    total_subjects = sum(
        len(s["subjects"])
        for c in taxonomy["categories"]
        for s in c["subcategories"]
    )
    print(f"  Subjects: {total_subjects}")

    html = build_html(taxonomy, mapping, overrides, appearances, doc_meta, variant_groups)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"\nWrote: {OUTPUT_FILE} ({size_kb:.0f} KB)")
    print(f"Serve locally:")
    print(f"  make serve")
    print(f"  Open http://localhost:9090/taxonomy-review.html")


if __name__ == "__main__":
    main()
