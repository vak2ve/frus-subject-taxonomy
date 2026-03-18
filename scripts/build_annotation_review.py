#!/usr/bin/env python3
"""
Generate a self-contained HTML review tool for string-match annotation results.

Discovers all string_match_results_*.json files and produces an interactive
HTML file with a volume selector, Browse-by-Document, Browse-by-Term, and
Statistics views. Volume data is loaded dynamically via fetch().

Usage:
    python3 build_annotation_review.py
"""

import glob
import json
import os
import re
import sys
from html import escape
from lxml import etree

# Pattern to extract series from volume IDs like frus1981-88v41
SERIES_RE = re.compile(r"^frus(\d{4}-\d{2,4})")

os.chdir(os.path.dirname(os.path.abspath(__file__)))

OUTPUT_HTML = "../string-match-review.html"
TAXONOMY_PATH = "../subject-taxonomy-lcsh.xml"


def build_taxonomy_index():
    """Build compact taxonomy index for cross-volume merge target selection.

    Returns a list of {r, n, c, s} dicts (ref, name, category, subcategory)
    for all active subjects in the taxonomy.
    """
    if not os.path.exists(TAXONOMY_PATH):
        print(f"  WARNING: {TAXONOMY_PATH} not found, merge targets limited to current volume")
        return []

    tree = etree.parse(TAXONOMY_PATH)
    root = tree.getroot()
    index = []
    for cat in root.findall("category"):
        cat_label = cat.get("label", "")
        for sub in cat.findall("subcategory"):
            sub_label = sub.get("label", "")
            for subj in sub.findall("subject"):
                name_el = subj.find("name")
                if name_el is None or not name_el.text:
                    continue  # skip rejected subjects
                index.append({
                    "r": subj.get("ref", ""),
                    "n": name_el.text.strip(),
                    "c": cat_label,
                    "s": sub_label,
                })
    return index


def build_manifest():
    """Discover all string_match_results_*.json files and extract metadata."""
    files = sorted(glob.glob("../data/documents/*/string_match_results_*.json"))
    manifest = []
    for f in files:
        with open(f) as fh:
            results = json.load(fh)
        meta = results["metadata"]
        # Store relative path from repo root for fetch() in HTML
        fetch_path = f.replace("../", "")  # Convert from scripts/ relative to repo-root relative
        # Extract series from volume ID
        vol_id = meta["volume_id"]
        m = SERIES_RE.match(vol_id)
        series = m.group(1) if m else "other"

        manifest.append({
            "volume_id": vol_id,
            "series": series,
            "filename": fetch_path,
            "total_matches": meta["total_matches"],
            "unique_terms_matched": meta["unique_terms_matched"],
            "total_terms_searched": meta["total_terms_searched"],
            "total_documents": meta["total_documents"],
            "documents_with_matches": meta["documents_with_matches"],
            "terms_not_matched": meta["terms_not_matched"],
            "generated": meta.get("generated", ""),
        })
    return manifest


def build_html(manifest, taxonomy_index):
    manifest_json = json.dumps(manifest, ensure_ascii=False)
    taxonomy_index_json = json.dumps(taxonomy_index, separators=(",", ":"), ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>String Match Annotation Review</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Source Sans Pro', -apple-system, BlinkMacSystemFont, sans-serif; background: #f5f5f5; color: #1b1b1b; }}

/* Header */
.header {{ background: #112e51; color: white; padding: 12px 24px; position: sticky; top: 0; z-index: 100; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }}
.header h1 {{ font-size: 18px; font-weight: 600; white-space: nowrap; }}
.header .stats {{ display: flex; gap: 16px; flex-wrap: wrap; }}
.stat {{ background: rgba(255,255,255,0.12); border-radius: 4px; padding: 4px 10px; font-size: 13px; }}
.stat b {{ color: #a8d8ff; }}

/* Volume picker */
.vol-picker {{ position: relative; }}
.vol-picker-btn {{ padding: 5px 12px; border-radius: 4px; font-size: 13px; border: 1px solid rgba(255,255,255,0.3); background: rgba(255,255,255,0.15); color: white; cursor: pointer; max-width: 300px; text-align: left; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.vol-picker-btn:hover {{ background: rgba(255,255,255,0.25); }}
.vol-picker-btn::after {{ content: ' \u25BE'; font-size: 11px; }}
.vol-dropdown {{ display: none; position: absolute; top: 100%; left: 0; z-index: 200; background: white; border: 1px solid #ccc; border-radius: 6px; box-shadow: 0 8px 24px rgba(0,0,0,0.2); width: 380px; max-height: 480px; overflow: hidden; flex-direction: column; }}
.vol-dropdown.open {{ display: flex; }}
.vol-search {{ padding: 10px; border-bottom: 1px solid #eee; }}
.vol-search input {{ width: 100%; padding: 7px 10px; border: 1px solid #ccc; border-radius: 4px; font-size: 13px; }}
.vol-list {{ flex: 1; overflow-y: auto; max-height: 400px; }}
.vol-series-header {{ padding: 8px 14px; background: #e8ecf0; font-weight: 700; font-size: 12px; color: #205493; text-transform: uppercase; letter-spacing: 0.5px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; position: sticky; top: 0; z-index: 1; }}
.vol-series-header .series-count {{ color: #71767a; font-weight: 400; font-size: 11px; }}
.vol-series-header:hover {{ background: #dce9f5; }}
.vol-item {{ padding: 8px 14px 8px 24px; cursor: pointer; font-size: 13px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #f5f5f5; color: #333; }}
.vol-item:hover {{ background: #f0f5fa; }}
.vol-item.active {{ background: #dce9f5; font-weight: 600; }}
.vol-item .vol-stats {{ font-size: 11px; color: #71767a; white-space: nowrap; }}

/* Batch import modal */
.batch-modal {{ display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.5); z-index: 300; justify-content: center; align-items: center; }}
.batch-modal.open {{ display: flex; }}
.batch-modal-content {{ background: white; border-radius: 8px; padding: 24px; width: 500px; max-height: 80vh; overflow-y: auto; box-shadow: 0 12px 36px rgba(0,0,0,0.3); }}
.batch-modal h2 {{ font-size: 18px; color: #112e51; margin-bottom: 16px; }}
.batch-series-list {{ margin: 16px 0; }}
.batch-series-row {{ display: flex; align-items: center; gap: 12px; padding: 10px 12px; border-bottom: 1px solid #eee; cursor: pointer; border-radius: 4px; }}
.batch-series-row:hover {{ background: #f0f5fa; }}
.batch-series-row.selected {{ background: #dce9f5; }}
.batch-series-row .series-name {{ font-weight: 600; font-size: 14px; color: #205493; min-width: 80px; }}
.batch-series-row .series-detail {{ font-size: 13px; color: #555; flex: 1; }}
.batch-series-row .series-badge {{ background: #fdb81e; color: #112e51; border-radius: 12px; padding: 2px 10px; font-size: 12px; font-weight: 600; }}
.batch-series-row .series-badge.done {{ background: #e0f2e9; color: #1b5e20; }}
.batch-btn-row {{ display: flex; gap: 10px; justify-content: flex-end; margin-top: 16px; }}
.batch-btn {{ padding: 8px 20px; border-radius: 4px; font-size: 14px; font-weight: 600; cursor: pointer; border: none; }}
.batch-btn.primary {{ background: #205493; color: white; }}
.batch-btn.primary:hover {{ background: #112e51; }}
.batch-btn.primary:disabled {{ background: #999; cursor: not-allowed; }}
.batch-btn.secondary {{ background: #eee; color: #333; }}
.batch-btn.secondary:hover {{ background: #ddd; }}

/* Tabs */
.tabs {{ background: #205493; display: flex; gap: 0; }}
.tab {{ padding: 10px 20px; color: rgba(255,255,255,0.7); cursor: pointer; font-size: 14px; font-weight: 600; border-bottom: 3px solid transparent; }}
.tab:hover {{ color: white; background: rgba(255,255,255,0.06); }}
.tab.active {{ color: white; border-bottom-color: #fdb81e; }}

/* Layout */
.layout {{ display: flex; height: calc(100vh - 100px); }}
.sidebar {{ width: 340px; min-width: 340px; background: white; border-right: 1px solid #ddd; display: flex; flex-direction: column; overflow: hidden; }}
.sidebar-search {{ padding: 10px; border-bottom: 1px solid #eee; }}
.sidebar-search input {{ width: 100%; padding: 8px 10px; border: 1px solid #ccc; border-radius: 4px; font-size: 14px; }}
.sidebar-filter {{ padding: 8px 10px; border-bottom: 1px solid #eee; }}
.sidebar-filter select {{ width: 100%; padding: 6px 8px; border: 1px solid #ccc; border-radius: 4px; font-size: 13px; }}
.sidebar-list {{ flex: 1; overflow-y: auto; }}
.sidebar-item {{ padding: 10px 14px; border-bottom: 1px solid #f0f0f0; cursor: pointer; display: flex; justify-content: space-between; align-items: flex-start; gap: 8px; }}
.sidebar-item:hover {{ background: #f0f5fa; }}
.sidebar-item.active {{ background: #dce9f5; border-left: 3px solid #205493; }}
.sidebar-item .title {{ font-size: 13px; line-height: 1.4; flex: 1; }}
.sidebar-item .badge {{ background: #205493; color: white; border-radius: 10px; padding: 2px 8px; font-size: 12px; font-weight: 600; white-space: nowrap; }}
.sidebar-item .cat-label {{ font-size: 11px; color: #71767a; margin-top: 2px; }}

/* Category group headers in term view */
.sidebar-cat {{ padding: 8px 14px; background: #e8ecf0; font-weight: 700; font-size: 12px; color: #205493; text-transform: uppercase; letter-spacing: 0.5px; cursor: pointer; display: flex; justify-content: space-between; }}
.sidebar-cat .count {{ color: #71767a; font-weight: 400; }}

/* Main content */
.main {{ flex: 1; overflow-y: auto; padding: 24px; }}
.main h2 {{ font-size: 20px; color: #112e51; margin-bottom: 4px; }}
.main .subtitle {{ color: #71767a; font-size: 14px; margin-bottom: 16px; }}
.match-card {{ background: white; border: 1px solid #ddd; border-radius: 6px; padding: 14px 18px; margin-bottom: 10px; position: relative; }}
.match-card .term-name {{ font-weight: 700; color: #205493; font-size: 15px; }}
.match-card .cat-path {{ font-size: 12px; color: #71767a; margin-top: 2px; }}
.match-card .doc-title {{ font-size: 13px; color: #205493; margin-top: 6px; }}
.match-card .context {{ margin-top: 8px; font-size: 14px; line-height: 1.6; color: #333; padding: 8px 12px; background: #fafafa; border-left: 3px solid #ddd; border-radius: 2px; }}
mark {{ background: #fce38a; padding: 1px 2px; border-radius: 2px; }}

/* Reject/accept toggle */
.match-actions {{ position: absolute; top: 10px; right: 12px; display: flex; gap: 6px; }}
.btn-reject, .btn-accept {{ border: none; border-radius: 4px; padding: 4px 10px; font-size: 12px; font-weight: 600; cursor: pointer; transition: all 0.15s; }}
.btn-reject {{ background: #f9e0e0; color: #b71c1c; }}
.btn-reject:hover {{ background: #f1c0c0; }}
.btn-accept {{ background: #e0f2e9; color: #1b5e20; display: none; }}
.btn-accept:hover {{ background: #c8e6c9; }}
.match-card.rejected {{ opacity: 0.45; border-color: #e0b0b0; }}
.match-card.rejected .context {{ border-left-color: #e0b0b0; text-decoration: line-through; text-decoration-color: #b71c1c; }}
.match-card.rejected .btn-reject {{ display: none; }}
.match-card.rejected .btn-accept {{ display: inline-block; }}
.match-card.excluded {{ opacity: 0.35; border-color: #fed7d7; }}
.match-card.excluded .term-name {{ text-decoration: line-through; color: #a0aec0; }}

/* Stats view */
.stats-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }}
.stats-card {{ background: white; border: 1px solid #ddd; border-radius: 8px; padding: 20px; }}
.stats-card h3 {{ font-size: 15px; color: #112e51; margin-bottom: 12px; border-bottom: 1px solid #eee; padding-bottom: 8px; }}
.stats-card .big-num {{ font-size: 36px; font-weight: 700; color: #205493; }}
.stats-card .label {{ font-size: 13px; color: #71767a; }}
.stats-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
.stats-table th {{ text-align: left; padding: 6px 10px; background: #f0f0f0; font-weight: 600; }}
.stats-table td {{ padding: 6px 10px; border-bottom: 1px solid #eee; }}
.stats-table td:last-child {{ text-align: right; font-weight: 600; }}

.empty-state {{ color: #71767a; font-size: 15px; text-align: center; padding: 60px 20px; }}

/* Unmatched terms section */
.unmatched-list {{ max-height: 400px; overflow-y: auto; }}
.unmatched-item {{ padding: 4px 0; font-size: 13px; display: flex; justify-content: space-between; border-bottom: 1px solid #f5f5f5; }}
.unmatched-item .cat {{ color: #71767a; font-size: 12px; }}

.hidden {{ display: none !important; }}

/* Export/import buttons */
.header-actions {{ display: flex; gap: 8px; margin-left: auto; }}
.header-btn {{ background: rgba(255,255,255,0.15); color: white; border: 1px solid rgba(255,255,255,0.3); border-radius: 4px; padding: 5px 12px; font-size: 12px; font-weight: 600; cursor: pointer; white-space: nowrap; }}
.header-btn:hover {{ background: rgba(255,255,255,0.25); }}
.header-btn.export {{ background: #fdb81e; color: #112e51; border-color: #fdb81e; }}
.header-btn.export:hover {{ background: #e5a617; }}
.stat-rejections {{ background: rgba(185,28,28,0.3); }}
.stat-rejections b {{ color: #fca5a5; }}

/* Variant info */
.variant-info {{ margin: 8px 0 12px; padding: 8px 12px; background: #f0f5fa; border-radius: 4px; font-size: 13px; }}
.variant-label {{ font-weight: 600; color: #205493; margin-right: 8px; }}
.variant-tag {{ background: #dce9f5; padding: 2px 8px; border-radius: 12px; margin: 2px 4px; font-size: 12px; display: inline-block; }}
.variant-note {{ font-size: 12px; color: #71767a; font-style: italic; margin-top: 2px; }}

/* LCSH review */
.lcsh-info {{ margin: 8px 0; padding: 10px 14px; background: #fafafa; border: 1px solid #e8e8e8; border-radius: 4px; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
.lcsh-badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 11px; font-weight: 600; text-transform: uppercase; }}
.lcsh-badge.exact {{ background: #e0f2e9; color: #1b5e20; }}
.lcsh-badge.good_close {{ background: #fff3e0; color: #e65100; }}
.lcsh-badge.bad_close {{ background: #fce4ec; color: #b71c1c; }}
.lcsh-form {{ font-size: 13px; color: #333; flex: 1; }}
.lcsh-link {{ font-size: 12px; color: #205493; text-decoration: none; }}
.lcsh-link:hover {{ text-decoration: underline; }}
.lcsh-actions {{ display: flex; gap: 6px; }}
.btn-lcsh {{ border: none; border-radius: 4px; padding: 3px 8px; font-size: 11px; font-weight: 600; cursor: pointer; }}
.btn-lcsh-accept {{ background: #e0f2e9; color: #1b5e20; }}
.btn-lcsh-reject {{ background: #f9e0e0; color: #b71c1c; }}
.btn-lcsh-accept:hover {{ background: #c8e6c9; }}
.btn-lcsh-reject:hover {{ background: #f1c0c0; }}
.btn-lcsh.active {{ outline: 2px solid currentColor; outline-offset: 1px; }}
.lcsh-info.lcsh-rejected {{ opacity: 0.5; }}
.lcsh-info.lcsh-accepted {{ border-color: #4caf50; }}

/* Merge feature */
.merge-section {{ margin: 8px 0 16px; padding: 10px 14px; background: #f3e5f5; border: 1px solid #ce93d8; border-radius: 4px; font-size: 13px; }}
.merge-section.is-target {{ background: #ede7f6; border-color: #b39ddb; }}
.btn-merge {{ border: none; border-radius: 4px; padding: 5px 12px; font-size: 12px; font-weight: 600; cursor: pointer; background: #7b1fa2; color: white; }}
.btn-merge:hover {{ background: #6a1b9a; }}
.btn-merge-undo {{ border: none; border-radius: 4px; padding: 3px 8px; font-size: 11px; font-weight: 600; cursor: pointer; background: #f3e5f5; color: #7b1fa2; margin-left: 8px; }}
.btn-merge-undo:hover {{ background: #e1bee7; }}
.merge-target-name {{ font-weight: 700; color: #4a148c; cursor: pointer; }}
.merge-target-name:hover {{ text-decoration: underline; }}
.merge-source-tag {{ background: #e1bee7; padding: 2px 8px; border-radius: 12px; margin: 2px 4px; font-size: 12px; display: inline-block; }}
.sidebar-item.merged {{ opacity: 0.5; font-style: italic; }}
.sidebar-item.merged .merge-arrow {{ font-size: 11px; color: #7b1fa2; display: block; margin-top: 2px; }}
.sidebar-item.excluded {{ opacity: 0.4; }}
.sidebar-item.excluded .title {{ text-decoration: line-through; color: #a0aec0; }}
.sidebar-item.excluded .exclude-marker {{ font-size: 10px; color: #e53e3e; display: block; margin-top: 2px; }}
.sidebar-item.global-rejected {{ opacity: 0.4; }}
.sidebar-item.global-rejected .title {{ color: #b71c1c; }}
.sidebar-item.global-rejected .reject-marker {{ font-size: 10px; color: #b71c1c; display: block; margin-top: 2px; }}
.match-card.global-rejected {{ opacity: 0.35; border-color: #e0b0b0; }}
.match-card.global-rejected .context {{ border-left-color: #e0b0b0; text-decoration: line-through; text-decoration-color: #b71c1c; }}

/* Merge modal */
.merge-modal-overlay {{ display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.5); z-index: 1000; justify-content: center; align-items: center; }}
.merge-modal-overlay.visible {{ display: flex; }}
.merge-modal {{ background: white; border-radius: 8px; width: 520px; max-height: 70vh; display: flex; flex-direction: column; box-shadow: 0 8px 30px rgba(0,0,0,0.3); }}
.merge-modal-header {{ padding: 16px 20px; border-bottom: 1px solid #eee; }}
.merge-modal-header h3 {{ font-size: 16px; color: #4a148c; margin-bottom: 8px; }}
.merge-modal-search {{ width: 100%; padding: 8px 10px; border: 1px solid #ccc; border-radius: 4px; font-size: 14px; }}
.merge-modal-list {{ flex: 1; overflow-y: auto; max-height: 50vh; }}
.merge-modal-item {{ padding: 10px 20px; cursor: pointer; border-bottom: 1px solid #f5f5f5; display: flex; justify-content: space-between; align-items: center; }}
.merge-modal-item:hover {{ background: #f3e5f5; }}
.merge-modal-item .term {{ font-weight: 600; font-size: 14px; }}
.merge-modal-item .info {{ font-size: 12px; color: #71767a; }}
.merge-modal-item .occ {{ font-size: 12px; font-weight: 600; color: #7b1fa2; }}
.merge-modal-item.taxonomy-only {{ opacity: 0.7; }}
.merge-modal-item .occ.not-in-vol {{ color: #71767a; font-weight: 400; font-style: italic; }}
.merge-modal-cat {{ padding: 6px 20px; background: #f5f5f5; font-size: 11px; font-weight: 700; color: #7b1fa2; text-transform: uppercase; letter-spacing: 0.5px; }}
.merge-modal-footer {{ padding: 12px 20px; border-top: 1px solid #eee; text-align: right; }}
.merge-modal-footer button {{ border: 1px solid #ccc; border-radius: 4px; padding: 6px 16px; font-size: 13px; cursor: pointer; background: white; }}
.merge-modal-footer button:hover {{ background: #f5f5f5; }}
.stat-merges {{ background: rgba(123,31,162,0.3); }}
.stat-merges b {{ color: #e1bee7; }}

/* Pipeline action buttons */
.header-btn.action {{ background: #2e8540; color: white; border-color: #2e8540; }}
.header-btn.action:hover {{ background: #267a38; }}
.header-btn.action:disabled {{ background: #71767a; border-color: #71767a; cursor: not-allowed; opacity: 0.7; }}
.header-btn.danger {{ background: #b71c1c; color: white; border-color: #b71c1c; }}
.header-btn.danger:hover {{ background: #9a1515; }}

/* Output panel */
.output-panel {{ display: none; position: fixed; bottom: 0; left: 0; right: 0; height: 260px; background: #1b2a3e; color: #e0e0e0; font-family: 'Menlo', 'Consolas', 'Liberation Mono', monospace; font-size: 12px; z-index: 200; flex-direction: column; border-top: 3px solid #205493; }}
.output-panel.visible {{ display: flex; }}
.output-header {{ display: flex; align-items: center; justify-content: space-between; padding: 6px 16px; background: #112e51; color: white; font-size: 13px; font-weight: 600; flex-shrink: 0; }}
.output-header button {{ background: none; border: 1px solid rgba(255,255,255,0.3); color: white; border-radius: 4px; padding: 2px 10px; font-size: 12px; cursor: pointer; }}
.output-header button:hover {{ background: rgba(255,255,255,0.15); }}
.output-body {{ flex: 1; overflow-y: auto; padding: 8px 16px; white-space: pre-wrap; word-break: break-all; }}
.output-body .line-error {{ color: #fca5a5; }}
.output-body .line-success {{ color: #86efac; }}
.output-body .line-heading {{ color: #a8d8ff; font-weight: 600; }}
.output-spinner {{ display: inline-block; width: 12px; height: 12px; border: 2px solid rgba(255,255,255,0.3); border-top-color: white; border-radius: 50%; animation: spin 0.6s linear infinite; margin-right: 8px; vertical-align: middle; }}
@keyframes spin {{ to {{ transform: rotate(360deg); }} }}
</style>
</head>
<body>

<div class="header">
    <h1 id="volume-title">String Match Annotation Review</h1>
    <div class="vol-picker" id="vol-picker">
        <button class="vol-picker-btn" id="vol-picker-btn" onclick="toggleVolPicker()">Select a volume\u2026</button>
        <div class="vol-dropdown" id="vol-dropdown">
            <div class="vol-search"><input type="text" id="vol-search" placeholder="Search volumes\u2026" oninput="filterVolumes(this.value)"></div>
            <div class="vol-list" id="vol-list"></div>
        </div>
    </div>
    <div class="stats" id="header-stats" style="display:none;">
        <span class="stat"><b id="stat-matches">--</b> matches</span>
        <span class="stat"><b id="stat-terms">--</b> terms</span>
        <span class="stat"><b id="stat-docs">--</b> docs</span>
        <span class="stat"><b id="stat-unmatched">--</b> unmatched</span>
        <span class="stat stat-rejections hidden" id="stat-rejections"><b id="reject-count">0</b> rejected</span>
        <span class="stat stat-merges hidden" id="stat-merges"><b id="merge-count">0</b> merges</span>
    </div>
    <span id="sync-status" style="font-size:12px;display:none;"></span>
    <div class="header-actions">
        <button class="header-btn" onclick="importDecisions()">Import</button>
        <button class="header-btn export" onclick="exportDecisions()">Export Decisions</button>
        <span style="width:1px;height:20px;background:rgba(255,255,255,0.3);margin:0 4px;"></span>
        <button class="header-btn action" onclick="runValidate()" id="btn-validate" title="Check data integrity across all volumes">Validate</button>
        <button class="header-btn action" onclick="runPipeline()" id="btn-pipeline" title="Apply review decisions and rebuild taxonomy for current volume" disabled>Run Pipeline</button>
        <button class="header-btn" onclick="runRebuildReview()" id="btn-rebuild" title="Rebuild this review tool with latest data">Rebuild</button>
        <button class="header-btn action" onclick="openBatchImport()" id="btn-import" title="Import volumes by series from volumes/">Batch Import</button>
        <span style="width:1px;height:20px;background:rgba(255,255,255,0.3);margin:0 4px;"></span>
        <button class="header-btn action" onclick="runOpenTaxonomyReview()" id="btn-taxonomy" title="Rebuild taxonomy review tool and open in new tab">Taxonomy Review &rarr;</button>
    </div>
</div>

<div class="output-panel" id="output-panel">
    <div class="output-header">
        <span id="output-title">Output</span>
        <button onclick="closeOutputPanel()">Close</button>
    </div>
    <div class="output-body" id="output-body"></div>
</div>

<div class="tabs">
    <div class="tab active" data-view="documents" onclick="switchView('documents')">Browse by Document</div>
    <div class="tab" data-view="terms" onclick="switchView('terms')">Browse by Term</div>
    <div class="tab" data-view="stats" onclick="switchView('stats')">Statistics</div>
</div>

<div class="layout">
    <div class="sidebar" id="sidebar">
        <div class="sidebar-search">
            <input type="text" id="search-input" placeholder="Search..." oninput="filterSidebar()">
        </div>
        <div class="sidebar-filter">
            <select id="cat-filter" onchange="filterSidebar()">
                <option value="">All categories</option>
            </select>
        </div>
        <div class="sidebar-list" id="sidebar-list"></div>
    </div>
    <div class="main" id="main-content">
        <div class="empty-state">Select a volume from the dropdown above to begin reviewing.</div>
    </div>
</div>

<div class="merge-modal-overlay" id="merge-modal">
    <div class="merge-modal">
        <div class="merge-modal-header">
            <h3 id="merge-modal-title">Merge into another term</h3>
            <input class="merge-modal-search" id="merge-modal-search" type="text" placeholder="Search terms..." oninput="filterMergeModal()">
        </div>
        <div class="merge-modal-list" id="merge-modal-list"></div>
        <div class="merge-modal-footer">
            <button onclick="closeMergeModal()">Cancel</button>
        </div>
    </div>
</div>

<div class="batch-modal" id="batch-modal" onclick="if(event.target===this)closeBatchImport()">
    <div class="batch-modal-content">
        <h2>Batch Import Volumes</h2>
        <p style="font-size:13px;color:#555;margin-bottom:12px;">Select a series to import all unprocessed volumes. New volumes in <code>volumes/</code> will be split, annotated, and added to the review tool.</p>
        <div class="batch-series-list" id="batch-series-list">
            <div style="text-align:center;color:#999;padding:20px;">Loading series information...</div>
        </div>
        <div class="batch-btn-row">
            <button class="batch-btn secondary" onclick="closeBatchImport()">Cancel</button>
            <button class="batch-btn primary" id="batch-start-btn" onclick="runBatchImport()" disabled>Import Selected Series</button>
        </div>
    </div>
</div>

<script id="volume-manifest" type="application/json">
{manifest_json}
</script>

<script id="taxonomy-index" type="application/json">
{taxonomy_index_json}
</script>

<script>
// ── Manifest and state ──────────────────────────────────
const manifest = JSON.parse(document.getElementById('volume-manifest').textContent);
const taxonomyIndex = JSON.parse(document.getElementById('taxonomy-index').textContent);
let data = null;
let currentVolumeId = null;
let currentView = 'documents';
let selectedId = null;
let rejections = {{}};
let lcshDecisions = {{}};
let mergeDecisions = {{}};
let mergeModalSourceRef = null;
let globalExclusions = {{}};  // ref -> {{ name }} — shared with taxonomy review
let globalRejections = {{}};  // ref -> {{ name }} — terms rejected across all volumes

function storageKey(prefix) {{
    return prefix + '-' + currentVolumeId;
}}

// ── Server sync ─────────────────────────────────────────

let saveTimeout = null;
let serverAvailable = null;  // null = unknown, true/false after first check

function showSyncStatus(msg, isError) {{
    let el = document.getElementById('sync-status');
    if (!el) return;
    el.textContent = msg;
    el.style.color = isError ? '#fca5a5' : '#a8d8ff';
    el.style.display = msg ? 'inline' : 'none';
}}

async function loadDecisionsFromServer(volumeId) {{
    // Try server first
    try {{
        const resp = await fetch('/api/load-decisions/' + volumeId);
        if (resp.ok) {{
            const saved = await resp.json();
            serverAvailable = true;

            // Restore rejections
            if (saved.rejections && Array.isArray(saved.rejections)) {{
                for (const r of saved.rejections) {{
                    if (r.key) rejections[r.key] = true;
                }}
            }}
            // Restore LCSH decisions
            if (saved.lcsh_decisions && Array.isArray(saved.lcsh_decisions)) {{
                for (const d of saved.lcsh_decisions) {{
                    if (d.ref && d.decision) lcshDecisions[d.ref] = d.decision;
                }}
            }}
            // Restore merge decisions
            if (saved.merge_decisions && Array.isArray(saved.merge_decisions)) {{
                for (const m of saved.merge_decisions) {{
                    if (m.source_ref && m.target_ref) {{
                        mergeDecisions[m.source_ref] = {{
                            targetRef: m.target_ref,
                            targetName: m.target_term || m.target_ref,
                        }};
                    }}
                }}
            }}

            // Also sync to localStorage as cache
            try {{
                localStorage.setItem(storageKey('annotation-rejections'), JSON.stringify(rejections));
                localStorage.setItem(storageKey('lcsh-decisions'), JSON.stringify(lcshDecisions));
                localStorage.setItem(storageKey('merge-decisions'), JSON.stringify(getLocalMerges()));
            }} catch(e) {{}}

            const total = Object.keys(rejections).length + Object.keys(lcshDecisions).length + Object.keys(mergeDecisions).length;
            if (total > 0) showSyncStatus('Loaded ' + total + ' decisions from disk', false);
            else showSyncStatus('', false);
            return;
        }}
    }} catch(e) {{
        serverAvailable = false;
    }}

    // Fall back to localStorage
    try {{
        const stored = localStorage.getItem(storageKey('annotation-rejections'));
        if (stored) rejections = JSON.parse(stored);
    }} catch(e) {{}}
    try {{
        const stored = localStorage.getItem(storageKey('lcsh-decisions'));
        if (stored) lcshDecisions = JSON.parse(stored);
    }} catch(e) {{}}
    try {{
        const stored = localStorage.getItem(storageKey('merge-decisions'));
        if (stored) mergeDecisions = JSON.parse(stored);
    }} catch(e) {{}}

    showSyncStatus('Using localStorage (no server)', true);
}}

function saveDecisionsToServer() {{
    if (serverAvailable === false || !currentVolumeId) return;

    // Debounce: wait 500ms after last change before saving
    if (saveTimeout) clearTimeout(saveTimeout);
    saveTimeout = setTimeout(async () => {{
        try {{
            const resp = await fetch('/api/save-decisions', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify({{
                    volume_id: currentVolumeId,
                    rejections: rejections,
                    lcsh_decisions: lcshDecisions,
                    // Only save per-volume merges, not taxonomy-sourced ones
                    merge_decisions: Object.fromEntries(
                        Object.entries(mergeDecisions).filter(([k, v]) => !v.fromTaxonomy)
                    ),
                }}),
            }});
            if (resp.ok) {{
                serverAvailable = true;
                showSyncStatus('Saved', false);
                setTimeout(() => showSyncStatus('', false), 2000);
            }} else {{
                showSyncStatus('Save failed', true);
            }}
        }} catch(e) {{
            serverAvailable = false;
            showSyncStatus('Server unavailable', true);
        }}
    }}, 500);
}}

// ── Volume selector ─────────────────────────────────────

// ── Series-grouped volume picker ───────────────────────

function getSeriesGroups() {{
    const groups = {{}};
    for (const vol of manifest) {{
        const s = vol.series || 'other';
        if (!groups[s]) groups[s] = [];
        groups[s].push(vol);
    }}
    // Sort series keys descending (most recent first)
    return Object.keys(groups).sort().reverse().map(s => ({{ series: s, volumes: groups[s] }}));
}}

function populateVolumeSelector() {{
    const list = document.getElementById('vol-list');
    list.innerHTML = '';
    const groups = getSeriesGroups();

    for (const grp of groups) {{
        const hdr = document.createElement('div');
        hdr.className = 'vol-series-header';
        hdr.dataset.series = grp.series;
        hdr.innerHTML = '<span>Series ' + escapeHtml(grp.series) + '</span><span class="series-count">' + grp.volumes.length + ' volume' + (grp.volumes.length !== 1 ? 's' : '') + '</span>';
        list.appendChild(hdr);

        for (const vol of grp.volumes) {{
            const item = document.createElement('div');
            item.className = 'vol-item';
            item.dataset.volid = vol.volume_id;
            item.dataset.series = grp.series;
            item.innerHTML = '<span>' + escapeHtml(vol.volume_id) + '</span><span class="vol-stats">' + vol.total_matches.toLocaleString() + ' matches, ' + vol.total_documents + ' docs</span>';
            item.onclick = () => selectVolume(vol.volume_id);
            list.appendChild(item);
        }}
    }}
}}

function toggleVolPicker() {{
    const dd = document.getElementById('vol-dropdown');
    dd.classList.toggle('open');
    if (dd.classList.contains('open')) {{
        document.getElementById('vol-search').focus();
        // Close on outside click
        setTimeout(() => document.addEventListener('click', closeVolPickerOutside), 10);
    }}
}}

function closeVolPickerOutside(e) {{
    const picker = document.getElementById('vol-picker');
    if (!picker.contains(e.target)) {{
        document.getElementById('vol-dropdown').classList.remove('open');
        document.removeEventListener('click', closeVolPickerOutside);
    }}
}}

function filterVolumes(query) {{
    const q = query.toLowerCase().trim();
    const list = document.getElementById('vol-list');
    const items = list.querySelectorAll('.vol-item');
    const headers = list.querySelectorAll('.vol-series-header');

    // Track which series have visible items
    const visibleSeries = new Set();

    items.forEach(item => {{
        const match = !q || item.dataset.volid.toLowerCase().includes(q);
        item.style.display = match ? '' : 'none';
        if (match) visibleSeries.add(item.dataset.series);
    }});

    headers.forEach(hdr => {{
        hdr.style.display = visibleSeries.has(hdr.dataset.series) ? '' : 'none';
    }});
}}

function selectVolume(volId) {{
    document.getElementById('vol-dropdown').classList.remove('open');
    document.getElementById('vol-picker-btn').textContent = volId;
    document.removeEventListener('click', closeVolPickerOutside);

    // Highlight active item
    document.querySelectorAll('.vol-item').forEach(el => el.classList.toggle('active', el.dataset.volid === volId));

    loadVolume(volId);
}}

async function loadVolume(volumeId) {{
    if (!volumeId) return;

    const entry = manifest.find(v => v.volume_id === volumeId);
    if (!entry) {{
        console.error('Volume not found in manifest:', volumeId);
        return;
    }}

    // Show loading state
    document.getElementById('main-content').innerHTML =
        '<div class="empty-state">Loading ' + escapeHtml(volumeId) + '\u2026</div>';
    document.getElementById('sidebar-list').innerHTML = '';

    try {{
        const resp = await fetch(entry.filename);
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const results = await resp.json();
        await initializeVolume(results);
    }} catch (err) {{
        document.getElementById('main-content').innerHTML =
            '<div class="empty-state" style="color:#b71c1c;">' +
            'Error loading ' + escapeHtml(volumeId) + ': ' + escapeHtml(err.message) +
            '<br><br>Start the server with:<br>' +
            '<code>python3 serve.py</code></div>';
    }}
}}

async function initializeVolume(results) {{
    data = results;
    currentVolumeId = data.metadata.volume_id;
    currentView = 'documents';
    selectedId = null;

    // Update header
    document.getElementById('volume-title').textContent =
        'Review: ' + currentVolumeId;
    document.getElementById('header-stats').style.display = '';
    document.getElementById('btn-pipeline').disabled = false;

    // Update stats
    const meta = data.metadata;
    document.getElementById('stat-matches').textContent = meta.total_matches.toLocaleString();
    document.getElementById('stat-terms').textContent =
        meta.unique_terms_matched + ' / ' + meta.total_terms_searched;
    document.getElementById('stat-docs').textContent =
        meta.documents_with_matches + ' / ' + meta.total_documents;
    document.getElementById('stat-unmatched').textContent = meta.terms_not_matched;

    // Rebuild category filter
    const catFilter = document.getElementById('cat-filter');
    catFilter.innerHTML = '<option value="">All categories</option>';
    const categories = new Set();
    for (const ref in data.by_term) {{
        categories.add(data.by_term[ref].category);
    }}
    for (const t of data.unmatched_terms) {{
        categories.add(t.category);
    }}
    [...categories].sort().forEach(cat => {{
        const opt = document.createElement('option');
        opt.value = cat;
        opt.textContent = cat;
        catFilter.appendChild(opt);
    }});

    // Load decisions: try server first, fall back to localStorage
    rejections = {{}};
    lcshDecisions = {{}};
    mergeDecisions = {{}};
    await loadDecisionsFromServer(currentVolumeId);

    // Re-apply taxonomy decisions (merges, global rejections) on top of per-volume state
    await loadTaxonomyDecisions();
    applyGlobalRejectionsToVolume();

    // Update counts
    updateRejectCount();
    updateMergeCount();

    // Reset tabs and render
    document.querySelectorAll('.tab').forEach(
        t => t.classList.toggle('active', t.dataset.view === 'documents')
    );
    document.getElementById('sidebar').classList.remove('hidden');
    document.getElementById('search-input').value = '';
    renderSidebar();
    document.getElementById('main-content').innerHTML =
        '<div class="empty-state">Select a document or term from the sidebar to view annotations.</div>';
}}

// ── View switching ──────────────────────────────────────

function switchView(view) {{
    if (!data) return;
    currentView = view;
    selectedId = null;
    document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.view === view));
    document.getElementById('sidebar').classList.toggle('hidden', view === 'stats');
    if (view === 'stats') {{
        renderStats();
    }} else {{
        renderSidebar();
        document.getElementById('main-content').innerHTML = '<div class="empty-state">Select an item from the sidebar.</div>';
    }}
}}

function escapeHtml(s) {{
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}}

function highlightTerm(sentence, matchedText) {{
    if (!matchedText) return escapeHtml(sentence);
    const escaped = escapeHtml(sentence);
    const escapedTerm = escapeHtml(matchedText);
    const re = new RegExp('(' + escapedTerm.replace(/[.*+?^${{}}()|[\\]\\\\]/g, '\\\\$&') + ')', 'gi');
    return escaped.replace(re, '<mark>$1</mark>');
}}

// ── Rejection management ────────────────────────────────

function matchKey(docId, ref, position) {{
    return docId + ':' + ref + ':' + position;
}}

function saveRejections() {{
    try {{
        localStorage.setItem(storageKey('annotation-rejections'), JSON.stringify(rejections));
    }} catch(e) {{}}
    updateRejectCount();
    saveDecisionsToServer();
}}

function rejectMatch(key, btn) {{
    rejections[key] = true;
    saveRejections();
    const card = btn.closest('.match-card');
    if (card) card.classList.add('rejected');
}}

function acceptMatch(key, btn) {{
    delete rejections[key];
    saveRejections();
    const card = btn.closest('.match-card');
    if (card) card.classList.remove('rejected');
}}

function updateRejectCount() {{
    const count = Object.keys(rejections).length;
    const el = document.getElementById('reject-count');
    const wrapper = document.getElementById('stat-rejections');
    if (el) el.textContent = count;
    if (wrapper) wrapper.classList.toggle('hidden', count === 0);
}}

// ── Export / Import ─────────────────────────────────────

function exportDecisions() {{
    if (!data) return;
    const entries = [];
    for (const key of Object.keys(rejections)) {{
        const [docId, ref, pos] = key.split(':');
        const term = data.by_term[ref];
        const doc = data.by_document[docId];
        entries.push({{
            key,
            docId,
            ref,
            position: parseInt(pos),
            term: term ? term.term : ref,
            category: term ? term.category : '',
            doc_title: doc ? doc.title : '',
        }});
    }}
    const lcshEntries = [];
    for (const [ref, decision] of Object.entries(lcshDecisions)) {{
        const term = data.by_term[ref];
        lcshEntries.push({{
            ref,
            term: term ? term.term : ref,
            lcsh_uri: term ? term.lcsh_uri : '',
            decision,
        }});
    }}

    const mergeEntries = [];
    const taxonomyMergeEntries = [];
    const overrideSnippets = [];
    for (const [sourceRef, decision] of Object.entries(mergeDecisions)) {{
        const sourceTerm = data.by_term[sourceRef];
        const targetTerm = data.by_term[decision.targetRef];
        const sourceName = sourceTerm ? sourceTerm.term : lookupTaxonomyName(sourceRef);
        const targetName = targetTerm ? targetTerm.term : (decision.targetName || lookupTaxonomyName(decision.targetRef));
        const entry = {{
            source_ref: sourceRef,
            source_term: sourceName,
            target_ref: decision.targetRef,
            target_term: targetName,
        }};
        if (decision.fromTaxonomy) {{
            entry.source = 'taxonomy-review';
            taxonomyMergeEntries.push(entry);
        }} else {{
            mergeEntries.push(entry);
            overrideSnippets.push({{
                action: 'merge',
                canonical_ref: decision.targetRef,
                variant_refs: [sourceRef],
                reason: 'Fold \u2018' + sourceName + '\u2019 into \u2018' + targetName + '\u2019',
            }});
        }}
    }}

    const output = {{
        volume_id: data.metadata.volume_id,
        exported: new Date().toISOString(),
        total_rejections: entries.length,
        rejections: entries.sort((a, b) => a.term.localeCompare(b.term)),
        total_lcsh_decisions: lcshEntries.length,
        lcsh_decisions: lcshEntries.sort((a, b) => a.term.localeCompare(b.term)),
        total_merge_decisions: mergeEntries.length,
        merge_decisions: mergeEntries.sort((a, b) => a.source_term.localeCompare(b.source_term)),
        taxonomy_merges_applied: taxonomyMergeEntries.length,
        taxonomy_merges: taxonomyMergeEntries.sort((a, b) => a.source_term.localeCompare(b.source_term)),
        variant_overrides_snippet: overrideSnippets,
    }};
    const blob = new Blob([JSON.stringify(output, null, 2)], {{ type: 'application/json' }});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'annotation_rejections_' + data.metadata.volume_id + '.json';
    a.click();
    URL.revokeObjectURL(url);
}}

function importDecisions() {{
    if (!data) {{ alert('Load a volume first.'); return; }}
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.json';
    input.onchange = function(e) {{
        const file = e.target.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = function(ev) {{
            try {{
                const imported = JSON.parse(ev.target.result);
                let count = 0;
                if (imported.rejections && Array.isArray(imported.rejections)) {{
                    for (const r of imported.rejections) {{
                        if (r.key) {{ rejections[r.key] = true; count++; }}
                    }}
                }}
                let lcshCount = 0;
                if (imported.lcsh_decisions && Array.isArray(imported.lcsh_decisions)) {{
                    for (const d of imported.lcsh_decisions) {{
                        if (d.ref && d.decision) {{ lcshDecisions[d.ref] = d.decision; lcshCount++; }}
                    }}
                    saveLcshDecisions();
                }}
                let mergeCount = 0;
                if (imported.merge_decisions && Array.isArray(imported.merge_decisions)) {{
                    for (const m of imported.merge_decisions) {{
                        if (m.source_ref && m.target_ref) {{
                            mergeDecisions[m.source_ref] = {{ targetRef: m.target_ref, targetName: m.target_term || m.target_ref }};
                            mergeCount++;
                        }}
                    }}
                    saveMergeDecisions();
                }}
                saveRejections();
                alert('Imported ' + count + ' rejections, ' + lcshCount + ' LCSH decisions, ' + mergeCount + ' merge decisions.');
                if (selectedId) {{
                    if (currentView === 'documents') selectDoc(selectedId);
                    else if (currentView === 'terms') selectTerm(selectedId);
                }}
            }} catch(err) {{
                alert('Error importing file: ' + err.message);
            }}
        }};
        reader.readAsText(file);
    }};
    input.click();
}}

// ── LCSH decision management ────────────────────────────

function saveLcshDecisions() {{
    try {{
        localStorage.setItem(storageKey('lcsh-decisions'), JSON.stringify(lcshDecisions));
    }} catch(e) {{}}
    saveDecisionsToServer();
}}

function setLcshDecision(ref, decision, btn) {{
    if (lcshDecisions[ref] === decision) {{
        delete lcshDecisions[ref];
    }} else {{
        lcshDecisions[ref] = decision;
    }}
    saveLcshDecisions();
    const infoEl = btn.closest('.lcsh-info');
    if (infoEl) {{
        infoEl.classList.remove('lcsh-accepted', 'lcsh-rejected');
        if (lcshDecisions[ref] === 'accepted') infoEl.classList.add('lcsh-accepted');
        if (lcshDecisions[ref] === 'rejected') infoEl.classList.add('lcsh-rejected');
    }}
    const actions = btn.closest('.lcsh-actions');
    if (actions) {{
        actions.querySelectorAll('.btn-lcsh').forEach(b => b.classList.remove('active'));
        if (lcshDecisions[ref]) btn.classList.add('active');
    }}
}}

function renderLcshInfo(ref, term) {{
    if (!term.lcsh_uri) return '';
    const lcshMatch = term.lcsh_match || '';
    const badgeClass = lcshMatch === 'exact' ? 'exact' : lcshMatch.includes('good') ? 'good_close' : lcshMatch.includes('bad') ? 'bad_close' : '';
    const badge = badgeClass ? `<span class="lcsh-badge ${{badgeClass}}">${{escapeHtml(lcshMatch)}}</span>` : '';
    const decision = lcshDecisions[ref] || '';
    const stateClass = decision === 'accepted' ? ' lcsh-accepted' : decision === 'rejected' ? ' lcsh-rejected' : '';
    const acceptActive = decision === 'accepted' ? ' active' : '';
    const rejectActive = decision === 'rejected' ? ' active' : '';

    return `<div class="lcsh-info${{stateClass}}">
        ${{badge}}
        <span class="lcsh-form">LCSH: <b>${{escapeHtml(term.lcsh_uri.split('/').pop())}}</b></span>
        <a class="lcsh-link" href="${{escapeHtml(term.lcsh_uri)}}" target="_blank">View in LCSH</a>
        <div class="lcsh-actions">
            <button class="btn-lcsh btn-lcsh-accept${{acceptActive}}" onclick="setLcshDecision('${{ref}}','accepted',this)" title="Accept LCSH mapping">&#x2713; Accept</button>
            <button class="btn-lcsh btn-lcsh-reject${{rejectActive}}" onclick="setLcshDecision('${{ref}}','rejected',this)" title="Reject LCSH mapping">&#x2717; Reject</button>
        </div>
    </div>`;
}}

// ── Merge decision management ───────────────────────────

function getLocalMerges() {{
    // Filter out taxonomy-sourced merges for local storage/export
    return Object.fromEntries(
        Object.entries(mergeDecisions).filter(([k, v]) => !v.fromTaxonomy)
    );
}}

function saveMergeDecisions() {{
    try {{
        localStorage.setItem(storageKey('merge-decisions'), JSON.stringify(getLocalMerges()));
    }} catch(e) {{}}
    updateMergeCount();
    saveDecisionsToServer();
}}

function updateMergeCount() {{
    const count = Object.keys(mergeDecisions).length;
    const el = document.getElementById('merge-count');
    const wrapper = document.getElementById('stat-merges');
    if (el) el.textContent = count;
    if (wrapper) wrapper.classList.toggle('hidden', count === 0);
}}

function openMergeModal(sourceRef) {{
    mergeModalSourceRef = sourceRef;
    const sourceTerm = data.by_term[sourceRef];
    const title = document.getElementById('merge-modal-title');
    title.textContent = 'Merge "' + (sourceTerm ? sourceTerm.term : sourceRef) + '" into\u2026';
    document.getElementById('merge-modal-search').value = '';
    populateMergeModal('');
    document.getElementById('merge-modal').classList.add('visible');
    document.getElementById('merge-modal-search').focus();
}}

function closeMergeModal() {{
    document.getElementById('merge-modal').classList.remove('visible');
    mergeModalSourceRef = null;
}}

function filterMergeModal() {{
    const q = document.getElementById('merge-modal-search').value.toLowerCase();
    populateMergeModal(q);
}}

function populateMergeModal(query) {{
    if (!data) return;
    const list = document.getElementById('merge-modal-list');
    const byCat = {{}};
    // 1. Add terms from current volume results (with occurrence counts)
    for (const [ref, term] of Object.entries(data.by_term)) {{
        if (ref === mergeModalSourceRef) continue;
        if (query && !term.term.toLowerCase().includes(query)) continue;
        if (!byCat[term.category]) byCat[term.category] = [];
        byCat[term.category].push({{ ref, name: term.term, sub: term.subcategory, occ: term.total_occurrences, inVolume: true }});
    }}
    // 2. Add taxonomy terms NOT in current volume
    const volumeRefs = new Set(Object.keys(data.by_term));
    volumeRefs.add(mergeModalSourceRef);
    for (const t of taxonomyIndex) {{
        if (volumeRefs.has(t.r)) continue;
        if (query && !t.n.toLowerCase().includes(query)) continue;
        if (!byCat[t.c]) byCat[t.c] = [];
        byCat[t.c].push({{ ref: t.r, name: t.n, sub: t.s, occ: 0, inVolume: false }});
    }}
    let html = '';
    for (const cat of Object.keys(byCat).sort()) {{
        const terms = byCat[cat].sort((a, b) => {{
            if (a.inVolume !== b.inVolume) return a.inVolume ? -1 : 1;
            if (a.inVolume) return b.occ - a.occ;
            return a.name.localeCompare(b.name);
        }});
        html += `<div class="merge-modal-cat">${{escapeHtml(cat)}} (${{terms.length}})</div>`;
        for (const t of terms) {{
            const badge = t.inVolume
                ? `<span class="occ">${{t.occ}} occ</span>`
                : `<span class="occ not-in-vol">not in volume</span>`;
            html += `<div class="merge-modal-item${{t.inVolume ? '' : ' taxonomy-only'}}" onclick="confirmMerge('${{mergeModalSourceRef}}','${{t.ref}}')">
                <div><div class="term">${{escapeHtml(t.name)}}</div><div class="info">${{escapeHtml(t.sub)}}</div></div>
                ${{badge}}
            </div>`;
        }}
    }}
    if (!html) html = '<div style="padding:20px;color:#71767a;text-align:center;">No matching terms.</div>';
    list.innerHTML = html;
}}

function lookupTaxonomyName(ref) {{
    const entry = taxonomyIndex.find(t => t.r === ref);
    return entry ? entry.n : ref;
}}

function confirmMerge(sourceRef, targetRef) {{
    // Don't override taxonomy-sourced merges
    if (mergeDecisions[sourceRef] && mergeDecisions[sourceRef].fromTaxonomy) return;
    const targetTerm = data.by_term[targetRef];
    const targetName = targetTerm ? targetTerm.term : lookupTaxonomyName(targetRef);
    mergeDecisions[sourceRef] = {{
        targetRef: targetRef,
        targetName: targetName,
    }};
    saveMergeDecisions();
    closeMergeModal();
    if (currentView === 'terms') {{
        selectTerm(sourceRef);
        renderSidebar();
    }}
}}

function undoMerge(sourceRef) {{
    // Don't undo taxonomy-sourced merges
    if (mergeDecisions[sourceRef] && mergeDecisions[sourceRef].fromTaxonomy) return;
    delete mergeDecisions[sourceRef];
    saveMergeDecisions();
    if (currentView === 'terms') {{
        selectTerm(sourceRef);
        renderSidebar();
    }}
}}

function getMergeSources(targetRef) {{
    const sources = [];
    for (const [sourceRef, decision] of Object.entries(mergeDecisions)) {{
        if (decision.targetRef === targetRef) {{
            const sourceTerm = data.by_term[sourceRef];
            sources.push({{ ref: sourceRef, name: sourceTerm ? sourceTerm.term : sourceRef }});
        }}
    }}
    return sources;
}}

function renderMergeSection(ref, term) {{
    if (mergeDecisions[ref]) {{
        const d = mergeDecisions[ref];
        if (d.fromTaxonomy) {{
            return `<div class="merge-section" style="background:#f3e8ff;border-color:#d1c4e9;">
                <span style="color:#7b1fa2;font-weight:600;">Merged into:</span>
                <span class="merge-target-name" onclick="selectTerm('${{d.targetRef}}')">${{escapeHtml(d.targetName)}}</span>
                <span style="font-size:11px;color:#9575cd;margin-left:8px;">(from taxonomy review)</span>
            </div>`;
        }}
        return `<div class="merge-section">
            <span style="color:#7b1fa2;font-weight:600;">Merged into:</span>
            <span class="merge-target-name" onclick="selectTerm('${{d.targetRef}}')">${{escapeHtml(d.targetName)}}</span>
            <button class="btn-merge-undo" onclick="undoMerge('${{ref}}')">&#x2717; Undo merge</button>
        </div>`;
    }}

    const sources = getMergeSources(ref);
    let html = '';
    if (sources.length > 0) {{
        html += `<div class="merge-section is-target">
            <span style="color:#4a148c;font-weight:600;">Receiving merges from:</span> `;
        for (const s of sources) {{
            html += `<span class="merge-source-tag" style="cursor:pointer;" onclick="selectTerm('${{s.ref}}')">${{escapeHtml(s.name)}}</span>`;
        }}
        html += `</div>`;
    }}

    html += `<div class="merge-section" style="background:#fafafa;border-color:#ddd;">
        <button class="btn-merge" onclick="openMergeModal('${{ref}}')">Merge into another term\u2026</button>
    </div>`;

    return html;
}}

// ── Sidebar rendering ───────────────────────────────────

function renderSidebar() {{
    if (!data) return;
    const list = document.getElementById('sidebar-list');
    const search = document.getElementById('search-input').value.toLowerCase();
    const catVal = document.getElementById('cat-filter').value;

    if (currentView === 'documents') {{
        renderDocSidebar(list, search, catVal);
    }} else if (currentView === 'terms') {{
        renderTermSidebar(list, search, catVal);
    }}
}}

function renderDocSidebar(list, search, catVal) {{
    let html = '';
    const docs = Object.entries(data.by_document).sort((a, b) => b[1].match_count - a[1].match_count);
    for (const [docId, doc] of docs) {{
        if (search && !doc.title.toLowerCase().includes(search) && !docId.toLowerCase().includes(search)) continue;
        if (catVal) {{
            const hasCat = doc.matches.some(m => m.category === catVal);
            if (!hasCat) continue;
        }}
        const active = selectedId === docId ? ' active' : '';
        html += `<div class="sidebar-item${{active}}" onclick="selectDoc('${{docId}}')">
            <div>
                <div class="title"><b>${{docId}}</b>: ${{escapeHtml(doc.title.substring(0, 60))}}</div>
                <div class="cat-label">${{doc.date || 'No date'}}</div>
            </div>
            <span class="badge">${{doc.match_count}}</span>
        </div>`;
    }}
    if (!html) html = '<div class="empty-state">No matching documents.</div>';
    list.innerHTML = html;
}}

function renderTermSidebar(list, search, catVal) {{
    const byCat = {{}};
    for (const [ref, term] of Object.entries(data.by_term)) {{
        if (catVal && term.category !== catVal) continue;
        if (search && !term.term.toLowerCase().includes(search)) continue;
        if (!byCat[term.category]) byCat[term.category] = [];
        byCat[term.category].push({{ ref, ...term }});
    }}

    let html = '';
    const sortedCats = Object.keys(byCat).sort();
    for (const cat of sortedCats) {{
        const terms = byCat[cat].sort((a, b) => b.total_occurrences - a.total_occurrences);
        html += `<div class="sidebar-cat">${{escapeHtml(cat)}} <span class="count">${{terms.length}} terms</span></div>`;
        for (const t of terms) {{
            const active = selectedId === t.ref ? ' active' : '';
            const isMerged = mergeDecisions[t.ref] ? ' merged' : '';
            const isExcluded = globalExclusions[t.ref] ? ' excluded' : '';
            const isGlobalRejected = globalRejections[t.ref] ? ' global-rejected' : '';
            const md = mergeDecisions[t.ref];
            const mergeLabel = md ? `<span class="merge-arrow">\u2192 ${{escapeHtml(md.targetName)}}${{md.fromTaxonomy ? ' <small style="color:#9575cd;">(tax)</small>' : ''}}</span>` : '';
            const excludeLabel = globalExclusions[t.ref] ? '<span class="exclude-marker">excluded</span>' : '';
            const rejectLabel = globalRejections[t.ref] ? '<span class="reject-marker">rejected globally</span>' : '';
            html += `<div class="sidebar-item${{active}}${{isMerged}}${{isExcluded}}${{isGlobalRejected}}" onclick="selectTerm('${{t.ref}}')">
                <div>
                    <div class="title">${{escapeHtml(t.term)}}</div>
                    <div class="cat-label">${{escapeHtml(t.subcategory)}}</div>
                    ${{mergeLabel}}
                    ${{excludeLabel}}
                    ${{rejectLabel}}
                </div>
                <span class="badge">${{t.total_occurrences}}</span>
            </div>`;
        }}
    }}
    if (!html) html = '<div class="empty-state">No matching terms.</div>';
    list.innerHTML = html;
}}

function filterSidebar() {{
    renderSidebar();
}}

// ── Document detail ─────────────────────────────────────

function selectDoc(docId) {{
    if (!data) return;
    selectedId = docId;
    renderSidebar();
    const doc = data.by_document[docId];
    if (!doc) return;

    let html = `<h2>${{escapeHtml(docId)}}: ${{escapeHtml(doc.title)}}</h2>
        <div class="subtitle">${{escapeHtml(doc.date || '')}} &mdash; ${{doc.match_count}} matches, ${{doc.unique_terms}} unique terms</div>`;

    if (doc.matches.length === 0) {{
        html += '<div class="empty-state">No taxonomy term matches found in this document.</div>';
    }} else {{
        const byCat = {{}};
        for (const m of doc.matches) {{
            if (!byCat[m.category]) byCat[m.category] = [];
            byCat[m.category].push(m);
        }}
        for (const cat of Object.keys(byCat).sort()) {{
            html += `<h3 style="margin: 16px 0 8px; color: #205493; font-size: 16px;">${{escapeHtml(cat)}}</h3>`;
            for (const m of byCat[cat]) {{
                const key = matchKey(docId, m.ref, m.position);
                const rejected = rejections[key] ? ' rejected' : '';
                const termExcluded = globalExclusions[m.ref] ? ' excluded' : '';
                const termGlobalRejected = globalRejections[m.ref] ? ' global-rejected' : '';
                html += `<div class="match-card${{rejected}}${{termExcluded}}${{termGlobalRejected}}" data-key="${{key}}">
                    <div class="match-actions">
                        <button class="btn-reject" onclick="rejectMatch('${{key}}', this)" title="Reject this match">&#x2717; Reject</button>
                        <button class="btn-accept" onclick="acceptMatch('${{key}}', this)" title="Restore this match">&#x2713; Restore</button>
                    </div>
                    <div class="term-name">${{escapeHtml(m.term)}}${{globalExclusions[m.ref] ? ' <span style="color:#e53e3e;font-size:11px;">(excluded)</span>' : ''}}${{globalRejections[m.ref] ? ' <span style="color:#b71c1c;font-size:11px;">(rejected globally)</span>' : ''}}</div>
                    <div class="cat-path">${{escapeHtml(m.category)}} &rsaquo; ${{escapeHtml(m.subcategory)}}</div>
                    ${{m.is_consolidated ? '<div class="variant-note">Matched as variant: &ldquo;' + escapeHtml(m.matched_text) + '&rdquo;</div>' : ''}}
                    <div class="context">${{highlightTerm(m.sentence, m.matched_text)}}</div>
                </div>`;
            }}
        }}
    }}
    document.getElementById('main-content').innerHTML = html;
}}

// ── Term detail ─────────────────────────────────────────

function selectTerm(ref) {{
    if (!data) return;
    selectedId = ref;
    renderSidebar();
    const term = data.by_term[ref];
    if (!term) return;

    let html = `<h2>${{escapeHtml(term.term)}}</h2>
        <div class="subtitle">${{escapeHtml(term.category)}} &rsaquo; ${{escapeHtml(term.subcategory)}}
        &mdash; ${{term.total_occurrences}} occurrences in ${{term.document_count}} documents</div>`;

    html += renderLcshInfo(ref, term);

    if (term.variant_names && term.variant_names.length > 1) {{
        html += `<div class="variant-info"><span class="variant-label">Consolidated variants:</span>`;
        for (const vn of term.variant_names) {{
            html += `<span class="variant-tag">${{escapeHtml(vn)}}</span>`;
        }}
        html += `</div>`;
    }}

    html += renderMergeSection(ref, term);

    // Global rejection controls
    if (globalRejections[ref]) {{
        html += `<div style="margin:12px 0;padding:8px 12px;background:#fff0f0;border:1px solid #e0b0b0;border-radius:6px;">
            <span style="color:#b71c1c;font-weight:600;">Rejected globally</span>
            <span style="font-size:12px;color:#999;margin-left:4px;">(all matches auto-rejected across volumes)</span>
            <button onclick="unRejectTermGlobal('${{ref}}')" style="margin-left:8px;color:#b71c1c;text-decoration:underline;border:none;background:none;cursor:pointer;font-size:13px;">Undo</button>
        </div>`;
    }} else {{
        html += `<div style="margin:12px 0;display:flex;gap:8px;flex-wrap:wrap;">
            <button onclick="rejectTermGlobal('${{ref}}', '${{escapeHtml(term.term).replace(/'/g, "\\\\'")}}')"
                style="color:#b71c1c;border:1px solid #e0b0b0;background:#fff0f0;padding:4px 12px;border-radius:4px;cursor:pointer;font-size:13px;">Reject in all volumes</button>`;
    }}

    // Global exclusion controls
    if (globalExclusions[ref]) {{
        html += `<div style="margin:${{globalRejections[ref] ? '8px 0 12px' : '0'}};padding:8px 12px;background:#fff5f5;border:1px solid #fed7d7;border-radius:6px;">
            <span style="color:#e53e3e;font-weight:600;">Excluded from taxonomy</span>
            <button onclick="restoreTermGlobal('${{ref}}')" style="margin-left:8px;color:#e53e3e;text-decoration:underline;border:none;background:none;cursor:pointer;font-size:13px;">Restore</button>
        </div>`;
    }} else if (!globalRejections[ref]) {{
        html += `
            <button onclick="excludeTermGlobal('${{ref}}', '${{escapeHtml(term.term).replace(/'/g, "\\\\'")}}')"
                style="color:#e53e3e;border:1px solid #fed7d7;background:#fff5f5;padding:4px 12px;border-radius:4px;cursor:pointer;font-size:13px;">Exclude from taxonomy</button>
        </div>`;
    }} else {{
        html += `</div>`;
    }}

    const docIds = Object.keys(term.documents).sort((a, b) => {{
        const numA = parseInt(a.replace('d', ''));
        const numB = parseInt(b.replace('d', ''));
        return numA - numB;
    }});

    for (const docId of docIds) {{
        const docInfo = data.by_document[docId];
        const occurrences = term.documents[docId];
        for (const occ of occurrences) {{
            const key = matchKey(docId, ref, occ.position);
            const rejected = rejections[key] ? ' rejected' : '';
            html += `<div class="match-card${{rejected}}" data-key="${{key}}">
                <div class="match-actions">
                    <button class="btn-reject" onclick="rejectMatch('${{key}}', this)" title="Reject this match">&#x2717; Reject</button>
                    <button class="btn-accept" onclick="acceptMatch('${{key}}', this)" title="Restore this match">&#x2713; Restore</button>
                </div>
                <div class="doc-title" style="cursor:pointer;font-weight:700;" onclick="switchView('documents');setTimeout(()=>selectDoc('${{docId}}'),50)">
                    ${{escapeHtml(docId)}}: ${{escapeHtml(docInfo ? docInfo.title : '')}}</div>
                ${{occ.is_consolidated ? '<div class="variant-note">Matched as variant: &ldquo;' + escapeHtml(occ.matched_text) + '&rdquo;</div>' : ''}}
                <div class="context">${{highlightTerm(occ.sentence, occ.matched_text)}}</div>
            </div>`;
        }}
    }}

    document.getElementById('main-content').innerHTML = html;
}}

// ── Statistics view ─────────────────────────────────────

function renderStats() {{
    if (!data) return;
    const main = document.getElementById('main-content');
    const meta = data.metadata;

    let html = `<h2>Annotation Statistics</h2>
    <div class="subtitle">String match results for ${{escapeHtml(meta.volume_id)}}</div>

    <div class="stats-grid">
        <div class="stats-card"><div class="big-num">${{meta.total_matches.toLocaleString()}}</div><div class="label">Total matches</div></div>
        <div class="stats-card"><div class="big-num">${{meta.unique_terms_matched}}</div><div class="label">Unique terms matched (of ${{meta.total_terms_searched}})</div></div>
        <div class="stats-card"><div class="big-num">${{meta.documents_with_matches}}</div><div class="label">Documents with matches (of ${{meta.total_documents}})</div></div>
        <div class="stats-card"><div class="big-num">${{meta.terms_not_matched}}</div><div class="label">Terms not found in any document</div></div>
    </div>`;

    const termList = Object.entries(data.by_term).sort((a, b) => b[1].total_occurrences - a[1].total_occurrences);
    html += `<div class="stats-card" style="margin-bottom:20px;">
        <h3>Top 25 Most Frequent Terms</h3>
        <table class="stats-table">
            <tr><th>Term</th><th>Category</th><th>Docs</th><th>Occurrences</th></tr>`;
    for (const [ref, t] of termList.slice(0, 25)) {{
        html += `<tr><td>${{escapeHtml(t.term)}}</td><td style="font-size:12px;color:#71767a;">${{escapeHtml(t.category)}}</td>
            <td style="text-align:right">${{t.document_count}}</td><td>${{t.total_occurrences}}</td></tr>`;
    }}
    html += `</table></div>`;

    const catCounts = {{}};
    for (const [ref, t] of termList) {{
        if (!catCounts[t.category]) catCounts[t.category] = {{ terms: 0, occurrences: 0 }};
        catCounts[t.category].terms++;
        catCounts[t.category].occurrences += t.total_occurrences;
    }}
    html += `<div class="stats-card" style="margin-bottom:20px;">
        <h3>Matches by Category</h3>
        <table class="stats-table">
            <tr><th>Category</th><th>Terms Matched</th><th>Total Occurrences</th></tr>`;
    for (const [cat, c] of Object.entries(catCounts).sort((a, b) => b[1].occurrences - a[1].occurrences)) {{
        html += `<tr><td>${{escapeHtml(cat)}}</td><td style="text-align:right">${{c.terms}}</td><td>${{c.occurrences}}</td></tr>`;
    }}
    html += `</table></div>`;

    const docList = Object.entries(data.by_document).sort((a, b) => b[1].match_count - a[1].match_count);
    html += `<div class="stats-card" style="margin-bottom:20px;">
        <h3>Top 20 Documents by Match Count</h3>
        <table class="stats-table">
            <tr><th>Doc</th><th>Title</th><th>Matches</th></tr>`;
    for (const [docId, d] of docList.slice(0, 20)) {{
        html += `<tr><td style="font-weight:600">${{docId}}</td><td>${{escapeHtml(d.title.substring(0, 60))}}</td><td>${{d.match_count}}</td></tr>`;
    }}
    html += `</table></div>`;

    html += `<div class="stats-card" style="margin-bottom:20px;">
        <h3>Unmatched Terms (${{data.unmatched_terms.length}})</h3>
        <div class="unmatched-list">`;
    for (const t of data.unmatched_terms) {{
        html += `<div class="unmatched-item">
            <span>${{escapeHtml(t.term)}}</span>
            <span class="cat">${{escapeHtml(t.category)}}</span>
        </div>`;
    }}
    html += `</div></div>`;

    main.innerHTML = html;
}}

// ── Pipeline actions ─────────────────────────────────────

function openOutputPanel(title) {{
    const panel = document.getElementById('output-panel');
    const body = document.getElementById('output-body');
    const titleEl = document.getElementById('output-title');
    body.innerHTML = '';
    titleEl.innerHTML = `<span class="output-spinner"></span>${{title}}`;
    panel.classList.add('visible');
}}

function closeOutputPanel() {{
    document.getElementById('output-panel').classList.remove('visible');
}}

function appendOutput(text, className) {{
    const body = document.getElementById('output-body');
    const line = document.createElement('div');
    if (className) line.className = className;
    line.textContent = text;
    body.appendChild(line);
    body.scrollTop = body.scrollHeight;
}}

function classifyLine(text) {{
    if (/FAIL|ERROR|error:/i.test(text)) return 'line-error';
    if (/COMPLETE|SUCCESS|Done|PASS/i.test(text)) return 'line-success';
    if (/^={3,}/.test(text) || /Step:/.test(text)) return 'line-heading';
    return '';
}}

function streamAction(url, title, method) {{
    openOutputPanel(title);

    fetch(url, {{ method: method || 'POST' }})
        .then(response => {{
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';

            function read() {{
                reader.read().then(({{ done, value }}) => {{
                    if (done) {{
                        finishOutput();
                        return;
                    }}
                    buffer += decoder.decode(value, {{ stream: true }});
                    const lines = buffer.split('\\n');
                    buffer = lines.pop();  // keep incomplete line in buffer
                    for (const raw of lines) {{
                        if (!raw.startsWith('data: ')) continue;
                        try {{
                            const msg = JSON.parse(raw.slice(6));
                            if (msg.type === 'output' || msg.type === 'start') {{
                                appendOutput(msg.line, classifyLine(msg.line));
                            }} else if (msg.type === 'done') {{
                                const cls = msg.status === 'success' ? 'line-success' : 'line-error';
                                const label = msg.status === 'success' ? 'Finished successfully.' : `Failed (exit code ${{msg.code}}).`;
                                appendOutput(label, cls);
                            }} else if (msg.type === 'error') {{
                                appendOutput(msg.line, 'line-error');
                            }}
                        }} catch(e) {{}}
                    }}
                    read();
                }});
            }}
            read();
        }})
        .catch(err => {{
            appendOutput('Connection error: ' + err.message, 'line-error');
            finishOutput();
        }});
}}

function finishOutput() {{
    const titleEl = document.getElementById('output-title');
    titleEl.textContent = titleEl.textContent;  // removes spinner
}}

function runValidate() {{
    streamAction('/api/validate', 'Validating data...');
}}

function runPipeline() {{
    if (!currentVolumeId) {{
        alert('Select a volume first.');
        return;
    }}
    if (!confirm(`Run the full post-review pipeline for ${{currentVolumeId}}?\\n\\nThis will apply your review decisions and rebuild the taxonomy.`)) return;
    streamAction('/api/pipeline/' + currentVolumeId, 'Pipeline: ' + currentVolumeId);
}}

function runRebuildReview() {{
    if (!confirm('Rebuild the review tool?\\n\\nThe page will reload when done.')) return;
    openOutputPanel('Rebuilding review tool...');
    fetch('/api/rebuild-review', {{ method: 'POST' }})
        .then(response => {{
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            let success = false;

            function read() {{
                reader.read().then(({{ done, value }}) => {{
                    if (done) {{
                        if (success) {{
                            appendOutput('Reloading page...', 'line-success');
                            setTimeout(() => window.location.reload(), 1000);
                        }}
                        finishOutput();
                        return;
                    }}
                    buffer += decoder.decode(value, {{ stream: true }});
                    const lines = buffer.split('\\n');
                    buffer = lines.pop();
                    for (const raw of lines) {{
                        if (!raw.startsWith('data: ')) continue;
                        try {{
                            const msg = JSON.parse(raw.slice(6));
                            if (msg.type === 'output' || msg.type === 'start') {{
                                appendOutput(msg.line, classifyLine(msg.line));
                            }} else if (msg.type === 'done') {{
                                success = msg.status === 'success';
                                const cls = success ? 'line-success' : 'line-error';
                                appendOutput(success ? 'Rebuild complete.' : 'Rebuild failed.', cls);
                            }} else if (msg.type === 'error') {{
                                appendOutput(msg.line, 'line-error');
                            }}
                        }} catch(e) {{}}
                    }}
                    read();
                }});
            }}
            read();
        }})
        .catch(err => {{
            appendOutput('Connection error: ' + err.message, 'line-error');
            finishOutput();
        }});
}}

// ── Batch import ───────────────────────────────────────

let selectedBatchSeries = null;

function openBatchImport() {{
    const modal = document.getElementById('batch-modal');
    modal.classList.add('open');
    selectedBatchSeries = null;
    document.getElementById('batch-start-btn').disabled = true;
    document.getElementById('batch-start-btn').textContent = 'Import Selected Series';

    // Fetch series data from server
    const listEl = document.getElementById('batch-series-list');
    listEl.innerHTML = '<div style="text-align:center;color:#999;padding:20px;">Loading series information...</div>';

    fetch('/api/list-series')
        .then(r => r.json())
        .then(data => {{
            if (data.error) {{
                listEl.innerHTML = '<div style="color:#b71c1c;padding:12px;">Error: ' + escapeHtml(data.error) + '</div>';
                return;
            }}
            renderSeriesList(data.series);
        }})
        .catch(err => {{
            listEl.innerHTML = '<div style="color:#b71c1c;padding:12px;">Error loading series: ' + escapeHtml(err.message) + '<br><br>Make sure the server is running.</div>';
        }});
}}

function renderSeriesList(seriesData) {{
    const listEl = document.getElementById('batch-series-list');
    listEl.innerHTML = '';

    // Add "All series" option
    const totalNew = seriesData.reduce((sum, s) => sum + s.unprocessed, 0);
    const totalAll = seriesData.reduce((sum, s) => sum + s.total, 0);

    const allRow = document.createElement('div');
    allRow.className = 'batch-series-row';
    allRow.dataset.series = 'all';
    allRow.innerHTML = '<span class="series-name">All</span>' +
        '<span class="series-detail">' + totalAll + ' volumes total</span>' +
        (totalNew > 0
            ? '<span class="series-badge">' + totalNew + ' new</span>'
            : '<span class="series-badge done">all imported</span>');
    allRow.onclick = () => selectBatchSeries('all', allRow);
    listEl.appendChild(allRow);

    // Sort series descending (most recent first)
    const sorted = seriesData.slice().sort((a, b) => b.series.localeCompare(a.series));

    for (const s of sorted) {{
        const row = document.createElement('div');
        row.className = 'batch-series-row';
        row.dataset.series = s.series;
        row.innerHTML = '<span class="series-name">' + escapeHtml(s.series) + '</span>' +
            '<span class="series-detail">' + s.total + ' volume' + (s.total !== 1 ? 's' : '') + '</span>' +
            (s.unprocessed > 0
                ? '<span class="series-badge">' + s.unprocessed + ' new</span>'
                : '<span class="series-badge done">all imported</span>');
        row.onclick = () => selectBatchSeries(s.series, row);
        listEl.appendChild(row);
    }}
}}

function selectBatchSeries(series, rowEl) {{
    selectedBatchSeries = series;
    document.querySelectorAll('.batch-series-row').forEach(r => r.classList.remove('selected'));
    rowEl.classList.add('selected');
    const btn = document.getElementById('batch-start-btn');
    btn.disabled = false;
    btn.textContent = series === 'all' ? 'Import All New Volumes' : 'Import Series ' + series;
}}

function closeBatchImport() {{
    document.getElementById('batch-modal').classList.remove('open');
    selectedBatchSeries = null;
}}

function runBatchImport() {{
    if (!selectedBatchSeries) return;
    const series = selectedBatchSeries;
    closeBatchImport();

    const label = series === 'all' ? 'all new volumes' : 'series ' + series;
    openOutputPanel('Importing ' + label + '...');
    document.getElementById('btn-import').disabled = true;

    fetch('/api/import-volume', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ series: series }})
    }})
        .then(response => {{
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            let success = false;

            function read() {{
                reader.read().then(({{ done, value }}) => {{
                    if (done) {{
                        document.getElementById('btn-import').disabled = false;
                        if (success) {{
                            appendOutput('Reloading page...', 'line-success');
                            setTimeout(() => window.location.reload(), 1500);
                        }}
                        finishOutput();
                        return;
                    }}
                    buffer += decoder.decode(value, {{ stream: true }});
                    const lines = buffer.split('\\n');
                    buffer = lines.pop();
                    for (const raw of lines) {{
                        if (!raw.startsWith('data: ')) continue;
                        try {{
                            const msg = JSON.parse(raw.slice(6));
                            if (msg.type === 'output' || msg.type === 'start') {{
                                appendOutput(msg.line, classifyLine(msg.line));
                            }} else if (msg.type === 'done') {{
                                success = msg.status === 'success';
                                const cls = success ? 'line-success' : 'line-error';
                                appendOutput(success ? 'Import complete.' : 'Import failed.', cls);
                            }} else if (msg.type === 'error') {{
                                appendOutput(msg.line, 'line-error');
                            }}
                        }} catch(e) {{}}
                    }}
                    read();
                }});
            }}
            read();
        }})
        .catch(err => {{
            appendOutput('Connection error: ' + err.message, 'line-error');
            document.getElementById('btn-import').disabled = false;
            finishOutput();
        }});
}}

function runOpenTaxonomyReview() {{
    openOutputPanel('Building taxonomy review tool...');
    document.getElementById('btn-taxonomy').disabled = true;
    fetch('/api/rebuild-taxonomy-review', {{ method: 'POST' }})
        .then(response => {{
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            let success = false;

            function read() {{
                reader.read().then(({{ done, value }}) => {{
                    if (done) {{
                        document.getElementById('btn-taxonomy').disabled = false;
                        if (success) {{
                            appendOutput('Opening taxonomy review...', 'line-success');
                            window.open('/taxonomy-review.html', '_blank');
                        }}
                        finishOutput();
                        return;
                    }}
                    buffer += decoder.decode(value, {{ stream: true }});
                    const lines = buffer.split('\\n');
                    buffer = lines.pop();
                    for (const raw of lines) {{
                        if (!raw.startsWith('data: ')) continue;
                        try {{
                            const msg = JSON.parse(raw.slice(6));
                            if (msg.type === 'output' || msg.type === 'start') {{
                                appendOutput(msg.line, classifyLine(msg.line));
                            }} else if (msg.type === 'done') {{
                                success = msg.status === 'success';
                                const cls = success ? 'line-success' : 'line-error';
                                appendOutput(success ? 'Build complete.' : 'Build failed.', cls);
                            }} else if (msg.type === 'error') {{
                                appendOutput(msg.line, 'line-error');
                            }}
                        }} catch(e) {{}}
                    }}
                    read();
                }});
            }}
            read();
        }})
        .catch(err => {{
            document.getElementById('btn-taxonomy').disabled = false;
            appendOutput('Connection error: ' + err.message, 'line-error');
            finishOutput();
        }});
}}

// ── Global exclusions (shared with taxonomy review) ─────
async function loadTaxonomyDecisions() {{
    try {{
        const resp = await fetch('/api/load-taxonomy-decisions');
        if (resp.ok) {{
            const tdata = await resp.json();
            if (tdata.exclusions) {{
                Object.assign(globalExclusions, tdata.exclusions);
            }}
            if (tdata.global_rejections) {{
                Object.assign(globalRejections, tdata.global_rejections);
            }}
            // Load taxonomy merge decisions (global, not per-volume)
            if (tdata.merge_decisions) {{
                for (const [sourceRef, d] of Object.entries(tdata.merge_decisions)) {{
                    // Don't overwrite per-volume merge decisions
                    if (!mergeDecisions[sourceRef]) {{
                        mergeDecisions[sourceRef] = {{
                            targetRef: d.targetRef,
                            targetName: d.targetName,
                            fromTaxonomy: true,
                        }};
                    }}
                }}
            }}
        }}
    }} catch(e) {{
        console.log('Could not load taxonomy decisions:', e.message);
    }}
}}

async function saveGlobalExclusion(ref, name) {{
    globalExclusions[ref] = {{ name: name }};
    try {{
        // Load current taxonomy decisions, update exclusions, save back
        const loadResp = await fetch('/api/load-taxonomy-decisions');
        if (loadResp.ok) {{
            const current = await loadResp.json();
            current.exclusions = globalExclusions;
            current.saved = new Date().toISOString();
            await fetch('/api/save-taxonomy-decisions', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify(current),
            }});
        }}
    }} catch(e) {{
        console.log('Could not save exclusion:', e.message);
    }}
}}

async function removeGlobalExclusion(ref) {{
    delete globalExclusions[ref];
    try {{
        const loadResp = await fetch('/api/load-taxonomy-decisions');
        if (loadResp.ok) {{
            const current = await loadResp.json();
            current.exclusions = globalExclusions;
            current.saved = new Date().toISOString();
            await fetch('/api/save-taxonomy-decisions', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify(current),
            }});
        }}
    }} catch(e) {{
        console.log('Could not save exclusion removal:', e.message);
    }}
}}

function excludeTermGlobal(ref, name) {{
    saveGlobalExclusion(ref, name);
    renderSidebar();
    if (selectedId) {{
        if (currentView === 'terms') selectTerm(selectedId);
        else selectDoc(selectedId);
    }}
    showSyncStatus('Excluded: ' + name, false);
    setTimeout(() => showSyncStatus('', false), 2000);
}}

function restoreTermGlobal(ref) {{
    const name = globalExclusions[ref] ? globalExclusions[ref].name : ref;
    removeGlobalExclusion(ref);
    renderSidebar();
    if (selectedId) {{
        if (currentView === 'terms') selectTerm(selectedId);
        else selectDoc(selectedId);
    }}
    showSyncStatus('Restored: ' + name, false);
    setTimeout(() => showSyncStatus('', false), 2000);
}}

// ── Global rejections (reject a term across all volumes) ─
async function saveGlobalRejection(ref, name) {{
    globalRejections[ref] = {{ name: name }};
    try {{
        const loadResp = await fetch('/api/load-taxonomy-decisions');
        if (loadResp.ok) {{
            const current = await loadResp.json();
            current.global_rejections = globalRejections;
            current.saved = new Date().toISOString();
            await fetch('/api/save-taxonomy-decisions', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify(current),
            }});
        }}
    }} catch(e) {{
        console.log('Could not save global rejection:', e.message);
    }}
}}

async function removeGlobalRejection(ref) {{
    delete globalRejections[ref];
    try {{
        const loadResp = await fetch('/api/load-taxonomy-decisions');
        if (loadResp.ok) {{
            const current = await loadResp.json();
            current.global_rejections = globalRejections;
            current.saved = new Date().toISOString();
            await fetch('/api/save-taxonomy-decisions', {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify(current),
            }});
        }}
    }} catch(e) {{
        console.log('Could not save global rejection removal:', e.message);
    }}
}}

function rejectTermGlobal(ref, name) {{
    saveGlobalRejection(ref, name);
    // Auto-reject all matches for this ref in the current volume
    if (data) {{
        const term = data.by_term[ref];
        if (term) {{
            for (const docId of Object.keys(term.documents)) {{
                for (const occ of term.documents[docId]) {{
                    const key = matchKey(docId, ref, occ.position);
                    rejections[key] = true;
                }}
            }}
            saveRejections();
        }}
    }}
    renderSidebar();
    if (selectedId) {{
        if (currentView === 'terms') selectTerm(selectedId);
        else selectDoc(selectedId);
    }}
    showSyncStatus('Globally rejected: ' + name, false);
    setTimeout(() => showSyncStatus('', false), 2000);
}}

function unRejectTermGlobal(ref) {{
    const name = globalRejections[ref] ? globalRejections[ref].name : ref;
    removeGlobalRejection(ref);
    // Remove auto-rejections for this ref in the current volume
    if (data) {{
        const term = data.by_term[ref];
        if (term) {{
            for (const docId of Object.keys(term.documents)) {{
                for (const occ of term.documents[docId]) {{
                    const key = matchKey(docId, ref, occ.position);
                    delete rejections[key];
                }}
            }}
            saveRejections();
        }}
    }}
    renderSidebar();
    if (selectedId) {{
        if (currentView === 'terms') selectTerm(selectedId);
        else selectDoc(selectedId);
    }}
    showSyncStatus('Restored global rejection: ' + name, false);
    setTimeout(() => showSyncStatus('', false), 2000);
}}

function applyGlobalRejectionsToVolume() {{
    // Called after volume data loads — auto-reject all matches for globally rejected refs
    if (!data) return;
    let count = 0;
    for (const ref of Object.keys(globalRejections)) {{
        const term = data.by_term[ref];
        if (!term) continue;
        for (const docId of Object.keys(term.documents)) {{
            for (const occ of term.documents[docId]) {{
                const key = matchKey(docId, ref, occ.position);
                if (!rejections[key]) {{
                    rejections[key] = true;
                    count++;
                }}
            }}
        }}
    }}
    if (count > 0) {{
        saveRejections();
        showSyncStatus('Auto-rejected ' + count + ' matches from global rejections', false);
        setTimeout(() => showSyncStatus('', false), 3000);
    }}
}}

// ── Initialize ──────────────────────────────────────────
loadTaxonomyDecisions();
populateVolumeSelector();

// Auto-load first volume
if (manifest.length > 0) {{
    const firstVol = manifest[0].volume_id;
    document.getElementById('vol-picker-btn').textContent = firstVol;
    document.querySelectorAll('.vol-item').forEach(el => el.classList.toggle('active', el.dataset.volid === firstVol));
    loadVolume(firstVol);
}}
</script>
</body>
</html>"""

    return html


def main():
    manifest = build_manifest()
    if not manifest:
        print("ERROR: No string_match_results_*.json files found in data/documents/*/")
        print("Run annotate_documents.py first for each volume.")
        sys.exit(1)

    print(f"Found {len(manifest)} volumes:")
    for v in manifest:
        print(f"  {v['volume_id']}: {v['total_matches']:,} matches, {v['total_documents']} docs")

    taxonomy_index = build_taxonomy_index()
    print(f"\nTaxonomy index: {len(taxonomy_index)} active subjects for merge targets")

    html = build_html(manifest, taxonomy_index)

    with open(OUTPUT_HTML, "w") as f:
        f.write(html)

    size_kb = os.path.getsize(OUTPUT_HTML) / 1024
    print(f"\nWrote {OUTPUT_HTML} ({size_kb:.0f} KB)")
    print(f"  Volumes: {len(manifest)}")


if __name__ == "__main__":
    main()
