#!/usr/bin/env python3
"""
build_metadata_review.py — Build annotation review HTML from TEI header metadata.

Adapts the existing build_annotation_review.py to work with metadata pipeline data.
Produces metadata-annotation-review.html with the same interactive UI but sourced
from TEI header metadata instead of string-match results.

Usage:
    python3 build_metadata_review.py
"""

import json
import os
import re
import sys
from pathlib import Path

from lxml import etree

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PIPELINE_DIR.parent
DATA_DIR = PIPELINE_DIR / "data"
TAXONOMY_PATH = PIPELINE_DIR / "subject-taxonomy-metadata.xml"
CONFIG_DIR = REPO_ROOT / "config"
OUTPUT_HTML = PIPELINE_DIR / "metadata-annotation-review.html"

SERIES_RE = re.compile(r"^frus(\d{4}(?:-\d{2,4})?)")

# Add the main scripts dir to path so we can import shared logic
sys.path.insert(0, str(REPO_ROOT / "scripts"))


def build_taxonomy_index():
    """Build compact taxonomy index from metadata taxonomy for merge targets."""
    if not TAXONOMY_PATH.exists():
        print(f"  WARNING: {TAXONOMY_PATH} not found")
        return []

    tree = etree.parse(str(TAXONOMY_PATH))
    root = tree.getroot()
    index = []
    for cat in root.findall("category"):
        cat_label = cat.get("label", "")
        for sub in cat.findall("subcategory"):
            sub_label = sub.get("label", "")
            for subj in sub.findall("subject"):
                name_el = subj.find("name")
                if name_el is None or not name_el.text:
                    continue
                index.append({
                    "r": subj.get("ref", ""),
                    "n": name_el.text.strip(),
                    "c": cat_label,
                    "s": sub_label,
                })
    return index


def build_manifest():
    """Build volume manifest from metadata pipeline data."""
    manifest_path = DATA_DIR / "volume_manifest.json"
    if manifest_path.exists():
        with open(manifest_path) as f:
            manifest = json.load(f)
        # Adjust filename paths to be relative to pipeline dir for fetch()
        for entry in manifest:
            # Keep the path as-is since it's already relative to pipeline dir
            pass
        return manifest

    # Fallback: discover files
    manifest = []
    for vol_dir in sorted(DATA_DIR.iterdir()):
        if not vol_dir.is_dir():
            continue
        results_files = list(vol_dir.glob("metadata_results_*.json"))
        if not results_files:
            continue
        with open(results_files[0]) as f:
            data = json.load(f)
        meta = data["metadata"]
        vol_id = meta["volume_id"]
        m = SERIES_RE.match(vol_id)
        series = m.group(1) if m else "other"
        manifest.append({
            "volume_id": vol_id,
            "series": series,
            "filename": f"data/{vol_id}/metadata_results_{vol_id}.json",
            "total_matches": meta.get("total_annotations", meta.get("total_matches", 0)),
            "unique_terms_matched": meta.get("unique_terms_found", meta.get("unique_terms_matched", 0)),
            "total_terms_searched": meta.get("unique_terms_found", 0),
            "total_documents": meta["total_documents"],
            "documents_with_matches": meta.get("documents_with_annotations", meta.get("documents_with_matches", 0)),
            "terms_not_matched": 0,
            "generated": meta.get("generated", ""),
        })
    return manifest


def build_ref_names():
    """Build ref -> name lookup from metadata taxonomy."""
    ref_names = {}
    if TAXONOMY_PATH.exists():
        tree = etree.parse(str(TAXONOMY_PATH))
        for subj in tree.getroot().iter("subject"):
            name_el = subj.find("name")
            if name_el is not None and name_el.text:
                ref_names[subj.get("ref", "")] = name_el.text.strip()
    return ref_names


def build_lcsh_labels():
    """Build ref -> LCSH label lookup from config."""
    labels = {}
    mapping_path = CONFIG_DIR / "lcsh_mapping.json"
    if mapping_path.exists():
        with open(mapping_path) as f:
            mapping = json.load(f)
        for ref, data in mapping.items():
            if data.get("lcsh_label"):
                labels[ref] = data["lcsh_label"]
    return labels


def build_html(manifest, taxonomy_index, ref_names, ref_lcsh_labels):
    """
    Generate the annotation review HTML.
    Imports and delegates to the existing build_annotation_review.py's build_html,
    with modifications for metadata pipeline context.
    """
    # Try to import and reuse the existing builder
    try:
        # Save and restore cwd since the existing script changes it
        orig_cwd = os.getcwd()
        import build_annotation_review as existing_builder
        os.chdir(orig_cwd)

        # Generate HTML using existing builder
        html = existing_builder.build_html(manifest, taxonomy_index, ref_names, ref_lcsh_labels)

        # Patch the HTML to customize for metadata pipeline:
        # 1. Update title
        html = html.replace(
            "<title>String Match Annotation Review</title>",
            "<title>Metadata Annotation Review</title>"
        )
        # 2. Update header
        html = html.replace(
            "String Match Annotation Review",
            "Metadata Annotation Review"
        )
        # 3. Update API endpoints to use metadata pipeline state
        html = html.replace(
            "'/api/save-decisions'",
            "'/api/metadata/save-decisions'"
        )
        html = html.replace(
            '"/api/save-decisions"',
            '"/api/metadata/save-decisions"'
        )
        html = html.replace(
            "'/api/load-decisions/'",
            "'/api/metadata/load-decisions/'"
        )
        html = html.replace(
            '"/api/load-decisions/"',
            '"/api/metadata/load-decisions/"'
        )
        # 4. Update data paths for fetch() - the manifest filenames are already
        #    relative to metadata-pipeline/, but fetch() in the HTML resolves
        #    relative to the served URL root. We need them relative to metadata-pipeline/.
        # (Already handled by manifest filenames)

        # 5. Add a metadata source indicator
        html = html.replace(
            "</head>",
            """<style>
.header::after { content: "TEI Header Source"; background: #e6f4f4; color: #0d7377; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; margin-left: 8px; }
</style>
</head>"""
        )

        return html

    except Exception as e:
        print(f"  Note: Could not import existing builder ({e}), generating standalone version")
        return _build_standalone_html(manifest, taxonomy_index, ref_names, ref_lcsh_labels)


def _build_standalone_html(manifest, taxonomy_index, ref_names, ref_lcsh_labels):
    """
    Fallback: generate a minimal but functional annotation review HTML.
    This is used when the existing builder can't be imported.
    """
    manifest_json = json.dumps(manifest, ensure_ascii=False)
    taxonomy_index_json = json.dumps(taxonomy_index, separators=(",", ":"), ensure_ascii=False)
    ref_names_json = json.dumps(ref_names, separators=(",", ":"), ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Metadata Annotation Review</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Source Sans Pro', -apple-system, BlinkMacSystemFont, sans-serif; background: #fff; color: #1b1b1b; }}
.header {{ background: #fff; padding: 12px 24px; position: sticky; top: 0; z-index: 100; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; border-bottom: 3px solid #0d7377; }}
.header h1 {{ font-size: 18px; font-weight: 700; color: #0d7377; }}
.header .badge {{ background: #e6f4f4; color: #0d7377; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
.header .stats {{ font-size: 13px; color: #555; }}
.header .stats b {{ color: #0d7377; }}

select {{ padding: 6px 12px; border: 1px solid #0d7377; border-radius: 4px; font-size: 13px; color: #0d7377; background: #fff; cursor: pointer; }}
select:hover {{ background: #e6f4f4; }}

.tabs {{ background: #f8f9fa; display: flex; gap: 0; border-bottom: 1px solid #ddd; }}
.tab {{ padding: 10px 20px; color: #555; cursor: pointer; font-size: 14px; font-weight: 600; border-bottom: 3px solid transparent; }}
.tab:hover {{ color: #0d7377; background: #eef7f7; }}
.tab.active {{ color: #0d7377; border-bottom-color: #0d7377; }}

.layout {{ display: flex; height: calc(100vh - 100px); }}
.sidebar {{ width: 220px; min-width: 220px; background: #fff; border-right: 1px solid #e0e0e0; overflow-y: auto; }}
.sidebar-item {{ padding: 8px 12px; border-bottom: 1px solid #f0f0f0; cursor: pointer; font-size: 12px; display: flex; justify-content: space-between; align-items: center; border-left: 3px solid transparent; }}
.sidebar-item:hover {{ background: #f0fafa; }}
.sidebar-item.active {{ background: #e6f4f4; border-left-color: #0d7377; }}
.sidebar-item .badge {{ background: #0d7377; color: white; border-radius: 10px; padding: 1px 6px; font-size: 10px; }}

.main {{ flex: 1; overflow-y: auto; padding: 16px; }}
.main h2 {{ font-size: 18px; color: #0d7377; margin-bottom: 12px; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th {{ text-align: left; padding: 8px 10px; background: #f8f9fa; font-weight: 600; font-size: 12px; color: #555; border-bottom: 2px solid #0d7377; position: sticky; top: 0; }}
td {{ padding: 6px 10px; border-bottom: 1px solid #eee; }}
tr:hover {{ background: #e6f4f4; }}
tr.rejected {{ opacity: 0.4; }}
tr.rejected td {{ text-decoration: line-through; text-decoration-color: #b71c1c; }}

.lcsh-dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 4px; }}
.lcsh-dot.exact {{ background: #1b5e20; }}
.lcsh-dot.good_close {{ background: #e65100; }}
.lcsh-dot.bad_close {{ background: #b71c1c; }}
.lcsh-dot.lcsh_rejected {{ background: #999; }}
.lcsh-dot.none {{ background: #ccc; }}

.btn {{ border: none; border-radius: 3px; padding: 3px 8px; font-size: 11px; font-weight: 600; cursor: pointer; }}
.btn-reject {{ background: #f9e0e0; color: #b71c1c; }}
.btn-reject:hover {{ background: #f1c0c0; }}
.btn-accept {{ background: #e0f2e9; color: #1b5e20; }}
.btn-merge {{ background: #f3e5f5; color: #7b1fa2; }}
.btn-merge:hover {{ background: #e1bee7; }}

.save-bar {{ position: fixed; bottom: 0; left: 0; right: 0; background: #fff; border-top: 2px solid #0d7377; padding: 8px 24px; display: flex; align-items: center; gap: 12px; z-index: 100; }}
.save-btn {{ background: #0d7377; color: white; border: none; border-radius: 4px; padding: 8px 20px; font-weight: 600; cursor: pointer; }}
.save-btn:hover {{ background: #0a5c5f; }}

#loading {{ text-align: center; padding: 60px; color: #999; font-size: 16px; }}
</style>
</head>
<body>
<div class="header">
    <h1>Metadata Annotation Review</h1>
    <span class="badge">TEI Header Source</span>
    <select id="vol-select" onchange="loadVolume(this.value)">
        <option value="">Select a volume...</option>
    </select>
    <span class="stats" id="stats"></span>
</div>

<div class="tabs">
    <div class="tab active" onclick="switchTab('documents')">Documents</div>
    <div class="tab" onclick="switchTab('terms')">Terms</div>
    <div class="tab" onclick="switchTab('statistics')">Statistics</div>
</div>

<div id="loading">Select a volume to begin review</div>

<div class="layout" id="layout" style="display:none">
    <div class="sidebar" id="sidebar"></div>
    <div class="main" id="main"></div>
</div>

<div class="save-bar">
    <button class="save-btn" onclick="saveDecisions()">Save Decisions</button>
    <span id="save-status"></span>
    <span style="margin-left:auto;font-size:12px;color:#71767a" id="decision-counts"></span>
</div>

<script type="application/json" id="volume-manifest">{manifest_json}</script>
<script type="application/json" id="taxonomy-index">{taxonomy_index_json}</script>
<script type="application/json" id="ref-names">{ref_names_json}</script>

<script>
const MANIFEST = JSON.parse(document.getElementById('volume-manifest').textContent);
const TAXONOMY_INDEX = JSON.parse(document.getElementById('taxonomy-index').textContent);
const REF_NAMES = JSON.parse(document.getElementById('ref-names').textContent);

let currentVolume = null;
let currentData = null;
let rejections = {{}};
let currentTab = 'documents';

// Populate volume selector grouped by series
(function() {{
    const sel = document.getElementById('vol-select');
    const bySeries = {{}};
    MANIFEST.forEach(v => {{
        (bySeries[v.series] = bySeries[v.series] || []).push(v);
    }});
    Object.keys(bySeries).sort().reverse().forEach(series => {{
        const group = document.createElement('optgroup');
        group.label = series;
        bySeries[series].forEach(v => {{
            const opt = document.createElement('option');
            opt.value = v.volume_id;
            opt.textContent = v.volume_id + ' (' + v.total_matches + ' annotations)';
            group.appendChild(opt);
        }});
        sel.appendChild(group);
    }});
}})();

async function loadVolume(volId) {{
    if (!volId) return;
    currentVolume = volId;
    document.getElementById('loading').textContent = 'Loading ' + volId + '...';
    document.getElementById('loading').style.display = 'block';
    document.getElementById('layout').style.display = 'none';

    const entry = MANIFEST.find(v => v.volume_id === volId);
    if (!entry) return;

    try {{
        // Scan volume TEI headers on-demand via server API
        const resp = await fetch('/api/metadata/scan-volume/' + volId);
        currentData = await resp.json();
        rejections = {{}};

        // Try to load saved decisions
        try {{
            const dr = await fetch('/api/metadata/load-decisions/' + volId);
            if (dr.ok) {{
                const saved = await dr.json();
                (saved.rejections || []).forEach(r => rejections[r.key] = true);
            }}
        }} catch(e) {{}}

        document.getElementById('loading').style.display = 'none';
        document.getElementById('layout').style.display = 'flex';
        updateStats();
        renderTab();
    }} catch(e) {{
        document.getElementById('loading').textContent = 'Error loading: ' + e.message;
    }}
}}

function updateStats() {{
    const meta = currentData.metadata;
    const rejCount = Object.keys(rejections).length;
    document.getElementById('stats').innerHTML =
        '<b>' + meta.total_documents + '</b> docs, ' +
        '<b>' + meta.total_annotations + '</b> annotations, ' +
        '<b>' + meta.unique_terms_found + '</b> unique terms' +
        (rejCount ? ', <span style="color:#b71c1c"><b>' + rejCount + '</b> rejected</span>' : '');
    document.getElementById('decision-counts').textContent =
        rejCount + ' rejection(s)';
}}

function switchTab(tab) {{
    currentTab = tab;
    document.querySelectorAll('.tab').forEach((t, i) => {{
        t.classList.toggle('active', ['documents','terms','statistics'][i] === tab);
    }});
    renderTab();
}}

function renderTab() {{
    if (!currentData) return;
    if (currentTab === 'documents') renderDocuments();
    else if (currentTab === 'terms') renderTerms();
    else renderStatistics();
}}

function renderDocuments() {{
    const sidebar = document.getElementById('sidebar');
    const main = document.getElementById('main');
    const docs = currentData.by_document;
    const docIds = Object.keys(docs).sort((a,b) => {{
        const na = parseInt(a.replace('d','')) || 0;
        const nb = parseInt(b.replace('d','')) || 0;
        return na - nb;
    }});

    sidebar.innerHTML = docIds.map(id => {{
        const d = docs[id];
        return '<div class="sidebar-item" onclick="showDoc(\\'' + id + '\\')">' +
            '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' +
            id + '. ' + (d.title || 'Untitled').substring(0, 40) + '</span>' +
            '<span class="badge">' + d.match_count + '</span></div>';
    }}).join('');

    if (docIds.length > 0) showDoc(docIds[0]);
}}

function showDoc(docId) {{
    document.querySelectorAll('.sidebar-item').forEach(el => el.classList.remove('active'));
    // Find and activate
    const items = document.querySelectorAll('.sidebar-item');
    items.forEach(el => {{
        if (el.textContent.startsWith(docId + '.')) el.classList.add('active');
    }});

    const doc = currentData.by_document[docId];
    const main = document.getElementById('main');
    let html = '<h2>' + docId + '. ' + (doc.title || 'Untitled') + '</h2>';
    html += '<p style="color:#71767a;font-size:13px;margin-bottom:16px">' + (doc.date || 'No date') +
        ' — ' + doc.match_count + ' annotation(s)' +
        (doc.has_header ? '' : ' <span style="color:#b71c1c">(no TEI header)</span>') + '</p>';

    if (doc.matches.length === 0) {{
        html += '<p style="color:#999">No annotations in TEI header for this document.</p>';
    }} else {{
        html += '<table><thead><tr><th>Term</th><th>Category</th><th>LCSH</th><th>Actions</th></tr></thead><tbody>';
        doc.matches.forEach((m, i) => {{
            const key = docId + ':' + m.ref + ':' + i;
            const isRejected = rejections[key];
            const lcshClass = m.lcsh_match || 'none';
            html += '<tr class="' + (isRejected ? 'rejected' : '') + '">' +
                '<td style="font-weight:600">' + m.term + '</td>' +
                '<td style="font-size:12px;color:#71767a">' + m.category + ' › ' + m.subcategory + '</td>' +
                '<td><span class="lcsh-dot ' + lcshClass + '"></span>' + (m.lcsh_match || 'none') + '</td>' +
                '<td>' +
                (isRejected
                    ? '<button class="btn btn-accept" onclick="toggleReject(\\'' + key + '\\')">Restore</button>'
                    : '<button class="btn btn-reject" onclick="toggleReject(\\'' + key + '\\')">Reject</button>') +
                '</td></tr>';
        }});
        html += '</tbody></table>';
    }}
    main.innerHTML = html;
}}

function renderTerms() {{
    const terms = currentData.by_term;
    const refs = Object.keys(terms).sort((a,b) => terms[b].total_occurrences - terms[a].total_occurrences);
    const sidebar = document.getElementById('sidebar');
    const main = document.getElementById('main');

    sidebar.innerHTML = refs.map(ref => {{
        const t = terms[ref];
        return '<div class="sidebar-item" onclick="showTerm(\\'' + ref + '\\')">' +
            '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' +
            t.term + '</span>' +
            '<span class="badge">' + t.total_occurrences + '</span></div>';
    }}).join('');

    if (refs.length > 0) showTerm(refs[0]);
}}

function showTerm(ref) {{
    const t = currentData.by_term[ref];
    const main = document.getElementById('main');
    let html = '<h2>' + t.term + '</h2>';
    html += '<p style="color:#71767a;font-size:13px;margin-bottom:16px">' +
        t.category + ' › ' + t.subcategory +
        ' — ' + t.total_occurrences + ' occurrences in ' + t.documents.length + ' documents</p>';
    html += '<table><thead><tr><th>Document</th></tr></thead><tbody>';
    t.documents.forEach(docId => {{
        const doc = currentData.by_document[docId] || {{}};
        html += '<tr><td>' + docId + '. ' + (doc.title || '') + ' <span style="color:#71767a;font-size:12px">(' + (doc.date || '') + ')</span></td></tr>';
    }});
    html += '</tbody></table>';
    main.innerHTML = html;
}}

function renderStatistics() {{
    const meta = currentData.metadata;
    const terms = currentData.by_term;
    const sidebar = document.getElementById('sidebar');
    const main = document.getElementById('main');
    sidebar.innerHTML = '';

    // Category breakdown
    const catCounts = {{}};
    const lcshCounts = {{}};
    Object.values(terms).forEach(t => {{
        catCounts[t.category] = (catCounts[t.category] || 0) + t.total_occurrences;
        const lm = t.lcsh_match || 'none';
        lcshCounts[lm] = (lcshCounts[lm] || 0) + 1;
    }});

    let html = '<h2>Statistics for ' + meta.volume_id + '</h2>';
    html += '<p style="margin-bottom:16px">Source: TEI header metadata</p>';
    html += '<h3 style="color:#0d7377;margin:16px 0 8px">Coverage</h3>';
    html += '<table><tbody>';
    html += '<tr><td>Total documents</td><td><b>' + meta.total_documents + '</b></td></tr>';
    html += '<tr><td>Documents with headers</td><td><b>' + meta.documents_with_headers + '</b> (' + meta.header_coverage + '%)</td></tr>';
    html += '<tr><td>Documents with annotations</td><td><b>' + meta.documents_with_annotations + '</b> (' + meta.annotation_coverage + '%)</td></tr>';
    html += '<tr><td>Total annotations</td><td><b>' + meta.total_annotations + '</b></td></tr>';
    html += '<tr><td>Unique terms</td><td><b>' + meta.unique_terms_found + '</b></td></tr>';
    html += '</tbody></table>';

    html += '<h3 style="color:#0d7377;margin:16px 0 8px">By Category</h3>';
    html += '<table><thead><tr><th>Category</th><th>Annotations</th></tr></thead><tbody>';
    Object.entries(catCounts).sort((a,b) => b[1]-a[1]).forEach(([cat, count]) => {{
        html += '<tr><td>' + cat + '</td><td>' + count + '</td></tr>';
    }});
    html += '</tbody></table>';

    html += '<h3 style="color:#0d7377;margin:16px 0 8px">LCSH Match Quality</h3>';
    html += '<table><thead><tr><th>Quality</th><th>Terms</th></tr></thead><tbody>';
    Object.entries(lcshCounts).sort((a,b) => b[1]-a[1]).forEach(([q, count]) => {{
        html += '<tr><td><span class="lcsh-dot ' + q + '"></span> ' + q + '</td><td>' + count + '</td></tr>';
    }});
    html += '</tbody></table>';

    main.innerHTML = html;
}}

function toggleReject(key) {{
    if (rejections[key]) delete rejections[key];
    else rejections[key] = true;
    updateStats();
    renderTab();
}}

async function saveDecisions() {{
    const status = document.getElementById('save-status');
    status.textContent = 'Saving...';
    const rejList = Object.keys(rejections).map(k => {{
        const [docId, ref, pos] = k.split(':');
        return {{ key: k, docId, ref, position: parseInt(pos) }};
    }});

    try {{
        const resp = await fetch('/api/metadata/save-decisions', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{
                volume_id: currentVolume,
                rejections: rejList,
            }})
        }});
        if (resp.ok) {{
            status.textContent = 'Saved!';
            setTimeout(() => status.textContent = '', 2000);
        }} else {{
            status.textContent = 'Error saving';
        }}
    }} catch(e) {{
        status.textContent = 'Save failed: ' + e.message;
    }}
}}
</script>
</body>
</html>"""
    return html


def main():
    print("=== Building Metadata Annotation Review ===")

    manifest = build_manifest()
    print(f"  Found {len(manifest)} volumes in manifest")

    taxonomy_index = build_taxonomy_index()
    print(f"  Taxonomy index: {len(taxonomy_index)} subjects")

    ref_names = build_ref_names()
    ref_lcsh_labels = build_lcsh_labels()

    html = build_html(manifest, taxonomy_index, ref_names, ref_lcsh_labels)

    with open(OUTPUT_HTML, "w") as f:
        f.write(html)

    print(f"\nOutput: {OUTPUT_HTML}")
    print(f"  {len(html):,} bytes")


if __name__ == "__main__":
    main()
