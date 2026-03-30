#!/usr/bin/env python3
"""
build_metadata_taxonomy_review.py — Build taxonomy review HTML from metadata pipeline.

Produces metadata-taxonomy-review.html with the same interactive UI for reviewing
LCSH mappings, categories, exclusions, and merges — but sourced from the metadata
pipeline's taxonomy and data.

Usage:
    python3 build_metadata_taxonomy_review.py
"""

import json
import os
import sys
from pathlib import Path

from lxml import etree

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
REPO_ROOT = PIPELINE_DIR.parent
CONFIG_DIR = REPO_ROOT / "config"
TAXONOMY_PATH = PIPELINE_DIR / "subject-taxonomy-metadata.xml"
DOC_APPEARANCES_PATH = PIPELINE_DIR / "data" / "document_appearances.json"
OUTPUT_HTML = PIPELINE_DIR / "metadata-taxonomy-review.html"

sys.path.insert(0, str(REPO_ROOT / "scripts"))


def parse_taxonomy(path):
    """Parse taxonomy XML into nested dict structure for embedding."""
    tree = etree.parse(str(path))
    root = tree.getroot()
    taxonomy = {
        "generated": root.get("generated", ""),
        "total_subjects": root.get("total-subjects", "0"),
        "pipeline": root.get("pipeline", "metadata"),
        "categories": [],
    }

    for cat in root.findall("category"):
        cat_data = {
            "label": cat.get("label", ""),
            "total_annotations": int(cat.get("total-annotations", 0)),
            "total_subjects": int(cat.get("total-subjects", 0)),
            "subcategories": [],
        }
        for sub in cat.findall("subcategory"):
            sub_data = {
                "label": sub.get("label", ""),
                "total_annotations": int(sub.get("total-annotations", 0)),
                "total_subjects": int(sub.get("total-subjects", 0)),
                "subjects": [],
            }
            for subj in sub.findall("subject"):
                name_el = subj.find("name")
                s = {
                    "ref": subj.get("ref", ""),
                    "name": name_el.text.strip() if name_el is not None and name_el.text else "",
                    "type": subj.get("type", "topic"),
                    "count": int(subj.get("count", 0)),
                    "volumes": int(subj.get("volumes", 0)),
                    "lcsh_uri": subj.get("lcsh-uri", ""),
                    "lcsh_match": subj.get("lcsh-match", ""),
                }
                ai = subj.find("appearsIn")
                if ai is not None and ai.text:
                    s["appears_in"] = ai.text
                sub_data["subjects"].append(s)
            cat_data["subcategories"].append(sub_data)
        taxonomy["categories"].append(cat_data)

    return taxonomy


def load_doc_appearances(path):
    """Load document appearances, compacted to volume counts only."""
    if not path.exists():
        return {}
    with open(path) as f:
        full = json.load(f)
    # Compact: ref -> {vol_id: doc_count}
    compact = {}
    for ref, vols in full.items():
        compact[ref] = {vol: len(docs) for vol, docs in vols.items()}
    return compact


def load_mapping():
    """Load LCSH mapping from config."""
    mapping_path = CONFIG_DIR / "lcsh_mapping.json"
    if not mapping_path.exists():
        return {}
    with open(mapping_path) as f:
        return json.load(f)


def build_html(taxonomy, doc_appearances, mapping):
    """Generate the taxonomy review HTML."""
    taxonomy_json = json.dumps(taxonomy, separators=(",", ":"), ensure_ascii=False)
    appearances_json = json.dumps(doc_appearances, separators=(",", ":"), ensure_ascii=False)

    # Build ref->name and ref->lcsh label lookups
    ref_names = {}
    for cat in taxonomy["categories"]:
        for sub in cat["subcategories"]:
            for s in sub["subjects"]:
                ref_names[s["ref"]] = s["name"]
    ref_names_json = json.dumps(ref_names, separators=(",", ":"), ensure_ascii=False)

    # LCSH labels from mapping
    lcsh_labels = {}
    for ref, data in mapping.items():
        if data.get("lcsh_label"):
            lcsh_labels[ref] = data["lcsh_label"]
    lcsh_labels_json = json.dumps(lcsh_labels, separators=(",", ":"), ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Metadata Taxonomy Review</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Source Sans Pro', -apple-system, BlinkMacSystemFont, sans-serif; background: #fff; color: #1b1b1b; }}
.header {{ background: #fff; padding: 12px 24px; position: sticky; top: 0; z-index: 100; display: flex; align-items: center; gap: 16px; flex-wrap: wrap; border-bottom: 3px solid #0d7377; }}
.header h1 {{ font-size: 18px; font-weight: 700; color: #0d7377; }}
.header .badge {{ background: #e6f4f4; color: #0d7377; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; }}
.header .stats {{ font-size: 13px; color: #555; }}
.header .stats b {{ color: #0d7377; }}

.layout {{ display: flex; height: calc(100vh - 60px); }}
.sidebar {{ width: 280px; min-width: 280px; background: #fff; border-right: 1px solid #e0e0e0; display: flex; flex-direction: column; overflow: hidden; }}
.sidebar-search {{ padding: 8px; border-bottom: 1px solid #eee; }}
.sidebar-search input {{ width: 100%; padding: 6px 8px; border: 1px solid #ccc; border-radius: 4px; font-size: 12px; }}
.sidebar-list {{ flex: 1; overflow-y: auto; }}
.sidebar-cat {{ padding: 8px 10px; background: #f8f9fa; font-weight: 700; font-size: 11px; color: #0d7377; text-transform: uppercase; cursor: pointer; display: flex; justify-content: space-between; }}
.sidebar-cat:hover {{ background: #e6f4f4; }}
.sidebar-subcat {{ padding: 4px 10px 4px 20px; font-size: 12px; color: #555; cursor: pointer; display: flex; justify-content: space-between; }}
.sidebar-subcat:hover {{ background: #f0fafa; }}
.sidebar-subcat.active {{ background: #e6f4f4; font-weight: 600; }}
.sidebar-item {{ padding: 4px 10px 4px 30px; font-size: 12px; cursor: pointer; display: flex; justify-content: space-between; border-left: 3px solid transparent; }}
.sidebar-item:hover {{ background: #f0fafa; }}
.sidebar-item.active {{ background: #e6f4f4; border-left-color: #0d7377; }}
.sidebar-item.excluded {{ opacity: 0.4; text-decoration: line-through; }}
.sidebar-item .badge {{ background: #0d7377; color: white; border-radius: 10px; padding: 1px 6px; font-size: 10px; }}

.main {{ flex: 1; overflow-y: auto; padding: 20px; }}
.main h2 {{ font-size: 18px; color: #0d7377; margin-bottom: 4px; }}
.main .subtitle {{ color: #71767a; font-size: 13px; margin-bottom: 16px; }}

.detail-card {{ background: #f8f9fa; border-radius: 8px; padding: 16px; margin-bottom: 16px; }}
.detail-card h3 {{ font-size: 14px; color: #0d7377; margin-bottom: 8px; }}
.detail-row {{ display: flex; gap: 12px; margin-bottom: 6px; font-size: 13px; }}
.detail-label {{ font-weight: 600; color: #555; min-width: 120px; }}
.detail-value {{ color: #1b1b1b; }}

.lcsh-dot {{ display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 4px; }}
.lcsh-dot.exact {{ background: #1b5e20; }}
.lcsh-dot.good_close {{ background: #e65100; }}
.lcsh-dot.bad_close {{ background: #b71c1c; }}
.lcsh-dot.lcsh_rejected {{ background: #999; }}
.lcsh-dot.none {{ background: #ccc; }}

.btn {{ border: none; border-radius: 4px; padding: 6px 14px; font-size: 13px; font-weight: 600; cursor: pointer; margin-right: 6px; }}
.btn-exclude {{ background: #f9e0e0; color: #b71c1c; }}
.btn-exclude:hover {{ background: #f1c0c0; }}
.btn-merge {{ background: #f3e5f5; color: #7b1fa2; }}
.btn-merge:hover {{ background: #e1bee7; }}
.btn-recategorize {{ background: #fff3e0; color: #e65100; }}
.btn-recategorize:hover {{ background: #ffe0b2; }}
.btn-restore {{ background: #e0f2e9; color: #1b5e20; }}

.vol-list {{ display: flex; flex-wrap: wrap; gap: 4px; margin-top: 8px; }}
.vol-tag {{ background: #e6f4f4; color: #0d7377; padding: 2px 6px; border-radius: 3px; font-size: 11px; }}

.save-bar {{ position: fixed; bottom: 0; left: 0; right: 0; background: #fff; border-top: 2px solid #0d7377; padding: 8px 24px; display: flex; align-items: center; gap: 12px; z-index: 100; }}
.save-btn {{ background: #0d7377; color: white; border: none; border-radius: 4px; padding: 8px 20px; font-weight: 600; cursor: pointer; }}
.save-btn:hover {{ background: #0a5c5f; }}

.merge-modal {{ display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.5); z-index: 300; justify-content: center; align-items: center; }}
.merge-modal.open {{ display: flex; }}
.merge-modal-content {{ background: white; border-radius: 8px; padding: 24px; width: 500px; max-height: 80vh; overflow-y: auto; }}
.merge-modal h3 {{ color: #0d7377; margin-bottom: 16px; }}
.merge-search {{ width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; margin-bottom: 12px; font-size: 13px; }}
.merge-option {{ padding: 8px 12px; cursor: pointer; border-bottom: 1px solid #eee; font-size: 13px; }}
.merge-option:hover {{ background: #f0fafa; }}
.merge-cancel {{ background: #eee; color: #333; border: none; padding: 8px 16px; border-radius: 4px; cursor: pointer; margin-top: 12px; }}
</style>
</head>
<body>
<div class="header">
    <h1>Metadata Taxonomy Review</h1>
    <span class="badge">TEI Header Source</span>
    <span class="stats" id="stats"></span>
</div>

<div class="layout">
    <div class="sidebar">
        <div class="sidebar-search">
            <input type="text" id="search" placeholder="Search subjects..." oninput="filterSidebar(this.value)">
        </div>
        <div class="sidebar-list" id="sidebar-list"></div>
    </div>
    <div class="main" id="main">
        <h2>Select a subject to review</h2>
        <p class="subtitle">Browse the taxonomy in the sidebar. Click a subject to see details, set exclusions, or merge.</p>
    </div>
</div>

<div class="save-bar">
    <button class="save-btn" onclick="saveDecisions()">Save Decisions</button>
    <span id="save-status"></span>
    <span style="margin-left:auto;font-size:12px;color:#71767a" id="decision-counts"></span>
</div>

<div class="merge-modal" id="merge-modal">
    <div class="merge-modal-content">
        <h3>Merge into...</h3>
        <input type="text" class="merge-search" id="merge-search" placeholder="Search for merge target..." oninput="filterMergeTargets(this.value)">
        <div id="merge-targets"></div>
        <button class="merge-cancel" onclick="closeMergeModal()">Cancel</button>
    </div>
</div>

<script>
const TAXONOMY = {taxonomy_json};
const DOC_APPEARANCES = {appearances_json};
const REF_NAMES = {ref_names_json};
const LCSH_LABELS = {lcsh_labels_json};

let exclusions = {{}};
let mergeDecisions = {{}};
let categoryOverrides = {{}};
let currentSubject = null;
let mergeSourceRef = null;

// Build flat subject list for search
const ALL_SUBJECTS = [];
TAXONOMY.categories.forEach(cat => {{
    cat.subcategories.forEach(sub => {{
        sub.subjects.forEach(s => {{
            ALL_SUBJECTS.push({{ ...s, category: cat.label, subcategory: sub.label }});
        }});
    }});
}});

// Load saved state
(async function() {{
    try {{
        const resp = await fetch('/api/metadata/load-taxonomy-decisions');
        if (resp.ok) {{
            const state = await resp.json();
            exclusions = state.exclusions || {{}};
            mergeDecisions = state.merge_decisions || {{}};
            categoryOverrides = state.category_overrides || {{}};
        }}
    }} catch(e) {{}}
    renderSidebar();
    updateStats();
}})();

function updateStats() {{
    const exclCount = Object.keys(exclusions).length;
    const mergeCount = Object.keys(mergeDecisions).length;
    const overrideCount = Object.keys(categoryOverrides).length;
    document.getElementById('stats').innerHTML =
        '<b>' + TAXONOMY.total_subjects + '</b> subjects' +
        (exclCount ? ', <span style="color:#b71c1c"><b>' + exclCount + '</b> excluded</span>' : '') +
        (mergeCount ? ', <span style="color:#7b1fa2"><b>' + mergeCount + '</b> merged</span>' : '') +
        (overrideCount ? ', <span style="color:#e65100"><b>' + overrideCount + '</b> recategorized</span>' : '');
    document.getElementById('decision-counts').textContent =
        [exclCount && exclCount + ' exclusion(s)',
         mergeCount && mergeCount + ' merge(s)',
         overrideCount && overrideCount + ' override(s)']
        .filter(Boolean).join(', ') || 'No decisions';
}}

function renderSidebar(filter) {{
    const list = document.getElementById('sidebar-list');
    const fl = (filter || '').toLowerCase();
    let html = '';

    TAXONOMY.categories.forEach(cat => {{
        let catHtml = '';
        let catMatch = false;

        cat.subcategories.forEach(sub => {{
            let subHtml = '';
            sub.subjects.forEach(s => {{
                if (fl && !s.name.toLowerCase().includes(fl) && !s.ref.includes(fl)) return;
                catMatch = true;
                const isExcluded = exclusions[s.ref];
                const isMerged = mergeDecisions[s.ref];
                subHtml += '<div class="sidebar-item' +
                    (isExcluded ? ' excluded' : '') +
                    (currentSubject === s.ref ? ' active' : '') +
                    '" onclick="showSubject(\\'' + s.ref + '\\')">' +
                    '<span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' +
                    s.name + '</span>' +
                    '<span class="badge">' + s.count + '</span></div>';
            }});

            if (subHtml) {{
                catHtml += '<div class="sidebar-subcat" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\\'none\\'?\\'block\\':\\'none\\'">' +
                    '<span>' + sub.label + '</span>' +
                    '<span style="font-size:10px;color:#999">' + sub.total_subjects + '</span></div>' +
                    '<div>' + subHtml + '</div>';
            }}
        }});

        if (catHtml || !fl) {{
            html += '<div class="sidebar-cat" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\\'none\\'?\\'block\\':\\'none\\'">' +
                '<span>' + cat.label + '</span>' +
                '<span style="font-size:10px;color:#999">' + cat.total_subjects + '</span></div>' +
                '<div>' + catHtml + '</div>';
        }}
    }});

    list.innerHTML = html;
}}

function filterSidebar(val) {{
    renderSidebar(val);
}}

function showSubject(ref) {{
    currentSubject = ref;
    const s = ALL_SUBJECTS.find(x => x.ref === ref);
    if (!s) return;

    renderSidebar(document.getElementById('search').value);

    const apps = DOC_APPEARANCES[ref] || {{}};
    const volList = Object.entries(apps).sort((a,b) => b[1] - a[1]);
    const totalDocs = volList.reduce((sum, [,c]) => sum + c, 0);
    const lcshLabel = LCSH_LABELS[ref] || '';

    const main = document.getElementById('main');
    let html = '<h2>' + s.name + '</h2>';
    html += '<p class="subtitle">' + s.category + ' › ' + s.subcategory + '</p>';

    // Action buttons
    html += '<div style="margin-bottom:16px">';
    if (exclusions[ref]) {{
        html += '<button class="btn btn-restore" onclick="toggleExclusion(\\'' + ref + '\\')">Restore</button>';
    }} else {{
        html += '<button class="btn btn-exclude" onclick="toggleExclusion(\\'' + ref + '\\')">Exclude</button>';
    }}
    html += '<button class="btn btn-merge" onclick="openMergeModal(\\'' + ref + '\\')">Merge Into...</button>';
    html += '</div>';

    // Details
    html += '<div class="detail-card"><h3>Details</h3>';
    html += '<div class="detail-row"><span class="detail-label">Ref</span><span class="detail-value" style="font-family:monospace;font-size:12px">' + ref + '</span></div>';
    html += '<div class="detail-row"><span class="detail-label">Type</span><span class="detail-value">' + s.type + '</span></div>';
    html += '<div class="detail-row"><span class="detail-label">Annotations</span><span class="detail-value">' + s.count + '</span></div>';
    html += '<div class="detail-row"><span class="detail-label">Volumes</span><span class="detail-value">' + s.volumes + '</span></div>';
    if (s.lcsh_uri) {{
        html += '<div class="detail-row"><span class="detail-label">LCSH</span><span class="detail-value"><span class="lcsh-dot ' + s.lcsh_match + '"></span> ' +
            '<a href="' + s.lcsh_uri + '" target="_blank">' + (lcshLabel || s.lcsh_uri) + '</a> (' + s.lcsh_match + ')</span></div>';
    }}
    html += '</div>';

    // Merge info
    if (mergeDecisions[ref]) {{
        const target = mergeDecisions[ref];
        html += '<div class="detail-card" style="border-left:4px solid #7b1fa2"><h3 style="color:#7b1fa2">Merged</h3>';
        html += '<p style="font-size:13px">Merged into: <b>' + target.targetName + '</b> (' + target.targetRef + ')</p>';
        html += '<button class="btn btn-restore" onclick="undoMerge(\\'' + ref + '\\')">Undo Merge</button>';
        html += '</div>';
    }}

    // Volume appearances
    if (volList.length > 0) {{
        html += '<div class="detail-card"><h3>Appears In (' + totalDocs + ' documents across ' + volList.length + ' volumes)</h3>';
        html += '<div class="vol-list">';
        volList.forEach(([vol, count]) => {{
            html += '<span class="vol-tag">' + vol + ' (' + count + ')</span>';
        }});
        html += '</div></div>';
    }}

    main.innerHTML = html;
}}

function toggleExclusion(ref) {{
    if (exclusions[ref]) delete exclusions[ref];
    else exclusions[ref] = true;
    updateStats();
    showSubject(ref);
}}

function openMergeModal(ref) {{
    mergeSourceRef = ref;
    document.getElementById('merge-modal').classList.add('open');
    document.getElementById('merge-search').value = '';
    filterMergeTargets('');
}}

function closeMergeModal() {{
    document.getElementById('merge-modal').classList.remove('open');
    mergeSourceRef = null;
}}

function filterMergeTargets(filter) {{
    const fl = filter.toLowerCase();
    const targets = document.getElementById('merge-targets');
    const matches = ALL_SUBJECTS
        .filter(s => s.ref !== mergeSourceRef && (!fl || s.name.toLowerCase().includes(fl)))
        .slice(0, 30);
    targets.innerHTML = matches.map(s =>
        '<div class="merge-option" onclick="doMerge(\\'' + s.ref + '\\')">' +
        s.name + ' <span style="color:#71767a;font-size:11px">(' + s.category + ' › ' + s.subcategory + ')</span></div>'
    ).join('');
}}

function doMerge(targetRef) {{
    const target = ALL_SUBJECTS.find(s => s.ref === targetRef);
    mergeDecisions[mergeSourceRef] = {{
        targetRef: targetRef,
        targetName: target ? target.name : targetRef,
    }};
    closeMergeModal();
    updateStats();
    showSubject(mergeSourceRef);
}}

function undoMerge(ref) {{
    delete mergeDecisions[ref];
    updateStats();
    showSubject(ref);
}}

async function saveDecisions() {{
    const status = document.getElementById('save-status');
    status.textContent = 'Saving...';
    try {{
        const resp = await fetch('/api/metadata/save-taxonomy-decisions', {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify({{
                exclusions,
                merge_decisions: mergeDecisions,
                category_overrides: categoryOverrides,
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
    print("=== Building Metadata Taxonomy Review ===")

    if not TAXONOMY_PATH.exists():
        print(f"ERROR: Taxonomy not found at {TAXONOMY_PATH}")
        print("Run build_metadata_taxonomy.py first")
        sys.exit(1)

    taxonomy = parse_taxonomy(TAXONOMY_PATH)
    doc_appearances = load_doc_appearances(DOC_APPEARANCES_PATH)
    mapping = load_mapping()

    print(f"  {taxonomy['total_subjects']} subjects in {len(taxonomy['categories'])} categories")
    print(f"  {len(doc_appearances)} subjects with doc appearances")

    html = build_html(taxonomy, doc_appearances, mapping)

    with open(OUTPUT_HTML, "w") as f:
        f.write(html)

    print(f"\nOutput: {OUTPUT_HTML}")
    print(f"  {len(html):,} bytes")


if __name__ == "__main__":
    main()
