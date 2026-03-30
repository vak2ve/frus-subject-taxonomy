#!/usr/bin/env python3
"""
build_metadata_mockup.py — Build HSG-style mockup HTML from metadata pipeline data.

Produces metadata-hsg-mockup.html — a history.state.gov-styled subject browse
interface built from TEI header metadata.

Usage:
    python3 build_metadata_mockup.py
"""

import json
import os
import re
import unicodedata
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPT_DIR.parent
DATA_DIR = PIPELINE_DIR / "data"
SIDEBAR_DATA_PATH = DATA_DIR / "mockup_sidebar_data.json"
SUBJECT_DATA_PATH = DATA_DIR / "mockup_subject_data.json"
MOCKUP_DIR = DATA_DIR / "mockup"
OUTPUT_HTML = PIPELINE_DIR / "metadata-hsg-mockup.html"


def slugify(name):
    text = unicodedata.normalize("NFKD", name)
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text


def main():
    print("=== Building Metadata HSG Mockup ===")

    if not SIDEBAR_DATA_PATH.exists():
        print(f"ERROR: {SIDEBAR_DATA_PATH} not found. Run generate_metadata_mockup_data.py first.")
        return

    with open(SIDEBAR_DATA_PATH) as f:
        sidebar_data = json.load(f)

    # Build list of category slugs for lazy loading
    cat_slugs = {}
    for cat_label in sidebar_data:
        cat_slugs[cat_label] = slugify(cat_label)

    sidebar_json = json.dumps(sidebar_data, separators=(",", ":"), ensure_ascii=False)
    cat_slugs_json = json.dumps(cat_slugs, separators=(",", ":"), ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FRUS Subject Taxonomy — Metadata Mockup</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'Source Sans Pro', -apple-system, BlinkMacSystemFont, sans-serif; background: #fff; color: #1b1b1b; }}

/* Header matching HSG style */
.site-header {{ background: #1a3c5e; color: white; padding: 12px 24px; }}
.site-header h1 {{ font-size: 20px; font-weight: 400; }}
.site-header h1 span {{ font-weight: 700; }}
.site-header .subtitle {{ font-size: 13px; opacity: 0.8; margin-top: 2px; }}

.breadcrumb {{ padding: 10px 24px; background: #f5f5f5; border-bottom: 1px solid #ddd; font-size: 13px; color: #555; }}
.breadcrumb a {{ color: #1a3c5e; text-decoration: none; }}
.breadcrumb a:hover {{ text-decoration: underline; }}

.layout {{ display: flex; min-height: calc(100vh - 120px); }}

/* Sidebar */
.sidebar {{ width: 300px; min-width: 300px; background: #f8f9fa; border-right: 1px solid #e0e0e0; overflow-y: auto; }}
.sidebar-cat {{ padding: 10px 16px; background: #1a3c5e; color: white; font-weight: 700; font-size: 13px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; }}
.sidebar-cat:hover {{ background: #2a4c6e; }}
.sidebar-cat .arrow {{ font-size: 10px; transition: transform 0.2s; }}
.sidebar-cat .arrow.open {{ transform: rotate(90deg); }}
.sidebar-subcat {{ padding: 8px 16px 8px 24px; font-size: 13px; cursor: pointer; border-bottom: 1px solid #eee; display: flex; justify-content: space-between; }}
.sidebar-subcat:hover {{ background: #e8eef4; }}
.sidebar-subcat.active {{ background: #d0dde9; font-weight: 600; }}
.sidebar-subcat .count {{ color: #71767a; font-size: 12px; }}
.sidebar-subject {{ padding: 6px 16px 6px 36px; font-size: 12px; cursor: pointer; border-bottom: 1px solid #f0f0f0; display: flex; justify-content: space-between; }}
.sidebar-subject:hover {{ background: #eef2f6; }}
.sidebar-subject.active {{ background: #d0dde9; font-weight: 600; color: #1a3c5e; }}
.sidebar-subject .count {{ color: #71767a; font-size: 11px; }}

/* Main content */
.main {{ flex: 1; padding: 24px; overflow-y: auto; }}
.main h2 {{ font-size: 22px; color: #1a3c5e; margin-bottom: 8px; }}
.main .meta {{ font-size: 14px; color: #71767a; margin-bottom: 20px; }}
.main .badge {{ display: inline-block; background: #e6f4f4; color: #0d7377; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600; margin-left: 8px; }}

.vol-section {{ margin-bottom: 24px; }}
.vol-header {{ font-size: 15px; font-weight: 700; color: #1a3c5e; margin-bottom: 8px; padding-bottom: 4px; border-bottom: 1px solid #ddd; cursor: pointer; }}
.vol-header:hover {{ color: #2a5c8e; }}
.vol-header .vol-count {{ font-weight: 400; color: #71767a; font-size: 13px; }}
.doc-list {{ margin-left: 0; }}
.doc-item {{ padding: 6px 0; border-bottom: 1px solid #f0f0f0; font-size: 13px; }}
.doc-item a {{ color: #1a3c5e; text-decoration: none; }}
.doc-item a:hover {{ text-decoration: underline; }}
.doc-item .doc-date {{ color: #71767a; font-size: 12px; margin-left: 8px; }}

.merged-note {{ background: #f3e5f5; padding: 8px 12px; border-radius: 4px; font-size: 13px; color: #7b1fa2; margin-bottom: 16px; }}

.loading {{ text-align: center; padding: 40px; color: #999; }}

/* No subject selected */
.welcome {{ max-width: 600px; margin: 60px auto; text-align: center; }}
.welcome h2 {{ color: #1a3c5e; margin-bottom: 12px; }}
.welcome p {{ color: #71767a; font-size: 15px; line-height: 1.6; }}
</style>
</head>
<body>
<div class="site-header">
    <h1>Office of the Historian — <span>FRUS Subject Taxonomy</span></h1>
    <div class="subtitle">Metadata Pipeline Mockup — TEI Header Source</div>
</div>
<div class="breadcrumb">
    <a href="#">historicaldocuments</a> › <a href="#">subjects</a> › <span id="breadcrumb-trail">Browse</span>
</div>

<div class="layout">
    <div class="sidebar" id="sidebar"></div>
    <div class="main" id="main">
        <div class="welcome">
            <h2>FRUS Subject Taxonomy</h2>
            <p>Browse subjects by category in the sidebar. Click a subject to see all documents where it appears, with links to the original sources.</p>
        </div>
    </div>
</div>

<script>
const SIDEBAR = {sidebar_json};
const CAT_SLUGS = {cat_slugs_json};
const subjectCache = {{}};
let currentRef = null;

function renderSidebar() {{
    const el = document.getElementById('sidebar');
    let html = '';
    Object.entries(SIDEBAR).sort((a,b) => a[0].localeCompare(b[0])).forEach(([cat, subcats]) => {{
        const totalSubjects = subcats.reduce((s, sc) => s + sc.subjects.length, 0);
        html += '<div class="sidebar-cat" onclick="toggleCat(this)">' +
            '<span>' + cat + ' (' + totalSubjects + ')</span>' +
            '<span class="arrow">▶</span></div>';
        html += '<div style="display:none">';
        subcats.forEach(sc => {{
            html += '<div class="sidebar-subcat" onclick="toggleSubcat(this)">' +
                '<span>' + sc.name + '</span><span class="count">' + sc.subjects.length + '</span></div>';
            html += '<div style="display:none">';
            sc.subjects.forEach(s => {{
                html += '<div class="sidebar-subject" data-ref="' + s.ref + '" onclick="showSubject(\\'' + s.ref + '\\',\\'' + cat + '\\')">' +
                    '<span>' + s.name + '</span><span class="count">' + s.count.toLocaleString() + '</span></div>';
            }});
            html += '</div>';
        }});
        html += '</div>';
    }});
    el.innerHTML = html;
}}

function toggleCat(el) {{
    const content = el.nextElementSibling;
    const arrow = el.querySelector('.arrow');
    if (content.style.display === 'none') {{
        content.style.display = 'block';
        arrow.classList.add('open');
    }} else {{
        content.style.display = 'none';
        arrow.classList.remove('open');
    }}
}}

function toggleSubcat(el) {{
    const content = el.nextElementSibling;
    if (content.style.display === 'none') {{
        content.style.display = 'block';
        el.classList.add('active');
    }} else {{
        content.style.display = 'none';
        el.classList.remove('active');
    }}
}}

async function showSubject(ref, catLabel) {{
    currentRef = ref;

    // Highlight in sidebar
    document.querySelectorAll('.sidebar-subject').forEach(el => el.classList.remove('active'));
    const active = document.querySelector('.sidebar-subject[data-ref="' + ref + '"]');
    if (active) active.classList.add('active');

    const main = document.getElementById('main');
    main.innerHTML = '<div class="loading">Loading...</div>';

    // Load subject data (lazy per category)
    let data = subjectCache[ref];
    if (!data) {{
        const slug = CAT_SLUGS[catLabel];
        if (slug && !subjectCache['__cat_' + slug]) {{
            try {{
                const resp = await fetch('data/mockup/' + slug + '.json');
                const catData = await resp.json();
                Object.assign(subjectCache, catData);
                subjectCache['__cat_' + slug] = true;
                data = subjectCache[ref];
            }} catch(e) {{
                main.innerHTML = '<p style="color:red">Error loading data: ' + e.message + '</p>';
                return;
            }}
        }}
        data = subjectCache[ref];
    }}

    if (!data) {{
        main.innerHTML = '<p style="color:#999">No detailed data available for this subject.</p>';
        return;
    }}

    document.getElementById('breadcrumb-trail').textContent = catLabel + ' › ' + data.name;

    let html = '<h2>' + data.name + '<span class="badge">TEI Header Source</span></h2>';
    html += '<div class="meta">' + data.count.toLocaleString() + ' annotations across ' +
        Object.keys(data.volumes || {{}}).length + ' volumes</div>';

    if (data.merged_names && data.merged_names.length > 0) {{
        html += '<div class="merged-note">Also includes: ' + data.merged_names.join(', ') + '</div>';
    }}

    const vols = Object.entries(data.volumes || {{}}).sort((a,b) => a[0].localeCompare(b[0]));
    vols.forEach(([volId, vol]) => {{
        html += '<div class="vol-section">';
        html += '<div class="vol-header" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\\'none\\'?\\'block\\':\\'none\\'">' +
            '<a href="' + vol.url + '" target="_blank">' + (vol.title || volId) + '</a>' +
            ' <span class="vol-count">(' + vol.docs.length + ' document' + (vol.docs.length !== 1 ? 's' : '') + ')</span></div>';
        html += '<div class="doc-list">';
        vol.docs.forEach(doc => {{
            html += '<div class="doc-item">';
            html += '<a href="' + doc.url + '" target="_blank">' + (doc.title || doc.id) + '</a>';
            if (doc.date) html += '<span class="doc-date">' + doc.date + '</span>';
            html += '</div>';
        }});
        html += '</div></div>';
    }});

    main.innerHTML = html;
}}

renderSidebar();
</script>
</body>
</html>"""

    with open(OUTPUT_HTML, "w") as f:
        f.write(html)

    print(f"\nOutput: {OUTPUT_HTML}")
    print(f"  {len(html):,} bytes")
    print(f"  {len(sidebar_data)} categories, {len(cat_slugs)} slugs")


if __name__ == "__main__":
    main()
