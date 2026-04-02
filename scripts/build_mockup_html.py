#!/usr/bin/env python3
"""Build hsg-subjects-mockup.html matching history.state.gov design system."""

import json
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

CAT_DESCRIPTIONS = {
    "Arms Control and Disarmament": "FRUS documents relating to arms control negotiations, disarmament initiatives, and weapons limitation treaties and agreements.",
    "Politico-Military Issues": "FRUS documents relating to political-military affairs, defense policy, intelligence, terrorism, and security cooperation.",
    "Foreign Economic Policy": "FRUS documents relating to trade, finance, energy, agriculture, economic sanctions, and international economic relations.",
    "Human Rights": "FRUS documents relating to human rights issues, refugees, asylum, political prisoners, and civil liberties.",
    "Global Issues": "FRUS documents relating to global issues including narcotics, environment, population, decolonization, immigration, and elections.",
    "Warfare": "FRUS documents relating to armed conflict, military operations, peace negotiations, and war-related diplomatic activities.",
    "Department of State": "FRUS documents relating to the internal operations, organization, and management of the U.S. Department of State.",
    "International Law": "FRUS documents relating to international law, the law of the sea, treaties, jurisdictional matters, and property claims.",
    "Information Programs": "FRUS documents relating to public diplomacy, propaganda, media, cultural exchange, and information programs.",
    "Science and Technology": "FRUS documents relating to scientific cooperation, technology transfer, atomic energy, and research and development.",
    "International Organizations": "FRUS documents relating to international organizations including the United Nations, OAS, IMF, and GATT.",
    "Bilateral Relations": "FRUS documents relating to bilateral and multilateral diplomatic relationships between the United States and foreign nations.",
}


def main():
    import re

    with open("../mockup_sidebar_data.json") as f:
        sidebar_data = json.load(f)

    sidebar_json = json.dumps(sidebar_data, separators=(",", ":"))

    # Load per-category data files (written by generate_mockup_data.py)
    mockup_dir = os.path.join("..", "data", "mockup")

    # Load category slug map
    cat_slugs_path = os.path.join(mockup_dir, "_cat_slugs.json")
    if os.path.exists(cat_slugs_path):
        with open(cat_slugs_path) as f:
            cat_slugs = json.load(f)
    else:
        # Fallback: generate slugs from sidebar data
        cat_slugs = {}
        for cat_name in sidebar_data:
            cat_slugs[cat_name] = re.sub(r"[^a-z0-9]+", "-", cat_name.lower()).strip("-")

    cat_slugs_json = json.dumps(cat_slugs, separators=(",", ":"))

    cat_names = list(sidebar_data.keys())
    default_cat = cat_names[0] if cat_names else "Arms Control and Disarmament"

    topics_items = []
    for cat in cat_names:
        active = " active" if cat == default_cat else ""
        topics_items.append(
            f'<li class="hsg-list-group-item{active}"><a href="#" onclick="switchCategory(\'{cat}\', this); return false;">{cat}</a></li>'
        )
    topics_li = "\n                            ".join(topics_items)

    default_subs = sidebar_data.get(default_cat, [])
    default_subjects = sum(len(s["subjects"]) for s in default_subs)
    default_docs = sum(s["docCount"] for s in default_subs)

    # Load only the default category's subject data to compute initial volume count
    default_slug = cat_slugs.get(default_cat, "")
    default_cat_path = os.path.join(mockup_dir, f"{default_slug}.json")
    default_subject_data = {}
    if os.path.exists(default_cat_path):
        with open(default_cat_path) as f:
            default_subject_data = json.load(f)
    default_vols = len(set(
        vol_id
        for s in default_subs
        for subj in s["subjects"]
        for vol_id in default_subject_data.get(subj["ref"], {}).get("volumes", {}).keys()
    ))
    default_desc = CAT_DESCRIPTIONS.get(default_cat, "")

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tags - Office of the Historian</title>
<style>
  @import url('https://fonts.googleapis.com/css?family=Source+Sans+Pro:400,300,700,400italic,700italic|Merriweather:400,300,400italic,700,700italic');

  /* === Reset & Base (matching HSG all.css) === */
  *, *::before, *::after {{ box-sizing: border-box; }}
  body {{
    font-family: "Source Sans Pro", "Helvetica Neue", Helvetica, Arial, sans-serif;
    font-size: 14px;
    line-height: 1.428571429;
    color: #333;
    background-color: #fff;
    margin: 0;
  }}
  a {{ color: #3376b7; text-decoration: none; }}
  a:hover, a:focus {{ color: #224d7b; text-decoration: underline; }}
  h1, h2, h3, h4 {{ font-family: "Merriweather", Georgia, serif; font-weight: 700; line-height: 1.1; color: #333; }}
  h1 {{ font-size: 28px; margin: 20px 0 10px; }}
  h2 {{ font-size: 24px; margin: 20px 0 10px; }}
  h3 {{ font-size: 18px; margin: 20px 0 10px; }}
  ul {{ list-style: none; padding-left: 0; }}

  /* === Container === */
  .container {{
    max-width: 1170px;
    margin: 0 auto;
    padding-left: 15px;
    padding-right: 15px;
  }}
  .container::after {{ content: ""; display: table; clear: both; }}
  .row {{ margin-left: -15px; margin-right: -15px; }}
  .row::after {{ content: ""; display: table; clear: both; }}

  /* === HSG Header === */
  .hsg-header {{ background-color: #fff; }}
  .hsg-header-inner {{ margin-top: 15px; background-color: #fff; border-bottom: none !important; }}
  .hsg-header-content {{ padding-top: 12px; padding-bottom: 12px; }}
  .hsg-header-content .row {{ display: flex; align-items: center; justify-content: space-between; }}
  .hsg-header-logo {{ }}
  .hsg-header-logo img {{ max-width: 400px; height: auto; }}
  .hsg-header-search {{ }}
  .hsg-header-search input {{
    border: 1px solid #ccc; border-radius: 4px; padding: 6px 12px;
    font-size: 14px; width: 200px; line-height: 1.428571429;
  }}
  .hsg-header-search input:focus {{ border-color: #66afe9; outline: 0; box-shadow: inset 0 1px 1px rgba(0,0,0,.075), 0 0 8px rgba(102,175,233,.6); }}

  /* === HSG Nav === */
  .hsg-nav {{ background: #205493; }}
  .hsg-nav-content {{ padding: 0; }}
  .hsg-nav ul {{ display: flex; list-style: none; padding: 0; margin: 0; }}
  .hsg-nav ul li a {{
    display: block; color: #fff; padding: 10px 15px;
    font-size: 14px; text-decoration: none; transition: background 0.2s;
  }}
  .hsg-nav ul li a:hover,
  .hsg-nav ul li a:focus {{ background-color: #4773aa; color: #fff; text-decoration: none; }}
  .hsg-nav ul li.active > a {{ background-color: #4773aa; }}

  /* === Breadcrumb (HSG style) === */
  .hsg-breadcrumb {{ padding: 12px 0; font-size: 14px; }}
  .hsg-breadcrumb__list {{ display: flex; flex-wrap: wrap; align-items: center; list-style: none; padding: 0; margin: 0; }}
  .hsg-breadcrumb__list-item {{ display: inline-flex; align-items: center; }}
  .hsg-breadcrumb__list-item:not(:last-child)::after {{
    content: "\\203A"; color: #71767a; margin: 0 6px; font-size: 16px;
  }}
  .hsg-breadcrumb__link {{ color: #205493; text-decoration: none; }}
  .hsg-breadcrumb__link:hover {{ color: #112e51; text-decoration: underline; }}
  .hsg-breadcrumb__link[aria-current="page"] {{ color: #212121; }}

  /* === HSG Main Content Area === */
  .hsg-main {{ background-color: #fff; }}
  .hsg-main-content {{ padding-bottom: 24px; padding-top: 12px; }}

  /* === 2/3 + 1/3 Grid (HSG uses float-based Bootstrap 3) === */
  .hsg-width-two-thirds,
  .hsg-width-sidebar {{
    position: relative; min-height: 1px;
    padding-left: 15px; padding-right: 15px;
    float: left;
  }}
  .hsg-width-two-thirds {{ width: 66.6666666667%; }}
  .hsg-width-sidebar {{ width: 33.3333333333%; }}

  /* === HSG Panel (sidebar panels) === */
  .hsg-panel {{
    margin-bottom: 20px; background-color: #fff;
    border: 1px solid #d6d7d9; border-radius: 4px;
  }}
  .hsg-panel-heading {{
    padding: 10px 15px; border-bottom: 1px solid transparent;
    border-top-left-radius: 3px; border-top-right-radius: 3px;
    font-family: "Source Sans Pro", "Helvetica", "Arial", sans-serif;
  }}
  .hsg-sidebar-title {{
    margin-top: 0; margin-bottom: 0; font-size: 1.5rem; line-height: 1.5;
    font-weight: 800; text-transform: uppercase; color: #981b1e;
    font-family: "Source Sans Pro", "Helvetica", "Arial", sans-serif;
  }}

  /* === HSG List Group (sidebar items) === */
  .hsg-list-group {{ padding-left: 0; margin-bottom: 0; }}
  .hsg-list-group-item {{
    position: relative; display: block; padding: 10px 15px;
    background-color: #fff; border-top: 1px solid #d6d7d9;
    border-bottom: none; border-left: none; border-right: none;
    width: 100%; text-align: left; color: #555;
  }}
  .hsg-list-group-item:first-child {{ border-top: none; }}
  .hsg-list-group-item a {{
    color: #3376b7; text-decoration: none; display: block;
  }}
  .hsg-list-group-item a:hover {{ color: #224d7b; text-decoration: underline; }}
  .hsg-list-group-item.active {{
    background-color: #3376b7; border-color: #3376b7;
  }}
  .hsg-list-group-item.active a {{ color: #fff; }}
  .hsg-list-group-item:hover:not(.active) {{ background-color: #f5f5f5; }}
  .hsg-list-group-item .badge {{
    float: right; background-color: #3376b7; color: #fff;
    padding: 3px 7px; border-radius: 10px; font-size: 12px;
  }}
  .hsg-list-group-item.active .badge {{ background-color: #fff; color: #3376b7; }}

  /* === HSG Footer (3 tiers) === */
  .hsg-footer-top {{
    background-color: #f1f1f1; padding: 18px 0;
  }}
  .hsg-footer-top .row {{ display: flex; }}
  .hsg-footer-list {{
    width: 25%; float: left; padding-left: 15px; padding-right: 15px;
    list-style: none;
  }}
  .hsg-footer-list li {{ margin-bottom: 2px; }}
  .hsg-footer-list li:first-child {{
    font-weight: 800; text-transform: uppercase; color: #981b1e;
    font-size: 14px; margin-bottom: 6px;
  }}
  .hsg-footer-list a {{ color: #3376b7; display: block; margin-top: 0; text-decoration: none; font-size: 14px; }}
  .hsg-footer-list a:hover {{ text-decoration: underline; }}
  .hsg-footer-bottom {{
    background-color: #d6d7d9; padding: 10px 0; font-size: 13px;
  }}
  .hsg-footer-bottom address {{ text-align: right; font-style: normal; margin: 0; color: #333; }}
  .hsg-footer-nether {{
    background-color: #aeb0b5; padding: 10px 0; font-size: 13px; color: #333;
  }}
  .hsg-footer-nether p {{ margin: 4px 0; }}

  /* === Tag Description === */
  .tag-description {{ font-size: 15px; color: #555; margin-bottom: 16px; }}

  /* === Stats Banner === */
  .stats-banner {{
    background: #f1f1f1; border: 1px solid #d6d7d9; border-radius: 4px;
    padding: 10px 15px; margin-bottom: 20px;
    display: flex; gap: 24px; font-size: 14px; color: #555;
  }}
  .stats-banner .stat-val {{ font-weight: 700; color: #112e51; font-size: 16px; }}

  /* === Subject View === */
  .subject-header {{ margin-bottom: 16px; }}
  .subject-header h2 {{ font-size: 22px; color: #333; margin-bottom: 4px; }}
  .subject-header .subcat-path {{ font-size: 14px; color: #71767a; margin-bottom: 6px; }}
  .subject-header .subcat-path a {{ color: #3376b7; cursor: pointer; }}
  .subject-header .subcat-path a:hover {{ text-decoration: underline; }}
  .subject-header .lcsh-info {{ font-size: 13px; color: #555; }}
  .subject-header .lcsh-info span {{ background: #e8f0fe; color: #205493; padding: 2px 8px; border-radius: 3px; }}
  .subject-header .merged-note {{ font-size: 13px; color: #71767a; font-style: italic; margin-top: 4px; }}

  .vol-group {{ margin-bottom: 16px; }}
  .vol-label {{
    font-weight: 700; font-size: 14px; color: #333;
    margin-bottom: 4px; padding-bottom: 4px;
    border-bottom: 1px solid #d6d7d9;
  }}
  .vol-label a {{ color: #3376b7; }}
  .vol-label a:hover {{ text-decoration: underline; }}

  .doc-table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  .doc-table td {{ padding: 6px 8px; vertical-align: top; border-bottom: 1px solid #f0f0f0; }}
  .doc-table tr:hover {{ background-color: #f5f5f5; }}
  .doc-table .doc-title a {{ color: #3376b7; }}
  .doc-table .doc-title a:hover {{ text-decoration: underline; }}
  .doc-table .doc-date {{ white-space: nowrap; color: #71767a; width: 160px; text-align: right; font-size: 13px; }}

  .welcome-view {{ text-align: center; padding: 48px 15px; color: #71767a; }}
  .welcome-view h2 {{ color: #333; font-size: 22px; margin-bottom: 8px; }}
  .welcome-view p {{ font-size: 15px; max-width: 500px; margin: 0 auto; }}

  /* === Subcategory Navigation (custom enhancement) === */
  .subcat-nav-list {{ padding: 0; margin: 0; }}
  .subcat-toggle {{
    color: #3376b7; font-size: 14px; font-weight: 600;
    display: flex; align-items: center; justify-content: space-between;
    padding: 8px 15px; cursor: pointer; user-select: none;
    border-top: 1px solid #d6d7d9; transition: background 0.15s;
  }}
  .subcat-toggle:first-child {{ border-top: none; }}
  .subcat-toggle:hover {{ background: #f5f5f5; }}
  .subcat-toggle .arrow {{ font-size: 10px; transition: transform 0.2s; color: #71767a; }}
  .subcat-toggle.open .arrow {{ transform: rotate(90deg); }}
  .subcat-toggle .scount {{ font-weight: 400; font-size: 12px; color: #71767a; margin-left: 4px; }}
  .subcat-subjects {{ display: none; padding: 0 0 4px 0; background: #fafafa; max-height: 160px; overflow-y: auto; }}
  .subcat-toggle.open + .subcat-subjects {{ display: block; }}
  .subcat-subjects li {{ }}
  .subcat-subjects a {{
    color: #555; font-size: 13px; display: block;
    padding: 4px 15px 4px 30px;
    transition: background 0.15s; text-decoration: none;
  }}
  .subcat-subjects a:hover {{ background: #e8f0fe; color: #3376b7; text-decoration: none; }}
  .subcat-subjects a.active {{ background: #3376b7; color: #fff; }}
  .subcat-subjects .sdcount {{ color: #aeb0b5; font-size: 11px; margin-left: 4px; }}
  .subcat-subjects a.active .sdcount {{ color: rgba(255,255,255,0.7); }}

  /* === Search === */
  .sidebar-search {{
    padding: 10px 15px; border-bottom: 1px solid #d6d7d9;
  }}
  .sidebar-search input {{
    width: 100%; padding: 6px 10px; border: 1px solid #ccc;
    border-radius: 4px; font-size: 13px; font-family: inherit;
  }}
  .sidebar-search input:focus {{ border-color: #66afe9; outline: 0; box-shadow: inset 0 1px 1px rgba(0,0,0,.075), 0 0 8px rgba(102,175,233,.6); }}
  #search-results {{ max-height: 400px; overflow-y: auto; }}
  #search-results a {{
    display: block; padding: 6px 15px; font-size: 13px;
    color: #555; text-decoration: none; border-bottom: 1px solid #f0f0f0;
  }}
  #search-results a:hover {{ background: #f5f5f5; color: #3376b7; text-decoration: none; }}
  #search-results .sr-meta {{ color: #aeb0b5; font-size: 11px; }}

  /* === Mockup Badge === */
  .mockup-badge {{
    position: fixed; top: 0; right: 0; z-index: 999;
    background: #981b1e; color: #fff; font-size: 11px;
    font-weight: 700; padding: 4px 16px; letter-spacing: 1px;
    text-transform: uppercase;
  }}

  /* === Rebuild Button === */
  .rebuild-btn {{
    position: fixed; top: 0; left: 0; z-index: 999;
    background: #205493; color: #fff; font-size: 11px;
    font-weight: 700; padding: 4px 16px; letter-spacing: 1px;
    text-transform: uppercase; border: none; cursor: pointer;
    transition: background 0.2s;
  }}
  .rebuild-btn:hover {{ background: #4773aa; }}
  .rebuild-btn:disabled {{ background: #aeb0b5; cursor: default; }}

  /* === Output Panel === */
  .output-overlay {{
    display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,0.5); z-index: 1000; justify-content: center; align-items: center;
  }}
  .output-overlay.visible {{ display: flex; }}
  .output-panel {{
    background: #1e1e1e; color: #d4d4d4; border-radius: 8px;
    width: 700px; max-width: 90vw; max-height: 80vh; display: flex; flex-direction: column;
    font-family: "SF Mono", "Monaco", "Menlo", "Courier New", monospace; font-size: 13px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.4);
  }}
  .output-header {{
    padding: 12px 16px; border-bottom: 1px solid #333; display: flex;
    justify-content: space-between; align-items: center;
  }}
  .output-header span {{ font-weight: 600; color: #fff; }}
  .output-close {{
    background: none; border: none; color: #888; font-size: 18px; cursor: pointer;
    padding: 0 4px;
  }}
  .output-close:hover {{ color: #fff; }}
  .output-body {{
    padding: 12px 16px; overflow-y: auto; flex: 1; white-space: pre-wrap;
    line-height: 1.5;
  }}
  .output-body .line-success {{ color: #4ec9b0; }}
  .output-body .line-error {{ color: #f44747; }}
  .output-body .line-heading {{ color: #569cd6; font-weight: 600; }}

  /* === Review Panel === */
  .review-panel {{
    background: #f8f4ff; border: 1px solid #d1c4e9; border-radius: 4px;
    margin-bottom: 16px;
  }}
  .review-toggle {{
    padding: 10px 15px; cursor: pointer; user-select: none;
    font-size: 14px; font-weight: 600; color: #4a148c;
    display: flex; align-items: center; gap: 8px;
  }}
  .review-toggle:hover {{ background: #ede7f6; }}
  .review-toggle .arrow {{ font-size: 10px; transition: transform 0.2s; }}
  .review-toggle.open .arrow {{ transform: rotate(90deg); }}
  .review-badge {{
    background: #7b1fa2; color: #fff; font-size: 11px; font-weight: 700;
    padding: 2px 8px; border-radius: 10px; margin-left: 4px;
  }}
  .review-content {{
    padding: 0 15px 15px; border-top: 1px solid #d1c4e9;
  }}
  .review-import-row {{
    display: flex; align-items: center; gap: 12px;
    padding: 8px 0; border-bottom: 1px solid #e8e0f0;
  }}
  .review-import-row label {{
    font-size: 13px; color: #333; min-width: 280px; font-weight: 600;
  }}
  .review-import-row input[type="file"] {{
    font-size: 13px; flex: 1;
  }}
  .review-status {{
    font-size: 12px; color: #71767a; white-space: nowrap;
  }}
  .review-status.loaded {{ color: #2e7d32; font-weight: 600; }}
  .review-buttons {{
    display: flex; gap: 8px; margin-top: 12px;
  }}
  .review-buttons button {{
    padding: 6px 16px; border-radius: 4px; font-size: 13px;
    font-weight: 600; cursor: pointer; border: 1px solid;
  }}
  .btn-apply {{
    background: #7b1fa2; color: #fff; border-color: #6a1b9a;
  }}
  .btn-apply:hover {{ background: #6a1b9a; }}
  .btn-apply:disabled {{ background: #bbb; border-color: #aaa; cursor: default; }}
  .btn-clear {{
    background: #fff; color: #666; border-color: #ccc;
  }}
  .btn-clear:hover {{ background: #f5f5f5; }}
  .review-summary {{
    margin-top: 12px; font-size: 13px; color: #333;
    padding: 8px 12px; background: #ede7f6; border-radius: 4px;
    display: none;
  }}
  .review-summary.visible {{ display: block; }}

  /* === Responsive === */
  @media (max-width: 991px) {{
    .hsg-width-two-thirds, .hsg-width-sidebar {{ width: 100%; float: none; }}
  }}
</style>
</head>
<body>
<div class="mockup-badge">Mockup &mdash; Proposed Design</div>
<button class="rebuild-btn" id="btn-rebuild" onclick="runRebuildMockup()">Rebuild Data</button>

<!-- Output Panel -->
<div class="output-overlay" id="output-overlay">
  <div class="output-panel">
    <div class="output-header">
      <span id="output-title">Rebuilding mockup...</span>
      <button class="output-close" onclick="closeOutputPanel()">&times;</button>
    </div>
    <div class="output-body" id="output-body"></div>
  </div>
</div>

<!-- === HSG Header === -->
<header class="hsg-header">
  <section class="hsg-header-inner">
    <div class="container hsg-header-content">
      <div class="row">
        <div class="hsg-header-logo">
          <a href="https://history.state.gov">
            <img src="https://history.state.gov/resources/images/Office-of-the-Historian-logo_500x168.jpg" alt="Office of the Historian" width="400">
          </a>
        </div>
        <div class="hsg-header-search">
          <input type="text" placeholder="Search..." class="form-control">
        </div>
      </div>
    </div>
  </section>
</header>

<!-- === HSG Nav === -->
<nav class="hsg-nav">
  <div class="container hsg-nav-content">
    <ul>
      <li><a href="https://history.state.gov">Home</a></li>
      <li><a href="https://history.state.gov/historicaldocuments">Historical Documents</a></li>
      <li><a href="https://history.state.gov/departmenthistory">Department History</a></li>
      <li><a href="https://history.state.gov/countries">Guide to Countries</a></li>
      <li><a href="https://history.state.gov/about">About</a></li>
      <li class="active"><a href="https://history.state.gov/tags">Tags</a></li>
    </ul>
  </div>
</nav>

<!-- === HSG Main === -->
<div class="hsg-main">
  <section class="hsg-main-inner">
    <div class="container hsg-main-content">

      <!-- Breadcrumb -->
      <nav class="hsg-breadcrumb" aria-label="breadcrumbs">
        <ol class="hsg-breadcrumb__list">
          <li class="hsg-breadcrumb__list-item"><a href="https://history.state.gov" class="hsg-breadcrumb__link">Home</a></li>
          <li class="hsg-breadcrumb__list-item"><a href="https://history.state.gov/tags" class="hsg-breadcrumb__link">Tags</a></li>
          <li class="hsg-breadcrumb__list-item"><a href="#" class="hsg-breadcrumb__link" aria-current="page" id="breadcrumb-cat">{default_cat}</a></li>
        </ol>
      </nav>

      <!-- Review Decision Import Panel -->
      <div class="review-panel" id="review-panel">
        <div class="review-toggle" onclick="toggleReviewPanel()">
          <span class="arrow">&#9654;</span> Import Review Decisions <span class="review-badge" id="review-badge" style="display:none;"></span>
        </div>
        <div class="review-content" id="review-content" style="display:none;">
          <div class="review-import-row">
            <label>Taxonomy Decisions <small>(from taxonomy-review: .xml or .json)</small>:</label>
            <input type="file" id="lcsh-file" accept=".xml,.json" onchange="importLcshFile(this)">
            <span class="review-status" id="lcsh-status"></span>
          </div>
          <div class="review-import-row">
            <label>Annotation Decisions <small>(from string-match-review)</small>:</label>
            <input type="file" id="annotation-file" accept=".json" onchange="importAnnotationFile(this)">
            <span class="review-status" id="annotation-status"></span>
          </div>
          <div class="review-buttons">
            <button class="btn-apply" id="btn-apply" onclick="applyReviewDecisions()" disabled>Apply Decisions</button>
            <button class="btn-clear" onclick="clearReviewDecisions()">Clear All</button>
          </div>
          <div class="review-summary" id="review-summary"></div>
        </div>
      </div>

      <div class="row">
        <!-- Sidebar (1/3 left) -->
        <div class="hsg-width-sidebar">

          <!-- Subcategories Panel -->
          <div class="hsg-panel">
            <div class="hsg-panel-heading">
              <h2 class="hsg-sidebar-title">Subcategories</h2>
            </div>
            <div class="sidebar-search">
              <input type="text" id="sidebar-search" placeholder="Search subjects..." oninput="filterSidebar(this.value)">
            </div>
            <div id="search-results" style="display:none;"></div>
            <ul id="subcat-nav" class="subcat-nav-list"></ul>
          </div>

          <!-- Topics Panel -->
          <aside class="hsg-panel">
            <div class="hsg-panel-heading">
              <h2 class="hsg-sidebar-title">Topics</h2>
            </div>
            <ul class="hsg-list-group" id="topics-list">
                            {topics_li}
            </ul>
          </aside>

        </div>

        <!-- Main Content (2/3 right) -->
        <div class="hsg-width-two-thirds" id="main-area">
          <h1 id="page-title">{default_cat}</h1>
          <p class="tag-description" id="page-description">{default_desc}</p>
          <div class="stats-banner" id="stats-banner">
            <div><span class="stat-val" id="stat-subjects">{default_subjects}</span> subjects</div>
            <div><span class="stat-val" id="stat-docs">{default_docs:,}</span> document references</div>
            <div><span class="stat-val" id="stat-vols">{default_vols}</span> FRUS volumes</div>
          </div>
          <div id="subject-view">
            <div class="welcome-view">
              <h2>Select a subject</h2>
              <p>Use the sidebar to browse subcategories and select a subject heading to view its associated FRUS documents.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  </section>
</div>

<!-- === HSG Footer (3 tiers) === -->
<footer>
  <section class="hsg-footer-top">
    <div class="container">
      <div class="row">
        <ul class="hsg-footer-list">
          <li>Learn More</li>
          <li><a href="https://history.state.gov">Home</a></li>
          <li><a href="https://history.state.gov/search">Search</a></li>
          <li><a href="https://history.state.gov/about/faq">FAQ</a></li>
        </ul>
        <ul class="hsg-footer-list">
          <li>Topics</li>
          <li><a href="https://history.state.gov/historicaldocuments">Historical Documents</a></li>
          <li><a href="https://history.state.gov/departmenthistory">Department History</a></li>
          <li><a href="https://history.state.gov/countries">Countries</a></li>
        </ul>
        <ul class="hsg-footer-list">
          <li>Contact</li>
          <li><a href="mailto:history@state.gov">history@state.gov</a></li>
          <li>Phone: 202-955-0200</li>
        </ul>
        <ul class="hsg-footer-list">
          <li>Policies</li>
          <li><a href="#">Accessibility</a></li>
          <li><a href="#">Privacy</a></li>
          <li><a href="#">Copyright</a></li>
        </ul>
      </div>
    </div>
  </section>
  <section class="hsg-footer-bottom">
    <div class="container">
      <address>Office of the Historian | Shared Knowledge Services | Bureau of Administration</address>
    </div>
  </section>
  <section class="hsg-footer-nether">
    <div class="container">
      <p>An official website of the United States Government</p>
    </div>
  </section>
</footer>

<script id="sidebar-data" type="application/json">{sidebar_json}</script>
<script>
const allSidebarData = JSON.parse(document.getElementById('sidebar-data').textContent);
const subjectData = {{}};  // populated lazily per-category
const categorySlugs = {cat_slugs_json};
const categoryCache = {{}};  // slug -> already loaded
let currentCategory = '{default_cat}';
let currentSidebarData = allSidebarData[currentCategory] || [];

async function loadCategoryData(catName) {{
  const slug = categorySlugs[catName];
  if (!slug || categoryCache[slug]) return;
  const resp = await fetch('/data/mockup/' + slug + '.json');
  if (!resp.ok) throw new Error('Failed to load ' + slug + '.json: ' + resp.status);
  const data = await resp.json();
  // Snapshot originals before merging (for review reset)
  for (const [k, v] of Object.entries(data)) {{
    originalSubjectData[k] = JSON.parse(JSON.stringify(v));
  }}
  Object.assign(subjectData, data);
  categoryCache[slug] = true;
}}

const CAT_DESCRIPTIONS = {json.dumps(CAT_DESCRIPTIONS, separators=(",", ":"))};

function esc(s) {{ return s ? String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;') : ''; }}

async function switchCategory(catName, linkEl) {{
  currentCategory = catName;
  currentSidebarData = allSidebarData[catName] || [];

  document.querySelectorAll('#topics-list .hsg-list-group-item').forEach(li => li.classList.remove('active'));
  if (linkEl) linkEl.closest('.hsg-list-group-item').classList.add('active');

  document.getElementById('breadcrumb-cat').textContent = catName;
  document.getElementById('page-title').textContent = catName;
  document.getElementById('page-description').textContent = CAT_DESCRIPTIONS[catName] || '';

  // Update stats that come from sidebar data (available immediately)
  const totalSubjects = currentSidebarData.reduce((sum, sc) => sum + sc.subjects.length, 0);
  const totalDocs = currentSidebarData.reduce((sum, sc) => sum + sc.docCount, 0);
  document.getElementById('stat-subjects').textContent = totalSubjects;
  document.getElementById('stat-docs').textContent = totalDocs.toLocaleString();
  document.getElementById('stat-vols').textContent = '...';

  buildSidebar();
  document.getElementById('sidebar-search').value = '';
  document.getElementById('search-results').style.display = 'none';

  // Show loading state while fetching category data
  const subjectView = document.getElementById('subject-view');
  if (!categoryCache[categorySlugs[catName]]) {{
    subjectView.innerHTML = '<div class="welcome-view"><h2>Loading...</h2><p>Fetching subject data for ' + esc(catName) + '...</p></div>';
  }}

  try {{
    await loadCategoryData(catName);
  }} catch(err) {{
    subjectView.innerHTML = '<div class="welcome-view"><h2>Error</h2><p>' + esc(err.message) + '</p></div>';
    return;
  }}

  // Update volume count (requires loaded subject data)
  const volSet = new Set();
  currentSidebarData.forEach(sc => sc.subjects.forEach(s => {{
    const sd = subjectData[s.ref];
    if (sd) Object.keys(sd.volumes).forEach(v => volSet.add(v));
  }}));
  document.getElementById('stat-vols').textContent = volSet.size;

  subjectView.innerHTML = '<div class="welcome-view"><h2>Select a subject</h2><p>Use the sidebar to browse subcategories and select a subject heading to view its associated FRUS documents.</p></div>';
}}

function buildSidebar() {{
  const nav = document.getElementById('subcat-nav');
  nav.innerHTML = '';
  currentSidebarData.forEach(sc => {{
    const li = document.createElement('li');
    li.innerHTML = '<div class="subcat-toggle" onclick="toggleSubcat(this)"><span>' + esc(sc.name) + ' <span class="scount">(' + sc.subjects.length + ')</span></span><span class="arrow">&#9654;</span></div>' +
      '<ul class="subcat-subjects">' + sc.subjects.map(s =>
        '<li><a href="#" onclick="selectSubject(event,\\'' + s.ref + '\\',\\'' + sc.id + '\\')">' + esc(s.name) + ' <span class="sdcount">' + s.count + '</span></a></li>'
      ).join('') + '</ul>';
    nav.appendChild(li);
  }});
}}

function toggleSubcat(el) {{ el.classList.toggle('open'); }}

function selectSubject(e, ref, subcatId) {{
  e.preventDefault();
  document.querySelectorAll('.subcat-subjects a').forEach(a => a.classList.remove('active'));
  e.currentTarget.classList.add('active');
  renderSubject(ref, subcatId);
}}

function renderSubject(ref, subcatId) {{
  const s = subjectData[ref];
  if (!s) return;
  const subcat = currentSidebarData.find(sc => sc.id === subcatId);
  const subcatName = subcat ? subcat.name : '';

  let html = '<div class="subject-header">';
  html += '<div class="subcat-path"><a onclick="switchCategory(currentCategory)">' + esc(currentCategory) + '</a> &#8250; <a onclick="showSubcatOverview(\\'' + subcatId + '\\')">' + esc(subcatName) + '</a> &#8250; ' + esc(s.name) + '</div>';
  html += '<h2>' + esc(s.name) + '</h2>';
  if (s.lcsh) {{
    html += '<div class="lcsh-info">LCSH: <span>' + esc(s.lcsh) + '</span></div>';
  }} else if (reviewApplied && lcshDecisions[ref] === 'rejected') {{
    html += '<div class="lcsh-info" style="color:#999;"><s>LCSH rejected</s></div>';
  }}
  const totalDocs = Object.values(s.volumes).reduce((sum, v) => sum + v.docs.length, 0);
  const numVols = Object.keys(s.volumes).length;
  html += '<div style="font-size:14px;color:#71767a;margin-top:4px;">' + totalDocs + ' documents across ' + numVols + ' volume' + (numVols !== 1 ? 's' : '') + '</div>';
  if (s.merged && s.merged.length > 1) {{
    const names = (s.merged_names || s.merged).filter(n => n !== s.name);
    if (names.length > 0) {{
      html += '<div class="merged-note">Includes merged: ' + names.map(n => esc(n)).join(', ') + '</div>';
    }} else {{
      html += '<div class="merged-note">Combined from ' + s.merged.length + ' variant entries</div>';
    }}
  }}
  html += '</div>';

  for (const [volId, vol] of Object.entries(s.volumes)) {{
    html += '<div class="vol-group"><div class="vol-label"><a href="' + vol.url + '" target="_blank">' + esc(vol.title) + '</a></div><table class="doc-table">';
    for (const doc of vol.docs) {{
      html += '<tr><td class="doc-title"><a href="' + doc.url + '" target="_blank">' + esc(doc.title) + '</a></td><td class="doc-date">' + esc(doc.date) + '</td></tr>';
    }}
    html += '</table></div>';
  }}
  document.getElementById('subject-view').innerHTML = html;
}}

function showSubcatOverview(subcatId) {{
  const subcat = currentSidebarData.find(sc => sc.id === subcatId);
  if (!subcat) return;
  let html = '<div class="subject-header"><h2>' + esc(subcat.name) + '</h2>';
  html += '<div style="font-size:14px;color:#71767a;margin-top:4px;">' + subcat.subjects.length + ' subjects, ' + subcat.docCount.toLocaleString() + ' document references</div></div>';
  html += '<table class="doc-table">';
  subcat.subjects.forEach(s => {{
    html += '<tr><td class="doc-title"><a href="#" onclick="selectSubjectFromOverview(\\'' + s.ref + '\\',\\'' + subcatId + '\\'); return false;">' + esc(s.name) + '</a></td><td class="doc-date">' + s.count + ' refs</td></tr>';
  }});
  html += '</table>';
  document.getElementById('subject-view').innerHTML = html;
}}

function selectSubjectFromOverview(ref, subcatId) {{
  renderSubject(ref, subcatId);
  document.querySelectorAll('.subcat-subjects a').forEach(a => {{
    a.classList.remove('active');
    if (a.onclick && a.onclick.toString().includes(ref)) a.classList.add('active');
  }});
}}

function filterSidebar(query) {{
  const q = query.trim().toLowerCase();
  const resultsEl = document.getElementById('search-results');
  const navEl = document.getElementById('subcat-nav');
  if (!q) {{
    resultsEl.style.display = 'none';
    navEl.style.display = '';
    return;
  }}
  navEl.style.display = 'none';
  resultsEl.style.display = 'block';

  let matches = [];
  for (const [catName, subs] of Object.entries(allSidebarData)) {{
    for (const sc of subs) {{
      for (const s of sc.subjects) {{
        if (s.name.toLowerCase().includes(q)) {{
          matches.push({{ ref: s.ref, name: s.name, count: s.count, subcatId: sc.id, subcatName: sc.name, catName: catName }});
        }}
      }}
    }}
  }}
  matches.sort((a, b) => b.count - a.count);
  if (matches.length > 50) matches = matches.slice(0, 50);

  if (matches.length === 0) {{
    resultsEl.innerHTML = '<div style="padding:10px 15px;font-size:13px;color:#71767a;">No subjects found</div>';
  }} else {{
    resultsEl.innerHTML = matches.map(m =>
      '<a href="#" onclick="switchToCatAndSelect(\\'' + esc(m.catName) + '\\',\\'' + m.ref + '\\',\\'' + m.subcatId + '\\'); return false;">' +
      esc(m.name) + ' <span class="sr-meta">' + m.count + ' &middot; ' + esc(m.catName) + ' &#8250; ' + esc(m.subcatName) + '</span></a>'
    ).join('');
  }}
}}

async function switchToCatAndSelect(catName, ref, subcatId) {{
  if (catName !== currentCategory) {{
    const items = document.querySelectorAll('#topics-list .hsg-list-group-item a');
    let targetLink = null;
    items.forEach(a => {{ if (a.textContent === catName) targetLink = a; }});
    if (targetLink) await switchCategory(catName, targetLink);
  }} else {{
    // Ensure data is loaded even if category matches
    await loadCategoryData(catName);
  }}
  renderSubject(ref, subcatId);
  document.getElementById('search-results').style.display = 'none';
  document.getElementById('subcat-nav').style.display = '';
  document.getElementById('sidebar-search').value = '';
}}

// === Review Decision Import ===
const originalSidebarData = JSON.parse(JSON.stringify(allSidebarData));
// Original subject data snapshots (saved per-category as loaded, for review reset)
const originalSubjectData = {{}};

let lcshDecisions = {{}};
let mergeDecisions = {{}};
let reviewApplied = false;

function toggleReviewPanel() {{
  const toggle = document.querySelector('.review-toggle');
  const content = document.getElementById('review-content');
  toggle.classList.toggle('open');
  content.style.display = content.style.display === 'none' ? 'block' : 'none';
}}

function importLcshFile(input) {{
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = function(e) {{
    try {{
      const text = e.target.result;
      const isXml = file.name.endsWith('.xml') || text.trimStart().startsWith('<?xml') || text.trimStart().startsWith('<taxonomy');
      let accepted = 0, rejected = 0;

      if (isXml) {{
        // Parse taxonomy XML exported from taxonomy-review.html
        const parser = new DOMParser();
        const doc = parser.parseFromString(text, 'application/xml');

        // Read lcsh-review attributes from <subject> elements
        const subjects = doc.querySelectorAll('subject');
        for (const subj of subjects) {{
          const ref = subj.getAttribute('ref');
          const lcshReview = subj.getAttribute('lcsh-review');
          if (ref && lcshReview) {{
            lcshDecisions[ref] = lcshReview;
            if (lcshReview === 'rejected') rejected++;
            else if (lcshReview === 'accepted') accepted++;
          }}
        }}

        // Subjects in <rejected> section are fully rejected
        const rejectedSubjects = doc.querySelectorAll('rejected > subject');
        for (const subj of rejectedSubjects) {{
          const ref = subj.getAttribute('ref');
          if (ref && !lcshDecisions[ref]) {{
            // These are fully rejected subjects (not just LCSH rejections)
            // Mark them so they can be noted, but don't count as LCSH decisions
          }}
        }}

        const statusEl = document.getElementById('lcsh-status');
        statusEl.textContent = `Loaded XML: ${{accepted}} LCSH accepted, ${{rejected}} LCSH rejected (${{subjects.length}} subjects total)`;
        statusEl.className = 'review-status loaded';
      }} else {{
        // Parse lcsh_decisions.json
        const data = JSON.parse(text);
        const decisions = data.decisions || [];
        for (const d of decisions) {{
          if (d.ref && d.decision) {{
            lcshDecisions[d.ref] = d.decision;
            if (d.decision === 'rejected') rejected++;
            else if (d.decision === 'accepted') accepted++;
          }}
        }}
        const statusEl = document.getElementById('lcsh-status');
        statusEl.textContent = `Loaded: ${{accepted}} accepted, ${{rejected}} rejected`;
        statusEl.className = 'review-status loaded';
      }}

      updateApplyButton();
    }} catch(err) {{
      document.getElementById('lcsh-status').textContent = 'Error: ' + err.message;
    }}
  }};
  reader.readAsText(file);
}}

function importAnnotationFile(input) {{
  const file = input.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = function(e) {{
    try {{
      const data = JSON.parse(e.target.result);
      // Import LCSH decisions from annotation review (lower priority — taxonomy wins)
      let lcshCount = 0;
      for (const d of (data.lcsh_decisions || [])) {{
        if (d.ref && d.decision && !lcshDecisions[d.ref]) {{
          lcshDecisions[d.ref] = d.decision;
          lcshCount++;
        }}
      }}
      // Import merge decisions
      let mergeCount = 0;
      for (const m of (data.merge_decisions || [])) {{
        if (m.source_ref && m.target_ref) {{
          mergeDecisions[m.source_ref] = {{
            targetRef: m.target_ref,
            targetName: m.target_term || m.target_ref,
            sourceName: m.source_term || m.source_ref,
          }};
          mergeCount++;
        }}
      }}
      const rejectCount = (data.rejections || []).length;
      const statusEl = document.getElementById('annotation-status');
      statusEl.textContent = `Loaded: ${{mergeCount}} merges, ${{lcshCount}} LCSH, ${{rejectCount}} rejections`;
      statusEl.className = 'review-status loaded';
      updateApplyButton();
    }} catch(err) {{
      document.getElementById('annotation-status').textContent = 'Error: invalid JSON';
    }}
  }};
  reader.readAsText(file);
}}

function updateApplyButton() {{
  const hasData = Object.keys(lcshDecisions).length > 0 || Object.keys(mergeDecisions).length > 0;
  document.getElementById('btn-apply').disabled = !hasData;
}}

function applyReviewDecisions() {{
  if (reviewApplied) {{
    // Reset first
    clearReviewDecisions(true);
  }}

  let lcshRejected = 0;
  let mergesApplied = 0;
  let mergedNames = [];

  // 1. Apply LCSH rejections
  for (const [ref, decision] of Object.entries(lcshDecisions)) {{
    if (decision === 'rejected' && subjectData[ref]) {{
      subjectData[ref].lcsh = null;
      lcshRejected++;
    }}
  }}

  // 2. Apply merge decisions
  for (const [sourceRef, merge] of Object.entries(mergeDecisions)) {{
    const source = subjectData[sourceRef];
    const target = subjectData[merge.targetRef];
    if (!source || !target) continue;

    // Merge volumes/docs from source into target
    for (const [volId, vol] of Object.entries(source.volumes || {{}})) {{
      if (target.volumes[volId]) {{
        // Merge doc lists (avoid duplicates by doc URL)
        const existingUrls = new Set(target.volumes[volId].docs.map(d => d.url));
        for (const doc of vol.docs) {{
          if (!existingUrls.has(doc.url)) {{
            target.volumes[volId].docs.push(doc);
          }}
        }}
      }} else {{
        target.volumes[volId] = JSON.parse(JSON.stringify(vol));
      }}
    }}

    // Update target merged list
    target.merged = target.merged || [];
    target.merged.push(sourceRef);

    // Update target count
    target.count = (target.count || 0) + (source.count || 0);

    // Remove source from sidebar data
    for (const [catName, subs] of Object.entries(allSidebarData)) {{
      for (const sc of subs) {{
        const idx = sc.subjects.findIndex(s => s.ref === sourceRef);
        if (idx !== -1) {{
          // Add source's count to target in sidebar
          const targetInSidebar = sc.subjects.find(s => s.ref === merge.targetRef);
          if (targetInSidebar) {{
            targetInSidebar.count += sc.subjects[idx].count;
          }}
          sc.subjects.splice(idx, 1);
          sc.docCount = sc.subjects.reduce((sum, s) => sum + s.count, 0);
        }}
      }}
    }}

    // Also check if target is in a different subcategory
    for (const [catName, subs] of Object.entries(allSidebarData)) {{
      for (const sc of subs) {{
        const targetInSidebar = sc.subjects.find(s => s.ref === merge.targetRef);
        if (targetInSidebar && !sc.subjects.find(s => s.ref === sourceRef)) {{
          // Target is here, update count from source we already removed
          sc.docCount = sc.subjects.reduce((sum, s) => sum + s.count, 0);
        }}
      }}
    }}

    mergedNames.push(`${{merge.sourceName}} → ${{merge.targetName}}`);
    mergesApplied++;
  }}

  // 3. Re-render
  currentSidebarData = allSidebarData[currentCategory] || [];
  switchCategory(currentCategory, document.querySelector('#topics-list .hsg-list-group-item.active a'));

  // 4. Show summary
  reviewApplied = true;
  const summaryEl = document.getElementById('review-summary');
  let summaryHtml = '<strong>Applied:</strong> ';
  const parts = [];
  if (lcshRejected > 0) parts.push(`${{lcshRejected}} LCSH rejection${{lcshRejected > 1 ? 's' : ''}}`);
  if (mergesApplied > 0) parts.push(`${{mergesApplied}} merge${{mergesApplied > 1 ? 's' : ''}}`);
  summaryHtml += parts.join(', ') || 'No changes';
  if (mergedNames.length > 0) {{
    summaryHtml += '<br><small>' + mergedNames.map(n => esc(n)).join('<br>') + '</small>';
  }}
  summaryEl.innerHTML = summaryHtml;
  summaryEl.className = 'review-summary visible';

  // Update badge
  const total = lcshRejected + mergesApplied;
  const badge = document.getElementById('review-badge');
  if (total > 0) {{
    badge.textContent = `${{total}} applied`;
    badge.style.display = 'inline';
  }}
}}

function clearReviewDecisions(silent) {{
  // Restore original data
  for (const key of Object.keys(allSidebarData)) delete allSidebarData[key];
  Object.assign(allSidebarData, JSON.parse(JSON.stringify(originalSidebarData)));

  // Restore subject data from snapshots (only keys that were loaded)
  for (const [key, val] of Object.entries(originalSubjectData)) {{
    subjectData[key] = JSON.parse(JSON.stringify(val));
  }}

  if (!silent) {{
    lcshDecisions = {{}};
    mergeDecisions = {{}};
    document.getElementById('lcsh-file').value = '';
    document.getElementById('annotation-file').value = '';
    document.getElementById('lcsh-status').textContent = '';
    document.getElementById('lcsh-status').className = 'review-status';
    document.getElementById('annotation-status').textContent = '';
    document.getElementById('annotation-status').className = 'review-status';
    updateApplyButton();
  }}

  reviewApplied = false;
  document.getElementById('review-summary').className = 'review-summary';
  document.getElementById('review-summary').innerHTML = '';
  document.getElementById('review-badge').style.display = 'none';

  // Re-render
  currentSidebarData = allSidebarData[currentCategory] || [];
  switchCategory(currentCategory, document.querySelector('#topics-list .hsg-list-group-item.active a'));
}}

// Load default category data, then build sidebar
(async () => {{
  try {{
    await loadCategoryData(currentCategory);
  }} catch(e) {{
    console.error('Failed to load default category:', e);
  }}
  buildSidebar();

  // Compute initial stats now that data is loaded
  const totalSubjects = currentSidebarData.reduce((sum, sc) => sum + sc.subjects.length, 0);
  const totalDocs = currentSidebarData.reduce((sum, sc) => sum + sc.docCount, 0);
  const volSet = new Set();
  currentSidebarData.forEach(sc => sc.subjects.forEach(s => {{
    const sd = subjectData[s.ref];
    if (sd) Object.keys(sd.volumes).forEach(v => volSet.add(v));
  }}));
  document.getElementById('stat-subjects').textContent = totalSubjects;
  document.getElementById('stat-docs').textContent = totalDocs.toLocaleString();
  document.getElementById('stat-vols').textContent = volSet.size;
}})();

// === Rebuild Mockup ===
function openOutputPanel(title) {{
  document.getElementById('output-title').textContent = title;
  document.getElementById('output-body').innerHTML = '';
  document.getElementById('output-overlay').classList.add('visible');
}}

function closeOutputPanel() {{
  document.getElementById('output-overlay').classList.remove('visible');
}}

function appendOutput(text, className) {{
  const body = document.getElementById('output-body');
  const line = document.createElement('div');
  if (className) line.className = className;
  line.textContent = text;
  body.appendChild(line);
  body.scrollTop = body.scrollHeight;
}}

function runRebuildMockup() {{
  if (!confirm('Rebuild mockup data from current annotations and taxonomy?\\n\\nThis will regenerate the data and reload the page when done.')) return;

  const btn = document.getElementById('btn-rebuild');
  btn.disabled = true;
  btn.textContent = 'Rebuilding...';
  openOutputPanel('Rebuilding HSG subjects mockup...');

  fetch('/api/rebuild-mockup', {{ method: 'POST' }})
    .then(response => {{
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let success = false;

      function processStream() {{
        return reader.read().then(({{ done, value }}) => {{
          if (done) {{
            if (success) {{
              appendOutput('\\nReloading page...', 'line-success');
              setTimeout(() => location.reload(), 1000);
            }} else {{
              btn.disabled = false;
              btn.textContent = 'Rebuild Data';
            }}
            return;
          }}
          buffer += decoder.decode(value, {{ stream: true }});
          const lines = buffer.split('\\n');
          buffer = lines.pop();
          for (const line of lines) {{
            if (!line.startsWith('data: ')) continue;
            try {{
              const msg = JSON.parse(line.slice(6));
              if (msg.type === 'output') {{
                let cls = '';
                if (msg.line.includes('✓')) cls = 'line-success';
                else if (msg.line.includes('ERROR')) cls = 'line-error';
                else if (msg.line.startsWith('=') || msg.line.startsWith('─') || msg.line.startsWith('Step:')) cls = 'line-heading';
                appendOutput(msg.line, cls);
              }} else if (msg.type === 'start') {{
                appendOutput(msg.line, 'line-heading');
              }} else if (msg.type === 'done') {{
                success = msg.status === 'success';
                if (success) {{
                  appendOutput('\\nDone! Reloading...', 'line-success');
                }} else {{
                  appendOutput('\\nFailed (exit code ' + msg.code + ')', 'line-error');
                }}
              }} else if (msg.type === 'error') {{
                appendOutput(msg.line, 'line-error');
              }}
            }} catch (e) {{}}
          }}
          return processStream();
        }});
      }}

      return processStream();
    }})
    .catch(err => {{
      appendOutput('Network error: ' + err.message, 'line-error');
      appendOutput('\\nIs the server running? Start it with: make serve', 'line-error');
      btn.disabled = false;
      btn.textContent = 'Rebuild Data';
    }});
}}
</script>
</body>
</html>'''

    with open("../hsg-subjects-mockup.html", "w") as f:
        f.write(html)

    file_size = os.path.getsize("../hsg-subjects-mockup.html")
    total_subjects = sum(len(s["subjects"]) for subs in sidebar_data.values() for s in subs)
    print(f"Wrote hsg-subjects-mockup.html ({file_size / 1024:.0f} KB)")
    print(f"  Categories: {len(cat_names)}")
    print(f"  Total subjects: {total_subjects}")


if __name__ == "__main__":
    main()
