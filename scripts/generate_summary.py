#!/usr/bin/env python3
"""
Generate a mobile-friendly HTML summary page from string match results.

Usage:
    python3 scripts/generate_summary.py string_match_results_frus1981-88v01.json
    python3 scripts/generate_summary.py string_match_results_*.json
"""

import json
import sys
import os
from pathlib import Path


def lcsh_badge(lm):
    colors = {
        'exact': '#2e7d32', 'good_close': '#1565c0', 'bad_close': '#e65100',
        'ambiguous_close': '#6a1b9a', 'none': '#757575'
    }
    return f'<span class="badge" style="background:{colors.get(lm or "none", "#757575")}">{lm or "none"}</span>'


def generate_summary(json_path):
    with open(json_path) as f:
        data = json.load(f)

    md = data['metadata']
    volume_id = md['volume_id']

    # Category stats
    cats = {}
    for ref, td in data['by_term'].items():
        cat = td.get('category', 'Unknown')
        if cat not in cats:
            cats[cat] = {'terms': 0, 'matches': 0}
        cats[cat]['terms'] += 1
        cats[cat]['matches'] += sum(len(d) for d in td['documents'].values())
    cats_sorted = sorted(cats.items(), key=lambda x: -x[1]['matches'])

    # Top terms
    term_counts = []
    for ref, td in data['by_term'].items():
        total = sum(len(d) for d in td['documents'].values())
        ndocs = len(td['documents'])
        term_counts.append({
            'term': td['term'], 'cat': td.get('category', '?'),
            'type': td.get('type', '?'), 'lcsh': td.get('lcsh_match', ''),
            'matches': total, 'docs': ndocs
        })
    term_counts.sort(key=lambda x: -x['matches'])

    # LCSH quality
    lcsh_q = {}
    total_terms = len(data['by_term'])
    for ref, td in data['by_term'].items():
        lm = td.get('lcsh_match', '') or 'none'
        lcsh_q[lm] = lcsh_q.get(lm, 0) + 1

    # Problematic terms
    problems = [t for t in term_counts if len(t['term']) <= 5 and t['matches'] > 50]

    # Sample contexts for top 15
    samples = {}
    for tc in term_counts[:15]:
        term_name = tc['term']
        for ref, td in data['by_term'].items():
            if td['term'] == term_name:
                sample_matches = []
                for doc_id, matches in list(td['documents'].items())[:3]:
                    for m in matches[:1]:
                        ctx = m.get('sentence', '')[:200]
                        sample_matches.append({'doc': doc_id, 'ctx': ctx})
                samples[term_name] = sample_matches
                break

    # Build HTML
    cats_rows = ''.join(
        f'<tr><td>{c}</td><td class="n">{v["terms"]}</td><td class="n">{v["matches"]}</td></tr>'
        for c, v in cats_sorted
    )

    top_rows = ''
    for t in term_counts[:50]:
        top_rows += (
            f'<tr><td><b>{t["term"]}</b><br><small class="muted">{t["cat"]}</small></td>'
            f'<td class="n">{t["matches"]}</td><td class="n">{t["docs"]}</td>'
            f'<td>{lcsh_badge(t["lcsh"])}</td></tr>\n'
        )

    problem_rows = ''
    for t in problems:
        problem_rows += (
            f'<tr><td><b>{t["term"]}</b></td><td class="n">{t["matches"]}</td>'
            f'<td>{lcsh_badge(t["lcsh"])}</td><td class="muted">Short/generic term</td></tr>\n'
        )

    lcsh_rows = ''.join(
        f'<tr><td>{lcsh_badge(k)}</td><td class="n">{v}</td>'
        f'<td class="n">{v * 100 // max(total_terms, 1)}%</td></tr>'
        for k, v in sorted(lcsh_q.items(), key=lambda x: -x[1])
    )

    sample_html = ''
    for term, samps in samples.items():
        from html import escape
        ctxs = ''.join(f'<div class="ctx">{escape(s["ctx"])}...</div>' for s in samps)
        sample_html += f'<div class="sample"><b>{escape(term)}</b>{ctxs}</div>'

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Match Review Summary - {volume_id}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,system-ui,sans-serif;background:#f5f5f5;color:#1b1b1b;padding:12px;max-width:100vw}}
h1{{font-size:18px;color:#112e51;margin-bottom:4px}}
h2{{font-size:16px;color:#205493;margin:20px 0 8px;border-bottom:2px solid #205493;padding-bottom:4px}}
.sub{{color:#71767a;font-size:13px;margin-bottom:16px}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:16px}}
.card{{background:white;border:1px solid #ddd;border-radius:8px;padding:14px;text-align:center}}
.big{{font-size:28px;font-weight:700;color:#205493}}
.label{{font-size:12px;color:#71767a}}
table{{width:100%;border-collapse:collapse;font-size:13px;background:white;border-radius:6px;overflow:hidden;margin-bottom:12px}}
th{{background:#112e51;color:white;padding:8px;text-align:left;font-size:12px}}
td{{padding:7px 8px;border-bottom:1px solid #eee}}
.n{{text-align:right;font-weight:600}}
.badge{{display:inline-block;padding:2px 6px;border-radius:3px;color:white;font-size:11px;font-weight:600}}
.muted{{color:#71767a;font-size:11px}}
small{{font-size:11px}}
.sample{{background:white;border:1px solid #ddd;border-radius:6px;padding:10px;margin-bottom:8px}}
.sample b{{color:#205493;font-size:14px}}
.ctx{{font-size:12px;color:#555;margin-top:4px;padding:4px 8px;background:#fafafa;border-left:3px solid #ddd;line-height:1.4}}
.warn{{background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:10px;margin-bottom:12px;font-size:13px}}
</style>
</head>
<body>
<h1>String Match Review</h1>
<div class="sub">{volume_id} &middot; Generated {md["generated"][:10]}</div>

<div class="grid">
<div class="card"><div class="big">{md["total_matches"]:,}</div><div class="label">Total Matches</div></div>
<div class="card"><div class="big">{md["total_documents"]}</div><div class="label">Documents</div></div>
<div class="card"><div class="big">{md["unique_terms_matched"]}</div><div class="label">Terms Matched</div></div>
<div class="card"><div class="big">{md["terms_not_matched"]}</div><div class="label">Unmatched Terms</div></div>
</div>

<h2>LCSH Match Quality</h2>
<table>{lcsh_rows}</table>

<h2>Categories</h2>
<table><tr><th>Category</th><th>Terms</th><th>Matches</th></tr>{cats_rows}</table>

<h2>Top 50 Terms</h2>
<table><tr><th>Term</th><th>Hits</th><th>Docs</th><th>LCSH</th></tr>{top_rows}</table>

{"<h2>Potentially Problematic Terms</h2><div class=warn>Short terms (5 chars) with many matches - may cause false positives</div><table><tr><th>Term</th><th>Hits</th><th>LCSH</th><th>Issue</th></tr>" + problem_rows + "</table>" if problem_rows else ""}

<h2>Sample Contexts (Top 15 Terms)</h2>
{sample_html}

</body></html>'''

    out_path = Path(json_path).parent / f'match-review-summary-{volume_id}.html'
    with open(out_path, 'w') as f:
        f.write(html)
    print(f"  {volume_id}: {md['total_matches']:,} matches, {md['unique_terms_matched']} terms -> {out_path.name}")
    return str(out_path)


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/generate_summary.py <results.json> [results.json ...]")
        sys.exit(1)

    for path in sys.argv[1:]:
        generate_summary(path)


if __name__ == "__main__":
    main()
