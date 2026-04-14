# FRUS Subject Taxonomy — Claude Code Guide

## Project Overview

Pipeline for building a subject taxonomy for the Foreign Relations of the United States (FRUS) series with LCSH integration. 1,315 taxonomy subjects across 13 categories, 43 source volumes, 38K+ discovery candidates.

## Key Files

- `subject-taxonomy-lcsh.xml` — The main taxonomy (source of truth, unfiltered)
- `taxonomy_review_state.json` — Central state for ALL review tools (decisions, overrides, exclusions, candidate reviews)
- `serve.py` — Flask dev server with API endpoints and SSE streaming
- `Makefile` — Entry point for all pipeline operations
- `exports/taxonomy.json` — Taxonomy structure JSON (for frus-otd)
- `exports/document_subjects.json` — Document-to-subject mapping JSON (for frus-otd)

## Architecture

### Pipeline Stages
1. **Split**: `volumes/*.xml` → `data/documents/{vol}/d*.xml`
2. **Annotate**: String-match against taxonomy → `string_match_results_{vol}.json`
3. **Review**: Browser tools save to `taxonomy_review_state.json` via API
4. **Build**: Decisions applied at build time → taxonomy XML, mockup HTML

### Discovery Pipeline (newer)
1. **Tier 2**: `discover_index_terms.py` → `data/index_candidates.json` (37,878 candidates)
2. **Tier 3**: `discover_lcsh_terms.py` → `data/lcsh_candidates.json` (259 candidates)
3. **Split**: `split_review_categories.py` → `data/review_{persons,organizations,topics}.json`
4. **Review**: `build_candidates_review.py --category all` → per-category HTML tools
5. **Promote**: `promote_candidates.py` → injects accepted terms into taxonomy

### Review Tools (all generated, not committed)
- `string-match-review.html` — Annotation review per volume
- `taxonomy-review.html` — Taxonomy organization + LCSH mapping
- `candidates-review-{persons,organizations,topics}.html` — Category candidate review

### State Management
All review tools share `taxonomy_review_state.json` via read-modify-write pattern.
Keys: `lcsh_decisions`, `category_overrides`, `merge_decisions`, `exclusions`, `global_rejections`, `candidate_decisions_{persons,organizations,topics}`, `reassigned_to_{persons,organizations,topics}`

## Common Tasks

```bash
make serve              # Start dev server on port 9090
make setup              # Full setup from scratch
make pipeline VOL=X     # Post-review pipeline for a volume
make discover           # Run term discovery (Tier 2 + 3)
make validate           # Data integrity check
make export-json        # Export JSON for frus-otd consumption
python3 scripts/build_candidates_review.py --category all  # Rebuild candidate review tools
python3 -m pytest tests/  # Run tests
```

## Code Conventions

- Scripts use `os.chdir(os.path.dirname(os.path.abspath(__file__)))` for relative paths from `scripts/`
- All generated HTML is self-contained (embedded CSS/JS/data)
- HTML review tools communicate with server via `/api/load-taxonomy-decisions` and `/api/save-taxonomy-decisions`
- Pipeline scripts are orchestrated by `run_reviewed_pipeline.py` (8 steps) or individual Makefile targets
- Config files live in `config/`, intermediate outputs at repo root, source data in `data/` and `volumes/`
- lxml is used for all XML processing
- Tests are in `tests/` using pytest

## Important Notes

- Never modify `data/documents/*/string_match_results_*.json` — these are read-only annotation records
- Taxonomy XML is always unfiltered; editorial decisions are layered on at build time
- The `config/backups/` directory holds auto-backups (20 most recent per file)
- Generated HTML files are in `.gitignore` — rebuild with make targets
