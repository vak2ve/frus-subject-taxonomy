# FRUS Subject Taxonomy Pipeline

A pipeline for building and maintaining a subject taxonomy for the *Foreign Relations of the United States* (FRUS) series, with LCSH (Library of Congress Subject Headings) integration. Developed at the Office of the Historian, U.S. Department of State.

## Overview

This project extracts subject annotations from FRUS TEI/XML volumes, consolidates duplicate and variant terms, maps subjects to LCSH authority records, and produces a structured taxonomy in XML. It includes browser-based review tools for editorial quality control at every stage: deduplication, annotation review, LCSH matching, taxonomy organization, and candidate term discovery.

**Current scale:**

- **1,375 taxonomy subjects** across 13 categories
- **560 source volumes** loaded, **551 split and annotated** (313,617 individual documents)
- **11 volumes** with curated annotation XML (1969–1988 subseries)
- **38,137 discovery candidates** (37,878 from back-of-book indexes + 259 from LCSH expansion)
- Split into 3 category-specific review queues: persons (13,577), organizations (9,507), topics (15,053)
- 9 volumes not split: 2 index volumes (no documents) and 7 placeholder/unpublished volumes

## Quick Start

Python 3.8+ with `flask` and `lxml` is required. A Makefile provides the single entry point for setup and all pipeline operations.

```bash
# Install dependencies + split volumes + convert annotations + build review tools
make setup

# Start the development server (port 9090)
make serve

# Open the review tools in your browser:
#   http://localhost:9090/string-match-review.html   (annotation review)
#   http://localhost:9090/taxonomy-review.html        (taxonomy/LCSH review)
```

Individual pipeline commands are also available:

```bash
# Run string-match annotation against a volume
python3 scripts/annotate_documents.py frus1969-76v19p2

# Run the full post-review pipeline for a volume
make pipeline VOL=frus1969-76v19p2

# Rebuild the taxonomy review tool (variant groups → doc appearances → XML → HTML)
make taxonomy-review

# Run term discovery (Tier 2 index + Tier 3 LCSH expansion)
make discover

# Build category-specific candidate review tools
python3 scripts/build_candidates_review.py --category all

# Validate data integrity across all volumes
make validate
```

## Repository Structure

```
├── Makefile                    Single entry point for setup and pipeline operations
├── serve.py                    Flask development server with SSE streaming + API
├── subject-taxonomy-lcsh.xml   The main taxonomy output (1,375 subjects)
├── taxonomy_review_state.json  Shared state for all review tools
│
├── scripts/                    Pipeline scripts (Python + JS)
├── volumes/                    Source TEI/XML volumes (560 volumes)
├── annotations/                Annotation output XML (11 volumes)
├── data/
│   ├── documents/              Per-volume split document files + annotation results
│   ├── index_candidates.json   Tier 2 discovery candidates (37,878)
│   ├── lcsh_candidates.json    Tier 3 discovery candidates (259)
│   ├── review_persons.json     Person candidates for review (13,577)
│   ├── review_organizations.json  Organization candidates for review (9,507)
│   └── review_topics.json      Topic candidates for review (15,053)
│
├── config/                     Editorial decisions and reference data
├── queries/                    XQuery files (BaseX + eXist-db)
│   └── exist-db/               eXist-db adapted versions
├── progress/                   Pipeline progress tracking XML
├── tests/                      Unit tests
├── mockups/                    UI mockups and screenshots
└── docs/                       Documentation
```

### Pipeline Scripts (`scripts/`)

#### Core Pipeline

| Script | Purpose |
|--------|---------|
| `split_volume.py` | Splits a monolithic TEI volume into per-document files |
| `annotate_documents.py` | String-match annotation engine with stoplist and variant support |
| `extract_existing_annotations.py` | Extracts pre-existing `<rs>` annotations from TEI volumes |
| `build_variant_groups.py` | Consolidates variant name forms from dedup decisions, semantic dedup, LCSH URI overlaps, and manual overrides |
| `apply_curated_annotations.py` | Applies reviewed annotations to TEI documents, removing rejected matches |
| `apply_review_decisions.py` | Applies exported review decisions (rejections, merges, LCSH) to results JSON |
| `merge_annotations.py` | Merges annotation data across volumes |
| `merge_annotations_to_appearances.py` | Merges annotations into document_appearances.json |
| `extract_doc_appearances.py` | Extracts document appearances from annotated XML |
| `build_taxonomy_lcsh.py` | Builds the taxonomy XML with LCSH broader-term hierarchies |
| `lcsh_mapper.py` | Queries id.loc.gov for LCSH matching |
| `run_reviewed_pipeline.py` | Orchestrates the full 8-step post-review pipeline |
| `validate_data.py` | Validates data integrity across all volumes |

#### Discovery & Candidate Review

| Script | Purpose |
|--------|---------|
| `discover_index_terms.py` | **Tier 2**: Extracts candidate terms from back-of-book indexes across the full FRUS corpus |
| `discover_lcsh_terms.py` | **Tier 3**: Expands taxonomy coverage via LCSH sibling/narrower-term relationships |
| `split_review_categories.py` | Splits raw discovery candidates into person, organization, and topic review queues |
| `build_candidates_review.py` | Builds interactive HTML review tools for each category (with cross-category reassignment) |
| `promote_candidates.py` | Injects accepted candidates into the taxonomy pipeline |
| `enrich_sentence_context.py` | Enriches candidate data with sentence-level context from source documents |

#### Review Tool Builders

| Script | Purpose |
|--------|---------|
| `build_annotation_review.py` | Builds the annotation review HTML tool |
| `build_taxonomy_review.py` | Builds the taxonomy/LCSH review HTML tool |
| `build_candidates_review.py` | Builds candidate term review HTML tools (per-category or combined) |
| `build_mockup_html.py` | Builds the self-contained HSG mockup |
| `generate_mockup_data.py` | Generates JSON data for the mockup; applies exclusions, merges, and category overrides |
| `rebuild_taxonomy_review.py` | Runs the full taxonomy rebuild chain (variant groups → doc appearances → XML → HTML) |
| `rebuild_mockup.py` | Runs the full mockup rebuild chain |

#### Utility & Conversion

| Script | Purpose |
|--------|---------|
| `convert_airtable_annotations.py` | Converts Airtable annotation XML exports to JSON |
| `convert_tei_annotations.py` | Converts TEI annotation markup to pipeline format |
| `suggest_categories.py` | Generates category suggestions using co-occurrence analysis and keyword heuristics |
| `apply_category_suggestions.py` | Reads approved suggestions from spreadsheet and writes to category_overrides |
| `patch_metadata.py` | Patches missing document metadata from split TEI files |
| `patch_metadata_v2.py` | Updated metadata patching logic |
| `generate_review.py` | Generates review summary data |
| `generate_summary.py` | Generates pipeline summary statistics |
| `generate_documentation.js` | Generates documentation (Node.js) |
| `import_volume.py` | Imports a new volume into the pipeline |
| `batch_split_and_annotate.py` | Bulk split and annotate all remaining volumes (loads resources once) |

### Editorial Decision Files (`config/`)

| File | Purpose |
|------|---------|
| `dedup_decisions.json` | Near-duplicate merge/skip decisions from dedup-review.html |
| `semantic_dedup_decisions.json` | Semantic duplicate decisions from semantic-dedup-review.html |
| `category_overrides.json` | Manual category assignment overrides |
| `variant_overrides.json` | Manual variant group split/merge overrides |
| `annotation_stoplist.json` | Terms excluded from string matching (too generic) |
| `annotation_rejections_*.json` | Per-volume annotation accept/reject decisions |
| `lcsh_mapping.json` | Full LCSH match data for all subjects |
| `lcsh_mapping_clean.json` | Curated subset of high-quality LCSH matches |
| `lcsh_review.tsv` | LCSH review status spreadsheet |
| `hsg_only_subjects.json` | Subjects unique to HSG (not in LCSH) |
| `hsg_variant_names.json` | HSG-specific variant name forms |

### Source TEI Volumes (`volumes/`)

The `frus*.xml` files are monolithic TEI/XML volumes used as input for annotation. These are copies from the [HistoryAtState/frus-history-data](https://github.com/HistoryAtState) repositories. All 560 volumes in the FRUS series are loaded; 551 have been split into individual documents and annotated via string matching against the taxonomy.

### Annotations (`annotations/`)

Extracted annotation XML files for the 11 volumes with curated annotations (1969–1988 subseries).

### XQuery Files (`queries/`)

XQuery scripts for running pipeline steps directly in an XQuery processor. The top-level files target **BaseX**; the `exist-db/` subdirectory contains adapted versions for **eXist-db** (swapping the I/O layer while preserving all business logic).

| File | Purpose |
|------|---------|
| `annotations-extraction-fast.xq` | Extracts annotations from TEI volumes via eXist-db |
| `annotations_to_airtable.xq` | Pushes extracted annotations to Airtable |
| `extract-doc-appearances.xq` | Extracts document appearances from annotated TEI volumes |
| `extract-existing-annotations.xq` | Extracts pre-existing `<rs>` annotations from TEI volumes |
| `merge-annotations.xq` | Merges annotated documents back into TEI volumes |
| `apply-curated-annotations.xq` | Applies reviewed annotations to TEI documents |
| `build-taxonomy-lcsh.xq` | Builds the subject taxonomy XML with HSG topic categorization |
| `lcsh-mapper.xq` | Maps subjects to LCSH via the id.loc.gov suggest2 API |
| `split-volume.xq` | Splits a monolithic TEI volume into individual document files |

### Shared State File

`taxonomy_review_state.json` is the central state file shared by all review tools. It stores:

| Field | Purpose |
|-------|---------|
| `lcsh_decisions` | Accept/reject decisions for LCSH mappings |
| `category_overrides` | Manual category/subcategory reassignments |
| `merge_decisions` | Term merge decisions (source ref → target ref/name) |
| `exclusions` | Terms excluded from the taxonomy |
| `global_rejections` | Terms rejected across all volumes (set in annotation review) |
| `candidate_decisions_persons` | Review decisions for person candidates |
| `candidate_decisions_organizations` | Review decisions for organization candidates |
| `candidate_decisions_topics` | Review decisions for topic candidates |
| `reassigned_to_*` | Candidates reassigned across categories |

All tools use a read-modify-write pattern when saving, so they preserve fields set by other tools. The mockup rebuild pipeline reads this file to apply exclusions and merges before generating output, including resolution of merge chains (A→B→C becomes A→C).

### Browser-Based Review Tools

Generated by the build scripts (not committed; regenerate with `make` targets):

| Tool | Build command | Purpose |
|------|---------------|---------|
| `string-match-review.html` | `make review` | Review annotation matches per volume; includes pipeline action buttons |
| `taxonomy-review.html` | `make taxonomy-review` | Review LCSH mappings, reassign categories, merge terms, exclude entries |
| `candidates-review-persons.html` | `python3 scripts/build_candidates_review.py --category persons` | Review person name candidates |
| `candidates-review-organizations.html` | `python3 scripts/build_candidates_review.py --category organizations` | Review organization/country candidates |
| `candidates-review-topics.html` | `python3 scripts/build_candidates_review.py --category topics` | Review topic/event/treaty candidates |
| `candidates-review.html` | `make candidates-review` | Combined candidate review (legacy) |
| `hsg-subjects-mockup.html` | `make mockup` | Interactive mockup of the taxonomy on history.state.gov |
| `dedup-review.html` | — | Review near-duplicate subject groups |
| `semantic-dedup-review.html` | — | Review semantic duplicate groups |

The category-specific candidate review tools support **cross-category reassignment** — entries can be moved between persons, organizations, and topics queues, with the reassigned entry appearing in the target category's tool on next load.

## Pipeline Workflow

### 1. Annotation Pipeline (per volume)

```
Split volume → Annotate documents → Review annotations → Apply decisions
    → Merge annotations → Extract appearances → Build taxonomy XML
```

### 2. Discovery Pipeline (corpus-wide)

```
Discover index terms (Tier 2) → Discover LCSH terms (Tier 3)
    → Split into category queues → Review candidates → Promote accepted → Rebuild taxonomy
```

### 3. Adding a New Volume

1. **Place the volume**: `cp frusNEW.xml volumes/`
2. **Split**: `make split`
3. **Build variant groups**: `python3 scripts/build_variant_groups.py`
4. **Annotate**: `python3 scripts/annotate_documents.py data/documents/<volume-id>`
5. **Review**: `make serve` → open `string-match-review.html`
6. **Run pipeline**: `make pipeline VOL=<volume-id>` (or click **Run Pipeline** in the review tool)
7. **Review taxonomy**: open `taxonomy-review.html`

## Propagating Review Decisions

1. **Taxonomy → Annotation Review**: Merge decisions and exclusions made in the taxonomy review tool are automatically loaded by the annotation review tool when a volume is opened. Taxonomy merges appear with a purple "(from taxonomy review)" label.

2. **Annotation → Taxonomy Review**: Global rejections set in the annotation review tool are preserved when the taxonomy review tool saves (read-modify-write pattern).

3. **Candidate Review → Taxonomy**: Run `make promote` to inject accepted candidates into the taxonomy pipeline and rebuild the taxonomy review.

4. **Cross-category reassignment**: Candidates can be reassigned between person/organization/topic queues. Reassigned entries are saved to the target category's state key and appear in its review tool on next load.

5. **All Tools → Mockup**: Click **Rebuild Data** in the mockup, or run `make mockup`.

6. **All Tools → Taxonomy XML + Annotated Volumes**: Click **Run Pipeline** in the annotation review tool, or run `make pipeline VOL=<volume-id>`.

Note: The taxonomy XML (`subject-taxonomy-lcsh.xml`) is intentionally kept as the unfiltered source of truth. It always contains every subject, including those that have been excluded or merged. Editorial decisions are applied at build time by downstream scripts.

## Development Server

The Flask development server (`serve.py`) provides:

- Static file serving with cache-busting headers (no-cache)
- SSE (Server-Sent Events) streaming for pipeline action buttons
- Auto-reload on Python file changes
- API endpoints for saving/loading review decisions
- Automatic backup system: creates timestamped copies in `config/backups/` before overwriting any config file (keeps 20 most recent per file)

Start with `make serve` (default port 9090). API endpoints:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/validate` | POST | Run data validation (SSE stream) |
| `/api/pipeline/<volume-id>` | POST | Run post-review pipeline (SSE stream) |
| `/api/rebuild-review` | POST | Rebuild string-match-review.html (SSE stream) |
| `/api/rebuild-taxonomy-review` | POST | Rebuild taxonomy chain + HTML (SSE stream) |
| `/api/rebuild-mockup` | POST | Rebuild mockup chain + HTML (SSE stream) |
| `/api/save-taxonomy-decisions` | POST | Save review decisions (all tools) |
| `/api/load-taxonomy-decisions` | GET | Load saved review decisions |

## Data Integrity

The annotation data in `data/documents/*/string_match_results_*.json` is never modified by merge or exclude operations. These files are read-only records of where terms were found in the source documents. Merge and exclude decisions are layered on top at display/build time, so original annotation provenance is always preserved.

The taxonomy XML (`subject-taxonomy-lcsh.xml`) serves as the unfiltered source of truth. It is regenerated from raw data by `build_taxonomy_lcsh.py` and always contains every subject. Editorial decisions (exclusions, merges, category overrides) are stored in `taxonomy_review_state.json` and applied at build time by downstream scripts.

## Tests

```bash
python3 -m pytest tests/
```

## Documentation

See `docs/FRUS-Subject-Taxonomy-Documentation.docx` for the full technical documentation covering the pipeline architecture, data formats, and editorial workflow.
