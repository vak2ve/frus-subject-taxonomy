# FRUS Subject Taxonomy Pipeline

A pipeline for building and maintaining a subject taxonomy for the *Foreign Relations of the United States* (FRUS) series, with LCSH (Library of Congress Subject Headings) integration. Developed at the Office of the Historian, U.S. Department of State.

## Overview

This project extracts subject annotations from FRUS TEI/XML volumes, consolidates duplicate and variant terms, maps subjects to LCSH authority records, and produces a structured taxonomy in XML. It includes browser-based review tools for editorial quality control at every stage: deduplication, annotation review, LCSH matching, and taxonomy organization.

The current proof-of-concept covers 21 volumes from the 1969--1988 subseries. The full FRUS corpus comprises 538 volumes.

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

# Validate data integrity across all volumes
make validate
```

## Repository Structure

```
├── Makefile              Single entry point for setup and pipeline operations
├── serve.py              Flask development server with SSE streaming + API
├── scripts/              Pipeline scripts (Python + JS)
├── volumes/              Source TEI/XML volumes (frus*.xml)
├── annotations/          Annotation output XML files
├── data/documents/       Per-volume split document files + annotation results
├── progress/             Pipeline progress tracking XML
├── config/               Editorial decisions and reference data
├── queries/              XQuery files (BaseX)
│   └── exist-db/         eXist-db adapted versions
├── docs/                 Documentation
└── subject-taxonomy-lcsh.xml   The main taxonomy output
```

### Pipeline Scripts (`scripts/`)

| Script | Purpose |
|--------|---------|
| `build_variant_groups.py` | Consolidates variant name forms from dedup decisions, semantic dedup, LCSH URI overlaps, and manual overrides |
| `annotate_documents.py` | String-match annotation engine with stoplist and variant support |
| `extract_existing_annotations.py` | Extracts pre-existing `<rs>` annotations from TEI volumes |
| `apply_curated_annotations.py` | Applies reviewed annotations to TEI documents, removing rejected matches |
| `apply_review_decisions.py` | Applies exported review decisions (rejections, merges, LCSH) to results JSON |
| `build_taxonomy_lcsh.py` | Builds the taxonomy XML with LCSH broader-term hierarchies |
| `lcsh_mapper.py` | Queries id.loc.gov for LCSH matching |
| `merge_annotations.py` | Merges annotation data across volumes |
| `merge_annotations_to_appearances.py` | Merges annotations into document_appearances.json |
| `extract_doc_appearances.py` | Extracts document appearances from annotated XML |
| `generate_mockup_data.py` | Generates JSON data for the interactive mockup; applies exclusions, merges, and category overrides from `taxonomy_review_state.json` |
| `build_mockup_html.py` | Builds the self-contained HTML mockup |
| `build_annotation_review.py` | Builds the annotation review HTML tool |
| `build_taxonomy_review.py` | Builds the taxonomy/LCSH review HTML tool |
| `rebuild_taxonomy_review.py` | Runs the full taxonomy rebuild chain (variant groups → doc appearances → XML → HTML) |
| `rebuild_mockup.py` | Runs the full mockup rebuild chain (variant groups → doc appearances → XML → mockup data → HTML) |
| `suggest_categories.py` | Generates category suggestions for uncategorized subjects using co-occurrence analysis and keyword heuristics |
| `apply_category_suggestions.py` | Reads approved suggestions from spreadsheet and writes to `category_overrides.json` and `taxonomy_review_state.json` |
| `run_reviewed_pipeline.py` | Orchestrates the full 8-step post-review pipeline |
| `split_volume.py` | Splits a monolithic TEI volume into per-document files |
| `validate_data.py` | Validates data integrity across all volumes |
| `patch_metadata.py` | Patches missing document metadata from split TEI files |
| `generate_review.py` | Generates review summary data |
| `apply_annotations.py` | Legacy annotation application script |

### Editorial Decision Files (`config/`)

| File | Purpose |
|------|---------|
| `dedup_decisions.json` | Near-duplicate merge/skip decisions from dedup-review.html |
| `semantic_dedup_decisions.json` | Semantic duplicate decisions from semantic-dedup-review.html |
| `category_overrides.json` | Manual category assignment overrides (also written by `apply_category_suggestions.py`) |
| `variant_overrides.json` | Manual variant group split/merge overrides |
| `annotation_stoplist.json` | Terms excluded from string matching (too generic) |
| `annotation_rejections_*.json` | Per-volume annotation accept/reject decisions |
| `lcsh_mapping.json` | Full LCSH match data for all subjects |
| `lcsh_mapping_clean.json` | Curated subset of high-quality LCSH matches |
| `lcsh_review.tsv` | LCSH review status spreadsheet |
| `backups/` | Timestamped backup copies of config files (20 most recent per file) |

### Source TEI Volumes (`volumes/`)

The `frus*.xml` files are monolithic TEI/XML volumes used as input for annotation. These are copies from the [HistoryAtState/frus-history-data](https://github.com/HistoryAtState) repositories.

### Annotations (`annotations/`)

Extracted annotation XML files produced by the XQuery extraction pipeline.

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

#### eXist-db Versions (`queries/exist-db/`)

Each file below is a direct adaptation of the corresponding BaseX query above, using eXist-db native modules (`xmldb`, `util`) instead of the EXPath `file:` module, and `util:wait()` instead of `prof:sleep()`. Data is read from and written to eXist-db collections (default: `/db/apps/hsg-annotate-data`).

| File | Purpose |
|------|---------|
| `extract-doc-appearances.xq` | Extracts document appearances (eXist-db) |
| `extract-existing-annotations.xq` | Extracts pre-existing `<rs>` annotations (eXist-db) |
| `merge-annotations.xq` | Merges annotated documents into TEI volumes (eXist-db) |
| `apply-curated-annotations.xq` | Applies curated annotations to TEI documents (eXist-db) |
| `build-taxonomy-lcsh.xq` | Builds subject taxonomy with HSG categorization (eXist-db) |
| `lcsh-mapper.xq` | Maps subjects to LCSH via id.loc.gov API (eXist-db) |
| `split-volume.xq` | Splits a monolithic TEI volume into document resources (eXist-db) |

### Shared State File

`taxonomy_review_state.json` is the central state file shared by both the taxonomy review and annotation review tools. It stores:

| Field | Purpose |
|-------|---------|
| `lcsh_decisions` | Accept/reject decisions for LCSH mappings |
| `category_overrides` | Manual category/subcategory reassignments |
| `merge_decisions` | Term merge decisions (source ref → target ref/name) |
| `exclusions` | Terms excluded from the taxonomy |
| `global_rejections` | Terms rejected across all volumes (set in annotation review) |

Both tools use a read-modify-write pattern when saving, so they preserve fields set by the other tool. The mockup rebuild pipeline (`rebuild_mockup.py`) reads this file to apply exclusions and merges before generating output, including resolution of merge chains (A→B→C becomes A→C).

### Browser-Based Review Tools

Generated by the build scripts (not committed; regenerate with `make review` and `make taxonomy-review`):

- **string-match-review.html** -- Review annotation matches per volume; includes pipeline action buttons (Validate, Run Pipeline, Rebuild, Taxonomy Review). Supports "Reject in all volumes" for global rejections and "Exclude from taxonomy" to hide terms. Taxonomy merge decisions flow in automatically.
- **taxonomy-review.html** -- Review taxonomy organization, accept/reject LCSH mappings, reassign categories, merge terms, and exclude entries. Action buttons (Reassign, Merge, Exclude) appear at the top of each entry. Auto-saves decisions to the server.
- **dedup-review.html** -- Review near-duplicate subject groups
- **semantic-dedup-review.html** -- Review semantic duplicate groups
- **hsg-subjects-mockup.html** -- Interactive mockup of the taxonomy as it would appear on history.state.gov. Includes a "Rebuild Data" button that runs the full mockup pipeline, applying all exclusions, merges, and category overrides from `taxonomy_review_state.json`.

## Adding a New Volume

To add a new unannotated FRUS volume and assign string-match annotations:

### 1. Place the monolithic TEI/XML volume

Copy the volume file into `volumes/`:

```bash
cp frusNEW-VOLUME.xml volumes/
```

### 2. Split into individual documents

Split the volume into per-document files. The Makefile handles this automatically for all volumes in `volumes/`:

```bash
make split
```

Or split a single volume manually:

```bash
python3 scripts/split_volume.py <volume-id>
```

Alternatively, use XQuery directly:

```bash
# BaseX
basex -b volume-id=<volume-id> queries/split-volume.xq

# eXist-db (after uploading the volume to the volumes collection)
# Run queries/exist-db/split-volume.xq via eXide with $volume-id set
```

This produces per-document files in `data/documents/<volume-id>/` (`d1.xml`, `d2.xml`, etc.).

### 3. Build variant groups

Consolidate variant name forms so the annotator maps variants to canonical terms:

```bash
python3 scripts/build_variant_groups.py
```

This reads from `config/dedup_decisions.json`, `config/semantic_dedup_decisions.json`, `config/lcsh_mapping.json`, and `config/variant_overrides.json` to produce `variant_groups.json`.

### 4. Run string-match annotation

```bash
python3 scripts/annotate_documents.py data/documents/<volume-id>
```

This produces `string_match_results_<volume-id>.json` in the volume's data directory, containing all matches with sentence context, positions, and term metadata.

### 5. Review annotations

```bash
make serve
```

Open `http://localhost:9090/string-match-review.html` in a browser, select the volume, review and accept/reject matches. The review tool includes pipeline action buttons:

- **Validate** — check data integrity across all volumes
- **Run Pipeline** — apply review decisions and rebuild taxonomy for the current volume
- **Rebuild** — regenerate string-match-review.html with the latest data
- **Taxonomy Review →** — rebuild and open the taxonomy review tool in a new tab

### 6. Run the post-review pipeline

Either click **Run Pipeline** in the annotation review tool, or run from the command line:

```bash
make pipeline VOL=<volume-id>
```

This runs the full 8-step pipeline: applies review decisions, rebuilds variant groups, applies curated annotations to the TEI documents, merges them into the volume, extracts document appearances, rebuilds the taxonomy XML, and regenerates the mockup.

### 7. Review taxonomy

Click **Taxonomy Review →** in the annotation review tool, or:

```bash
make taxonomy-review
```

Open `http://localhost:9090/taxonomy-review.html` to review LCSH mappings (accept/reject), reassign categories, and search across all subjects. Decisions auto-save to the server and can be exported as `lcsh_decisions.json`.

## Propagating Review Decisions

All review decisions are stored in `taxonomy_review_state.json`, which serves as the shared state between tools. Decisions propagate as follows:

1. **Taxonomy → Annotation Review**: Merge decisions and exclusions made in the taxonomy review tool are automatically loaded by the annotation review tool when a volume is opened. Taxonomy merges appear with a purple "(from taxonomy review)" label and cannot be undone from the annotation review.

2. **Annotation → Taxonomy Review**: Global rejections set in the annotation review tool are preserved when the taxonomy review tool saves (read-modify-write pattern).

3. **Both Tools → Mockup**: Click **Rebuild Data** in the mockup, or run `python3 scripts/rebuild_mockup.py`. The rebuild pipeline reads `taxonomy_review_state.json` and applies all exclusions, merges (including merge chains), and category overrides before generating the mockup output.

4. **Both Tools → Taxonomy XML + Annotated Volumes**: Click **Run Pipeline** in the annotation review tool, or run `make pipeline VOL=<volume-id>` from the command line.

Note: The taxonomy XML (`subject-taxonomy-lcsh.xml`) is intentionally kept as the unfiltered source of truth. It always contains every subject, including those that have been excluded or merged. Editorial decisions are applied at build time by downstream scripts.

## Development Server

The Flask development server (`serve.py`) provides:

- Static file serving with cache-busting headers (no-cache)
- SSE (Server-Sent Events) streaming for pipeline action buttons
- Auto-reload on Python file changes
- API endpoints for saving/loading taxonomy review decisions
- Automatic backup system: creates timestamped copies in `config/backups/` before overwriting any config file, keeping the 20 most recent per file

Start with `make serve` (default port 9090). API endpoints:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/validate` | POST | Run data validation (SSE stream) |
| `/api/pipeline/<volume-id>` | POST | Run post-review pipeline (SSE stream) |
| `/api/rebuild-review` | POST | Rebuild string-match-review.html (SSE stream) |
| `/api/rebuild-taxonomy-review` | POST | Rebuild taxonomy chain + HTML (SSE stream) |
| `/api/rebuild-mockup` | POST | Rebuild mockup chain + HTML (SSE stream) |
| `/api/save-taxonomy-decisions` | POST | Save LCSH, category, merge, exclusion, and global rejection decisions |
| `/api/load-taxonomy-decisions` | GET | Load saved taxonomy decisions |

## Data Integrity

The annotation data in `data/documents/*/string_match_results_*.json` is never modified by merge or exclude operations. These files are read-only records of where terms were found in the source documents. Merge and exclude decisions are layered on top at display/build time, so original annotation provenance is always preserved.

The taxonomy XML (`subject-taxonomy-lcsh.xml`) serves as the unfiltered source of truth. It is regenerated from raw data by `build_taxonomy_lcsh.py` and always contains every subject. Editorial decisions (exclusions, merges, category overrides) are stored in `taxonomy_review_state.json` and applied at build time by downstream scripts like `generate_mockup_data.py`.

## Documentation

See `docs/FRUS-Subject-Taxonomy-Documentation.docx` for the full technical documentation covering the pipeline architecture, data formats, and editorial workflow.
