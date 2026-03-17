# FRUS Subject Taxonomy Pipeline
# ================================
# Single entry point for the annotation pipeline.
#
# Quick start:
#   make setup    — install dependencies + build everything
#   make serve    — start the dev server on port 9090
#
# Individual targets:
#   make deps     — install Python dependencies
#   make split    — split volume XMLs into per-document files
#   make convert  — convert Airtable annotation XMLs to JSON
#   make review   — build the annotation review HTML
#   make validate — check data integrity
#   make clean    — remove generated files
#
# Post-review (after reviewing annotations in the browser):
#   make pipeline VOL=frus1969-76v19p2

.PHONY: setup deps split convert review taxonomy-review validate serve clean pipeline help

PYTHON ?= python3
PORT   ?= 9090
SCRIPTS = scripts

# ── Composite targets ────────────────────────────────────

help:
	@echo ""
	@echo "FRUS Subject Taxonomy Pipeline"
	@echo "=============================="
	@echo ""
	@echo "  make setup        Install deps + split + convert + build review tool"
	@echo "  make serve        Start dev server on port $(PORT)"
	@echo "  make validate     Check data integrity across all volumes"
	@echo "  make pipeline VOL=<id>  Run full post-review pipeline for a volume"
	@echo "  make clean        Remove generated files"
	@echo ""
	@echo "Individual steps:"
	@echo "  make deps         Install Python dependencies"
	@echo "  make split        Split volume XMLs into per-document files"
	@echo "  make convert      Convert Airtable annotations to JSON"
	@echo "  make review       Build string-match-review.html"
	@echo "  make taxonomy-review  Build taxonomy-review.html"
	@echo ""

setup: deps split convert review
	@echo ""
	@echo "============================================================"
	@echo "  Setup complete!"
	@echo ""
	@echo "  Start the server:"
	@echo "    make serve"
	@echo ""
	@echo "  Then open:"
	@echo "    http://localhost:$(PORT)/string-match-review.html"
	@echo "============================================================"

# ── Dependencies ─────────────────────────────────────────

deps:
	@echo "Installing Python dependencies..."
	$(PYTHON) -m pip install --quiet flask lxml
	@echo "  Done."

# ── Split volumes ────────────────────────────────────────
# Splits each volume XML in volumes/ into individual document
# files under data/documents/<vol>/d*.xml.
# Skips volumes that are already split.

split:
	@echo "Splitting volume XMLs..."
	@for vol in volumes/*.xml; do \
		vol_id=$$(basename "$$vol" .xml); \
		doc_dir="data/documents/$$vol_id"; \
		if [ -d "$$doc_dir" ] && ls "$$doc_dir"/d*.xml >/dev/null 2>&1; then \
			echo "  $$vol_id: already split, skipping"; \
		else \
			echo "  $$vol_id: splitting..."; \
			$(PYTHON) $(SCRIPTS)/split_volume.py "$$vol_id"; \
		fi \
	done
	@echo "  Done."

# ── Convert Airtable annotations ────────────────────────
# Converts annotations/*.xml → data/documents/<vol>/string_match_results_<vol>.json
# Skips volumes that already have results.

convert:
	@echo "Converting Airtable annotations..."
	@for ann in annotations/annotations_*.xml; do \
		vol_id=$$(basename "$$ann" .xml | sed 's/annotations_//'); \
		result="data/documents/$$vol_id/string_match_results_$$vol_id.json"; \
		if [ -f "$$result" ]; then \
			echo "  $$vol_id: results exist, skipping (use 'make reconvert' to force)"; \
		else \
			echo "  $$vol_id: converting..."; \
			$(PYTHON) $(SCRIPTS)/convert_airtable_annotations.py "$$vol_id"; \
		fi \
	done
	@echo "  Done."

reconvert:
	@echo "Re-converting ALL Airtable annotations (overwriting existing)..."
	$(PYTHON) $(SCRIPTS)/convert_airtable_annotations.py
	@echo "  Done."

# ── Build review HTML ────────────────────────────────────

review: string-match-review.html

string-match-review.html: $(SCRIPTS)/build_annotation_review.py
	@echo "Building annotation review tool..."
	$(PYTHON) $(SCRIPTS)/build_annotation_review.py
	@echo "  Done."

# ── Build taxonomy review HTML ─────────────────────────────

taxonomy-review: taxonomy-review.html

taxonomy-review.html: $(SCRIPTS)/build_taxonomy_review.py subject-taxonomy-lcsh.xml
	@echo "Building taxonomy review tool..."
	$(PYTHON) $(SCRIPTS)/build_taxonomy_review.py
	@echo "  Done."

# ── Validate ─────────────────────────────────────────────

validate:
	$(PYTHON) $(SCRIPTS)/validate_data.py

# ── Serve ────────────────────────────────────────────────

serve: string-match-review.html taxonomy-review.html
	@echo "Starting dev server on port $(PORT)..."
	@echo "  Annotation review: http://localhost:$(PORT)/string-match-review.html"
	@echo "  Taxonomy review:   http://localhost:$(PORT)/taxonomy-review.html"
	@echo ""
	$(PYTHON) serve.py --port $(PORT)

# ── Post-review pipeline ────────────────────────────────

pipeline:
ifndef VOL
	@echo "Error: specify a volume, e.g.  make pipeline VOL=frus1969-76v19p2"
	@exit 1
endif
	$(PYTHON) $(SCRIPTS)/run_reviewed_pipeline.py $(VOL)

# ── Clean ────────────────────────────────────────────────

clean:
	@echo "Removing generated files..."
	rm -f string-match-review.html
	rm -f taxonomy-review.html
	rm -f hsg-subjects-mockup.html
	@echo "  Done. (Data files in data/ are preserved.)"
