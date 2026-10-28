"""PDF-side brochure extraction, split out of ``brochure_extract`` (P17C.4).

archive        durable source-PDF archive, BrochureYMM / BrochureExtractResult
pdf_text       page text + layout capture, brochure_text JSON payload and persist
matrix_parse   equipment-matrix and buyer's-guide parsing, ladder mapping
overlay_store  extract_brochure_pdf, overlay/facts persistence, overlay load

``backend.enrichment.brochure_extract`` stays the import path for every consumer.
Patch a name on the submodule that owns it, not on the facade.
"""
