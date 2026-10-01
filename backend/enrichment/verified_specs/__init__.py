"""Per-field precedence for :func:`backend.enrichment.knowledge_engine_specs.merge_verified_specs`.

``merge_verified_specs`` is the composer; each module here owns one step:

* :mod:`.sources`       — gather every input (row clean, trim decoder, catalog
  link / per-trim / aggregate EPA, vPIC, extended specs, model_specs dictionary,
  generation) into one :class:`~.sources.SpecSources`.
* :mod:`.plausibility`  — the cross-checks that reject a source (catalog row vs
  engine text, vPIC cylinders vs engine text, dealer "Electric" label).
* :mod:`.cylinders`, :mod:`.drivetrain`, :mod:`.transmission`,
  :mod:`.electrification`, :mod:`.body_style`, :mod:`.fuel_economy` — one
  field family's priority chain each.
* :mod:`.result`        — the provenance list and the returned dict.

Every helper from ``backend.enrichment.knowledge_engine`` (and the extended-spec
resolvers on ``knowledge_engine_specs``) is looked up through its module at call
time, exactly as the pre-split function's lazy imports did, so tests that
monkeypatch those module attributes keep steering this code.
"""
