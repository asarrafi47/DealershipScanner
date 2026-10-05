"""
Brochure / specification-document acquisition lane, split out of
``backend/scripts/fetch_oem_brochures.py`` (audit 2026-10-01, datascripts.md F7).

The script is still the only CLI (same flags, same mode dispatch) and re-exports
every name it used to define. The code lives here, one module per job:

* ``paths``        -- ledger, content index and artifact locations
* ``gaps``         -- inventory gap list (``build_gap_list``, ``load_gap_list``)
* ``sources``      -- ``ArchiveResolver``, ``resolve_source`` (OEM tier first)
* ``download``     -- ``download_and_extract``
* ``corpus``       -- page-text helpers, fetch ledger, content index, corpus text audit
* ``quarantine``   -- ``quarantine_unidentified`` / ``quarantine_derived`` (moves files)
* ``audits``       -- ``archive_verify_overlap``, ``archive_audit``, ``audit_corpus``
* ``reachability`` -- per-brand probe and its report
* ``html_specs``   -- HTML specification pages and their overlays
* ``plan``         -- ``plan_and_download(args)``, the script's default mode

``backend/enrichment/brochure_sources`` is the policy/library layer underneath
(hosts, tiers, robots, identity and quality gates); this package is the lane
that drives it. Nothing is imported here so that importing one module does not
load the rest.
"""
