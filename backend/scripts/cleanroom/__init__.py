"""Clean-room acceptance tool internals (N1.3, docs/NEXT_PHASES_PLAN_2026_10.md).

Everything here is read-only. ``db`` opens every connection with
``default_transaction_read_only=on`` and refuses a session that does not report
it. The CLI is ``backend/scripts/cleanroom_acceptance.py``.

The incomplete-listings recompute is a port of the 10-15 scratchpad scripts that
were copied to ``workspace/cleanroom_2026_10/analysis/il_scripts/``:

=================  ==========================================================
il_scripts file    where it lives now
=================  ==========================================================
recompute.py       ``recompute.run_worker`` / ``recompute.run_chunks`` (same
                   calls: ``merge_verified_specs(include_extended_specs=False)``
                   + ``listing_missing_field_codes(include_non_actionable=True)``
                   inside ``borrow_read_connection`` after ``prime_vpic_cache``;
                   chunks by ``id % n`` instead of ``md5(vin)`` offsets)
summarize.py       ``recompute.summarize`` (per-code counts and only-gap counts)
split.py           ``recompute.summarize`` (``mileage_split`` and the
                   ``*_excl_new_null_mileage`` figures)
idxcmp.py          ``recompute.stale_index_flags``
arrivals.py,       ``recompute.window_split`` (first seen <=3 d / <=7 d / older,
arr2.py, age_old   through ``first_seen_at`` and a VIN join to the reference DB)
transit.py         ``recompute.transit_split``
paired.py,         ``recompute.paired_vs_reference`` (needs the reference DB's
paired2.py         recompute in the same work dir)
dealers.py         per-dealer incomplete counts in ``signals.build_signals``
body.py            ``metrics.body_style_vocabulary``
headline.sql       ``metrics.column_scan`` (``headline``)
=================  ==========================================================
"""
