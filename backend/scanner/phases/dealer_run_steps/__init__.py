"""The named steps of one dealer scan (``phases.dealer_run.run_dealer``).

``run_dealer`` is the orchestrator; each module here is one stage of it, in run
order, all sharing one :class:`~.state.DealerRun`:

* ``state``       — URL normalisation, the result-dict schema, the shared run state
* ``session``     — the optional browser context (discovery only, ``SCANNER_ALLOW_BROWSER=1``)
* ``feed``        — recipe replay, coverage verdict, provider hint
* ``attribution`` — parse the captured payloads, the all-rows rooftop pass
* ``recovery``    — feed sufficiency and the inventory recovery chain
* ``enrich``      — VIN dedupe, sister-store filters, prefetch, galleries, vision, VIN facts
* ``persist``     — upsert, VIN-owner conflicts, scan_log
* ``after_write`` — coverage + auto-heal, registry link, rooftop disown, reconcile
* ``summary``     — the per-dealer summary line and the "Dealer complete" log
"""
