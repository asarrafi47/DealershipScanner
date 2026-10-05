"""Named steps of the car detail (VDP) view-context pipeline.

``backend.routes.cars_pages._build_car_detail_view_context`` is the public entry
point (same name, same signature, same import path as before the split); it is a
short orchestrator that calls these steps in the original order:

* :mod:`.viewer`          — record the view, saved-car flag
* :mod:`.sticker`         — window-sticker eligibility / URLs / rendered status
* :mod:`.listing`         — serialize the car, incomplete fields, registry dealer,
                             attribution + sticker-fact (MSRP / color) overlays
* :mod:`.insights`        — market price + deal score (paid), trim ladder, rarity
* :mod:`.location`        — dealer map, hero location line
* :mod:`.options`         — generated build sheet, unified Options-tab list
* :mod:`.assemble`        — the final context dict (key order preserved)
* :mod:`.packages_ensure` — the process-local packages/sticker fetch job manager

Steps import their helpers from the owning modules (tests patch them on the
step module that looks them up). Heavy imports stay function-local, exactly as
they were.
"""
