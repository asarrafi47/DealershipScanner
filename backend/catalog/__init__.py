"""
Vehicle catalog: ``epa_master`` (+ ``epa_extended_specs``) formalized as the
canonical (year, make, model, trim) → engine-variant reference store, plus the
``model_generations`` table. See ``docs/data_architecture_plan.md``.

Listing rows link to the catalog via ``cars.epa_master_id`` (written only by
``backend.catalog.resolver``); model-level facts are joined at read time and
never copied into ``cars``.
"""
