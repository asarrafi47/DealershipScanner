# P10B.4 runbook: local provenance backfill (owner-gated)

Local database only (`cars`). Never `cars_cleanroom` or `cars_cr_*`. No Railway. Preconditions:
V036 applied locally (P10A.3b), the root `DICTIONARY/` folder still present (P10D.8 deletes it;
the legacy class is classified from it), every scanner shard and mini job stopped.

1. Plan, then back up (pg_dump of epa_master / epa_extended_specs / epa_master_dump_id_map,
   `cars_epa_link_snapshot_<date>`, `rollback.sql`):

       workspace/phase_runs/N23/p10b4_prepare.sh            # prints the plan
       workspace/phase_runs/N23/p10b4_prepare.sh --run      # executes steps 1-3, writes rollback.sql

2. Dry run (read-only session). Expect the P10A.6 classification, 0 unmatched, no collisions:

       PYTHONPATH=. .venv/bin/python backend/scripts/backfill_epa_master_provenance.py

   Baseline counts from the 2026-10 draft: 70,496 rows classified, 0 unmatched. Paste the JSON
   into `workspace/dealer_logs/_learning/` or the checkpoint and ask the owner to OK the counts.

3. After the owner OK only:

       PYTHONPATH=. .venv/bin/python backend/scripts/backfill_epa_master_provenance.py --apply

   One transaction, refuses while another session is active, asserts per-source counts and that
   vid / atv_type / city_e were not nulled. A re-run is a no-op.

4. Verify: the `--apply` JSON shows the classified counts; `SELECT source, COUNT(*) FROM
   epa_master GROUP BY 1` matches the dry run; `SELECT COUNT(*) FROM epa_master WHERE
   natural_key IS NULL` is 0; cars with epa_master_id unchanged vs the snapshot:

       SELECT COUNT(*) FROM cars c JOIN cars_epa_link_snapshot_<date> s ON s.car_id = c.id
       WHERE c.epa_master_id IS DISTINCT FROM s.epa_master_id;   -- 0

5. Rollback: `psql -h /tmp -d cars -f <out>/rollback.sql`, or `pg_restore --clean --if-exists`
   of the relevant dump. Record the invariant baselines (P10B.7) only after step 4 passes.
