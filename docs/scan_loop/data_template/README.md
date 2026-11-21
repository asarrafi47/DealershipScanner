# Scan loop workspace template

Not a repository any more: the loop's data lives in the Railway database (`loop_files`, migration V032), and cloud
sessions reach it through the token-guarded `/api/loop` endpoints (`backend/routes/loop_api.py`). This folder holds
the two files the tooling copies out of the public repository:

- `CLAUDE.md`: copied into the hydrated data folder so an analysis session starts with the loop's rules.
- `config/loop.json`: the default budgets and kill switch. The Railway tick seeds it into the store when the store
  has none; the owner edits the stored copy (`ops_tick.py` reads it every tick). `apply` is not in `kinds`.
- `.claude/settings.json`: permissions for an analysis session (no web access, no git write).

Layout and writers: `docs/scan_loop/BUNDLE.md` section 3. Owner steps: `docs/scan_loop/OWNER_SETUP.md`.
