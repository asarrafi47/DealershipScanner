"""Importing ``enrich_from_dictionary`` must not mutate the process.

``backend/dictionary/epa_engine.py`` imports ``_load_epa_csv`` / ``_best_row`` /
``_is_empty`` from this module lazily, inside ``resolve_engine_display_from_epa``
-- which runs on ordinary car serialization (web app, scanner post-processing,
``listing_missing_field_codes``). The module used to run ``os.chdir()``,
``sys.path.insert()``, ``load_project_dotenv()`` and ``logging.basicConfig()`` at
import time, so the first car serialized in a process silently changed the
working directory, installed a root log handler, and re-injected ``.env`` over
the live environment.

The ``.env`` re-injection was the one with teeth: ``backend/tests/conftest.py``
clears ``INVENTORY_DATABASE_URL`` so tests run against SQLite, and this import
put it straight back -- after which every DB call in that pytest process wrote
to the real Postgres inventory.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_probe(cwd: Path) -> dict[str, str]:
    code = textwrap.dedent(
        """
        import json, logging, os, sys
        before_cwd = os.getcwd()
        before_env = os.environ.get("INVENTORY_DATABASE_URL")
        before_handlers = len(logging.getLogger().handlers)
        import backend.dictionary.enrich_from_dictionary  # noqa: F401
        print(json.dumps({
            "before_cwd": before_cwd,
            "after_cwd": os.getcwd(),
            "before_env": before_env,
            "after_env": os.environ.get("INVENTORY_DATABASE_URL"),
            "before_handlers": before_handlers,
            "after_handlers": len(logging.getLogger().handlers),
        }))
        """
    )
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(Path.home()),
        "PYTHONPATH": str(REPO_ROOT),
    }
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    import json

    return json.loads(out.stdout.strip().splitlines()[-1])


def test_import_does_not_chdir_or_reload_dotenv(tmp_path: Path) -> None:
    result = _run_probe(tmp_path)
    assert result["after_cwd"] == result["before_cwd"], (
        "importing enrich_from_dictionary changed the process working directory "
        f"({result['before_cwd']} -> {result['after_cwd']})"
    )
    assert result["after_env"] == result["before_env"], (
        "importing enrich_from_dictionary re-loaded .env and set "
        "INVENTORY_DATABASE_URL on a process that had it unset"
    )
    assert result["after_handlers"] == result["before_handlers"], (
        "importing enrich_from_dictionary called logging.basicConfig() and "
        "installed a root log handler"
    )
