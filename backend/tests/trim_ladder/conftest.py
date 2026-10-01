"""Shared fixtures for the trim-ladder tests (split from the former test_trim_ladder.py)."""

from __future__ import annotations

import pytest

# --- the rung-name gate, and why most of this package runs with it off -------
#
# ``TRIM_RUNGS_REQUIRE_PROVENANCE`` (default ON in production) drops any rung we
# cannot justify from an active listing of that exact year/make/model or from a
# verified brochure citation. Measured on the live fleet on 2026-08-01, that is
# 71,537 → 59,396 of 73,255 active cars still showing a ladder at all.
#
# Most tests here were written before that gate and assert things about BULLET
# text, rung ORDER, name cleaning and cross-model plausibility. They cannot run
# with it on, for a reason that has nothing to do with what they test: the
# conftest fixture ``_inventory_sqlite_tests_mode`` deletes
# ``INVENTORY_DATABASE_URL`` and points every test at the local SQLite inventory,
# whose ``cars`` table is EMPTY. With no active listings there is no evidence for
# any rung, so with the gate on all 85 of them correctly resolve to None and none
# of them reaches the logic it is about.
#
# So this package turns the gate off by default and tests the gate itself in
# test_rung_name_gate.py, which opts back in via the ``rung_gate_on`` fixture —
# stubbing the evidence, and in ``test_the_query_really_reads_active_rows``
# seeding a throwaway inventory so the SQL itself is exercised. Softening the
# gate to make the legacy tests pass was the alternative and was not taken.
#
# The gate's evidence at fleet scale is the measurement in the change report,
# taken against the live Postgres inventory, not this file.
#
# Scope is exactly what it was in the single file: EVERY test in this package
# runs with the gate off unless it requests ``rung_gate_on``.


@pytest.fixture
def rung_gate_on() -> bool:
    """Request this fixture to run a test with the rung-name gate at its default (ON)."""
    return True


@pytest.fixture(autouse=True)
def _legacy_rung_gate_off(request, monkeypatch) -> None:
    if "rung_gate_on" in request.fixturenames:
        return
    monkeypatch.setenv("TRIM_RUNGS_REQUIRE_PROVENANCE", "0")
