"""N3.11 (owner decision D-N19, 2026-10-18): "Inventory last checked <date>".

The dealer page and the car page each print one plain line of text while the
dealer's inventory is stale: the newest ``cars.scraped_at`` over that dealer's
rows (the car page uses its dealer's value), hidden when that value is under
48 hours old or missing. The owner's design rule is hard: no card, box, pill or
bordered container. The line is a ``<p>`` with an existing muted-text class,
directly inside the section that already holds the dealer's inventory counts
(dealer page) or the hero (car page).

Covers the rule, the repository read (SQLite here; Postgres when
``LAST_CHECKED_PG_DSN`` names a server where a throwaway ``pytest_*`` database
may be created and dropped), and both states on both pages against the real
current UTC time.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pytest

from backend.utils.inventory_last_checked import (
    FRESH_WINDOW,
    dealer_last_checked_label,
    last_checked_label,
)

_ROOT = Path(__file__).resolve().parents[2]
LINE = "Inventory last checked"
DEALER = "lastcheck-motors-com"
OTHER = "fresh-toyota-com"
_NOW = datetime(2026, 10, 22, 18, 0, tzinfo=timezone.utc)

# The existing muted-text class each page reuses, and the element that already
# holds the line (a wrapper added for the line would show up as its parent).
_DEALER_CLASS = "dealer-inventory__unconfirmed"
_CAR_CLASS = "car-detail-location-note"
_BOX_WORDS = ("card", "box", "pill", "badge", "chip", "panel", "tag", "banner", "callout", "alert")


def _iso(dt: datetime) -> str:
    """The scanner's ``scraped_at`` text shape (isoformat + "Z", see upsert/guard.py)."""
    return dt.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"


def _day(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return f"{dt:%b} {dt.day}, {dt.year}"


def _real_now() -> datetime:
    return datetime.now(timezone.utc)


# ── the rule ──────────────────────────────────────────────────────────────────


def test_fresh_window_is_48_hours():
    assert FRESH_WINDOW == timedelta(hours=48)


def test_stale_value_prints_its_date():
    assert last_checked_label("2026-09-28T22:59:34.864058Z", now=_NOW) == "Sep 28, 2026"
    assert last_checked_label("2026-10-02T00:00:00Z", now=_NOW) == "Oct 2, 2026"


def test_value_under_48_hours_hides_the_line():
    assert last_checked_label(_iso(_NOW - timedelta(hours=2)), now=_NOW) is None
    assert last_checked_label(_iso(_NOW - FRESH_WINDOW + timedelta(seconds=1)), now=_NOW) is None
    # At exactly 48 h the value is no longer "under 48 h", so the line shows.
    assert last_checked_label(_iso(_NOW - FRESH_WINDOW), now=_NOW) == "Oct 20, 2026"


def test_future_value_hides_the_line():
    assert last_checked_label(_iso(_NOW + timedelta(days=3)), now=_NOW) is None


@pytest.mark.parametrize("value", [None, "", "   ", "not a date", "2026-13-45"])
def test_missing_or_unreadable_value_hides_the_line(value):
    assert last_checked_label(value, now=_NOW) is None


def test_other_stored_shapes_read_the_same_way():
    # A naive value is UTC (the column has no zone); a datetime works too, in
    # case a backend ever hands one back.
    assert last_checked_label("2026-09-28 22:59:34", now=_NOW) == "Sep 28, 2026"
    assert last_checked_label(datetime(2026, 9, 28, 22, 59), now=_NOW) == "Sep 28, 2026"
    # The printed date is the UTC date.
    assert last_checked_label("2026-09-28T23:30:00-05:00", now=_NOW) == "Sep 29, 2026"


def test_default_clock_is_the_real_current_utc_time():
    stale = _real_now() - timedelta(days=5)
    assert last_checked_label(_iso(stale)) == _day(stale)
    assert last_checked_label(_iso(_real_now() - timedelta(hours=1))) is None


def test_dealer_label_never_raises(monkeypatch):
    import backend.db.inventory_db as inv

    def broken(_dealer_id):
        raise RuntimeError("database is down")

    monkeypatch.setattr(inv, "dealer_last_scraped_at", broken)
    assert dealer_last_checked_label(DEALER) is None
    assert dealer_last_checked_label(None) is None
    assert dealer_last_checked_label("  ") is None


# ── the repository read ───────────────────────────────────────────────────────


def _seed(sqlite_inventory, rows):
    sqlite_inventory.add_cars([
        {"dealer_id": d, "scraped_at": s, "listing_active": a, "make": "Honda", "model": "Accord"}
        for d, s, a in rows
    ])


def test_repo_returns_the_newest_scrape_over_the_dealers_rows(sqlite_inventory):
    from backend.db.inventory_db import dealer_last_scraped_at

    old = _NOW - timedelta(days=20)
    newest = _NOW - timedelta(days=6)
    _seed(sqlite_inventory, [
        (DEALER, _iso(old), 1),
        # A row the last scan retired still records that scan's time.
        (DEALER, _iso(newest), 0),
        (DEALER, None, 1),
        (OTHER, _iso(_NOW), 1),
    ])
    assert dealer_last_scraped_at(DEALER) == _iso(newest)
    assert dealer_last_scraped_at(f" {DEALER} ") == _iso(newest)
    # A mixed-case page key finds the same rows, as the dealer's card grid does.
    assert dealer_last_scraped_at(DEALER.upper()) == _iso(newest)
    assert dealer_last_scraped_at(OTHER) == _iso(_NOW)
    assert dealer_last_scraped_at("nobody-com") is None
    assert dealer_last_scraped_at("") is None
    assert dealer_last_scraped_at(None) is None


def _recorded_sql(monkeypatch, dealer_key: str) -> list[tuple[str, tuple]]:
    """The statements ``dealer_last_scraped_at`` runs for *dealer_key*."""
    import backend.db.repositories.dealers_repo as dr

    seen: list[tuple[str, tuple]] = []
    real_db_conn = dr.db_conn

    class _Cur:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, params=()):
            seen.append((sql, tuple(params)))
            return self._cur.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    class _Conn:
        def __init__(self, conn):
            self._conn = conn

        def cursor(self, *a, **k):
            return _Cur(self._conn.cursor(*a, **k))

    @contextmanager
    def recording_db_conn(**kw):
        with real_db_conn(**kw) as conn:
            yield _Conn(conn)

    monkeypatch.setattr(dr, "db_conn", recording_db_conn)
    dr.dealer_last_scraped_at(dealer_key)
    monkeypatch.setattr(dr, "db_conn", real_db_conn)
    return seen


def test_repo_read_is_one_indexed_query(sqlite_inventory, monkeypatch):
    for key in (DEALER, DEALER.upper()):
        statements = _recorded_sql(monkeypatch, key)
        assert len(statements) == 1, statements
        sql, params = statements[0]
        conn = sqlite3.connect(str(sqlite_inventory.path))
        try:
            plan = " ".join(str(r[-1]) for r in conn.execute("EXPLAIN QUERY PLAN " + sql, params))
        finally:
            conn.close()
        assert "USING INDEX" in plan and "dealer_id=?" in plan, (key, plan)
    # Postgres (prod) has the same index from the baseline migration.
    baseline = (_ROOT / "migrations" / "V001__baseline.sql").read_text(encoding="utf-8")
    assert "CREATE INDEX idx_cars_dealer_listing ON public.cars USING btree (dealer_id, listing_active);" in baseline


def test_repo_without_a_cars_table_reads_as_no_date(tmp_path, monkeypatch):
    import backend.db.inventory_db as inv
    from backend.db.inventory_db import dealer_last_scraped_at

    empty = tmp_path / "empty.db"
    sqlite3.connect(str(empty)).close()
    monkeypatch.setattr(inv, "DB_PATH", str(empty))
    monkeypatch.setenv("INVENTORY_DB_PATH", str(empty))
    assert dealer_last_scraped_at(DEALER) is None


_PG_DSN_ENV = "LAST_CHECKED_PG_DSN"


@pytest.fixture
def pg_scratch_inventory(monkeypatch):
    """A throwaway ``pytest_last_checked_*`` Postgres database with a minimal cars table."""
    base = (os.environ.get(_PG_DSN_ENV) or "").strip()
    if not base:
        pytest.skip(f"{_PG_DSN_ENV} not set (a postgresql:// URL where pytest_* databases may be created)")
    import psycopg

    parts = urlsplit(base)
    if parts.scheme not in ("postgresql", "postgres"):
        pytest.skip(f"{_PG_DSN_ENV} must be a postgresql:// URL")
    name = f"pytest_last_checked_{uuid.uuid4().hex[:12]}"
    dsn = urlunsplit(parts._replace(path="/" + name))
    with psycopg.connect(base, autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{name}"')
    try:
        # Column types as in prod (V001: TEXT), and the index DDL read from V001.
        ddl = next(
            line for line in (_ROOT / "migrations" / "V001__baseline.sql").read_text(encoding="utf-8").splitlines()
            if line.startswith("CREATE INDEX idx_cars_dealer_listing ")
        )
        with psycopg.connect(dsn, autocommit=True) as c:
            c.execute(
                "CREATE TABLE public.cars (id serial PRIMARY KEY, vin text, dealer_id text, "
                "scraped_at text, listing_active integer)"
            )
            c.execute(ddl)
        monkeypatch.setenv("INVENTORY_DATABASE_URL", dsn)
        from backend.db.inventory_pg import is_inventory_postgres

        assert is_inventory_postgres()
        yield dsn
    finally:
        with psycopg.connect(base, autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def test_repo_on_postgres(pg_scratch_inventory):
    import psycopg

    from backend.db.inventory_db import dealer_last_scraped_at

    stale = _real_now() - timedelta(days=6)
    fresh = _real_now() - timedelta(hours=3)
    with psycopg.connect(pg_scratch_inventory, autocommit=True) as c:
        for dealer, at, active in (
            (DEALER, _iso(stale - timedelta(days=9)), 1),
            (DEALER, _iso(stale), 0),
            (DEALER, None, 1),
            (OTHER, _iso(fresh), 1),
        ):
            c.execute(
                "INSERT INTO cars (dealer_id, scraped_at, listing_active) VALUES (%s, %s, %s)",
                (dealer, at, active),
            )
        c.execute("ANALYZE cars")
        # Seq scans off: on a four-row table that is the only way to see whether
        # the planner CAN serve this predicate from the index.
        c.execute("SET enable_seqscan = off")
        for args in ((DEALER,), (DEALER.upper(), DEALER)):
            marks = ", ".join(["%s"] * len(args))
            plan = " ".join(r[0] for r in c.execute(
                f"EXPLAIN SELECT MAX(scraped_at) FROM cars WHERE dealer_id IN ({marks})", args
            ))
            assert "idx_cars_dealer_listing" in plan, plan
    assert dealer_last_scraped_at(DEALER) == _iso(stale)
    assert dealer_last_scraped_at(DEALER.upper()) == _iso(stale)
    assert dealer_last_scraped_at("nobody-com") is None
    assert dealer_last_checked_label(DEALER) == _day(stale)
    assert dealer_last_checked_label(OTHER) is None
    assert dealer_last_checked_label("nobody-com") is None


# ── markup ────────────────────────────────────────────────────────────────────

_VOID = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "source", "track", "wbr"}


class _LineFinder(HTMLParser):
    """Every text node containing LINE, with its element and that element's parent."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack: list[tuple[str, dict, list[str]]] = []
        self.hits: list[dict] = []

    def handle_starttag(self, tag, attrs):
        if self.stack:
            self.stack[-1][2].append(tag)
        if tag not in _VOID:
            self.stack.append((tag, dict(attrs), []))

    def handle_startendtag(self, tag, attrs):
        if self.stack:
            self.stack[-1][2].append(tag)

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, -1, -1):
            if self.stack[i][0] == tag:
                del self.stack[i:]
                return

    def handle_data(self, data):
        if LINE not in data:
            return
        el, parent = self.stack[-1], self.stack[-2]
        self.hits.append({
            "text": " ".join(data.split()),
            "tag": el[0],
            "attrs": el[1],
            "parent_tag": parent[0],
            "parent_class": parent[1].get("class") or "",
            "parent_children_before": list(parent[2]),
            "in_script": any(t in ("script", "style") for t, _a, _c in self.stack),
        })


def _line_hits(html: str) -> list[dict]:
    finder = _LineFinder()
    finder.feed(html)
    finder.close()
    return [h for h in finder.hits if not h["in_script"]]


def _assert_plain_line(html: str, *, date: str, css_class: str, parent_tag: str, parent_class: str):
    hits = _line_hits(html)
    assert len(hits) == 1, hits
    hit = hits[0]
    assert hit["text"] == f"{LINE} {date}"
    # A <p> carrying only the existing muted class: no style attribute, no id,
    # no extra class, and (per the raw markup) no child element either.
    assert hit["tag"] == "p"
    assert hit["attrs"] == {"class": css_class}
    assert f'<p class="{css_class}">{LINE} {date}</p>' in html
    assert not any(w in css_class for w in _BOX_WORDS)
    # Its parent is the element that already held this content, not a wrapper
    # added for the line: it is the named section/hero, and other content
    # precedes the line inside it.
    assert hit["parent_tag"] == parent_tag
    assert hit["parent_class"] == parent_class
    assert hit["parent_children_before"], "the line's parent holds nothing else: a wrapper?"
    assert not any(w in hit["parent_class"] for w in ("card", "box", "pill", "badge", "chip"))


def _css_rule_bodies(css_class: str) -> list[str]:
    bodies = []
    for path in sorted((_ROOT / "frontend" / "static" / "css").glob("*.css")):
        css = re.sub(r"/\*.*?\*/", "", path.read_text(encoding="utf-8"), flags=re.S)
        for selector, body in re.findall(r"([^{}]+)\{([^{}]*)\}", css):
            if re.search(rf"\.{re.escape(css_class)}(?![\w-])", selector):
                bodies.append(body)
    return bodies


@pytest.mark.parametrize("css_class", [_DEALER_CLASS, _CAR_CLASS])
def test_the_reused_muted_class_draws_no_box(css_class):
    bodies = _css_rule_bodies(css_class)
    assert bodies, f".{css_class} has no CSS rule"
    for body in bodies:
        props = {d.split(":", 1)[0].strip().lower() for d in body.split(";") if ":" in d}
        assert "color" in props
        assert not props & {"border", "border-top", "border-bottom", "border-left", "border-right",
                            "border-radius", "background", "background-color", "box-shadow",
                            "outline", "padding"}, (css_class, props)


def test_templates_carry_the_line_once_without_inline_style():
    for name, css_class in (("dealership.html", _DEALER_CLASS), ("car.html", _CAR_CLASS)):
        src = (_ROOT / "frontend" / "templates" / name).read_text(encoding="utf-8")
        lines = [ln.strip() for ln in src.splitlines() if LINE in ln and "{#" not in ln and "#}" not in ln]
        assert lines == [f'<p class="{css_class}">{LINE} {{{{ inventory_last_checked }}}}</p>'], (name, lines)
        assert "{% if inventory_last_checked %}" in src, name


# ── the dealer page ───────────────────────────────────────────────────────────


def _card():
    return {
        "id": 1, "title": "2024 Honda Accord EX", "make": "Honda", "model": "Accord", "trim": "EX",
        "price": 30000, "mileage": 12000, "condition": "Used", "dealer_id": DEALER,
        "dealer_name": "Last Check Motors", "dealer_url": "https://www.lastcheck-motors.com",
    }


@pytest.fixture
def dealer_client(sqlite_inventory, app_factory, tmp_path, monkeypatch):
    from backend.routes import dealership_page as dp

    main = app_factory(ALLOW_DEFAULT_APP_USER="0", RATE_LIMIT_SQLITE_PATH=str(tmp_path / "rate_limits.db"))
    # The page's other reads (card store, registry, specials, reviews) are not
    # under test; the last-checked read goes to the seeded SQLite inventory.
    monkeypatch.setattr(dp, "_dealer_grid_cards_json",
                        lambda d: [json.dumps(_card())] if d == DEALER else [])
    monkeypatch.setattr(dp, "_find_dealership_by_dealer_id", lambda _d: None)
    monkeypatch.setattr(dp, "_dealer_specials", lambda _d: {"total": 0, "groups": [], "scraped_at": None})
    monkeypatch.setattr(dp, "_dealer_reviews", lambda _d: {
        "summary": {"count": 0, "avg_rating": None, "addon_fee_count": 0, "addon_fees": []}, "reviews": []})
    monkeypatch.setattr(dp, "_known_catalog_makes", lambda: frozenset())
    with main.app.test_client() as client:
        yield client


def _query_spy(monkeypatch) -> list:
    import backend.db.inventory_db as inv

    calls: list = []
    real = inv.dealer_last_scraped_at

    def spy(dealer_id):
        calls.append(dealer_id)
        return real(dealer_id)

    monkeypatch.setattr(inv, "dealer_last_scraped_at", spy)
    return calls


def test_dealer_page_shows_the_line_when_stale(dealer_client, sqlite_inventory, monkeypatch):
    stale = _real_now() - timedelta(days=6, hours=5)
    _seed(sqlite_inventory, [(DEALER, _iso(stale - timedelta(days=3)), 1), (DEALER, _iso(stale), 1),
                             (OTHER, _iso(_real_now()), 1)])
    calls = _query_spy(monkeypatch)
    resp = dealer_client.get(f"/dealership/{DEALER}")
    assert resp.status_code == 200
    assert calls == [DEALER], "one last-checked read per page"
    _assert_plain_line(resp.get_data(as_text=True), date=_day(stale), css_class=_DEALER_CLASS,
                       parent_tag="section", parent_class="dealer-inventory")


def test_dealer_page_hides_the_line_under_48_hours(dealer_client, sqlite_inventory):
    _seed(sqlite_inventory, [(DEALER, _iso(_real_now() - timedelta(days=9)), 1),
                             (DEALER, _iso(_real_now() - timedelta(hours=47)), 1)])
    resp = dealer_client.get(f"/dealership/{DEALER}")
    assert resp.status_code == 200
    assert LINE not in resp.get_data(as_text=True)


def test_dealer_page_hides_the_line_without_a_date(dealer_client, sqlite_inventory):
    # Rows on file for other dealers only, and this dealer's one row has no date.
    _seed(sqlite_inventory, [(OTHER, _iso(_real_now() - timedelta(days=30)), 1), (DEALER, None, 1)])
    resp = dealer_client.get(f"/dealership/{DEALER}")
    assert resp.status_code == 200
    assert LINE not in resp.get_data(as_text=True)


# ── the car page ──────────────────────────────────────────────────────────────


def _car_row(**over):
    row = {"id": 77, "vin": "1HGCM82633A004352", "year": 2024, "make": "Honda", "model": "Accord",
           "trim": "EX", "title": "2024 Honda Accord EX", "price": 30000, "mileage": 12000,
           "condition": "Used", "dealer_id": DEALER, "dealer_name": "Last Check Motors",
           "inactive": 0, "gallery": [], "history_highlights": [], "fuel_type": "Gasoline",
           "drivetrain": "FWD"}
    row.update(over)
    return row


@pytest.fixture
def car_client(sqlite_inventory, app_factory, tmp_path, monkeypatch):
    from backend.routes import cars_pages

    main = app_factory(ALLOW_DEFAULT_APP_USER="0", RATE_LIMIT_SQLITE_PATH=str(tmp_path / "rate_limits.db"))
    state = {"row": _car_row()}
    monkeypatch.setattr(cars_pages, "get_car_by_id", lambda *_a, **_k: dict(state["row"]))
    monkeypatch.setattr(cars_pages, "prepare_car_detail_context",
                        lambda _raw: {"verified_specs": {}, "gallery_images": [], "packages_panel_has_content": False})
    with main.app.test_client() as client:
        yield client, state


def test_car_page_shows_its_dealers_date_when_stale(car_client, sqlite_inventory, monkeypatch):
    client, state = car_client
    stale = _real_now() - timedelta(days=4, hours=2)
    # The dealer's newest row is a different car; this car's own row is older.
    state["row"]["scraped_at"] = _iso(stale - timedelta(days=10))
    _seed(sqlite_inventory, [(DEALER, _iso(stale - timedelta(days=10)), 1), (DEALER, _iso(stale), 1),
                             (OTHER, _iso(_real_now()), 1)])
    calls = _query_spy(monkeypatch)
    page = client.get("/car/77")
    assert page.status_code == 200
    assert calls == [DEALER], "one last-checked read per page"
    _assert_plain_line(page.get_data(as_text=True), date=_day(stale), css_class=_CAR_CLASS,
                       parent_tag="div", parent_class="car-detail-info car-detail-info--hero")
    # GET /api/cars/<id> builds the same context, so it carries the same key.
    api = client.get("/api/cars/77").get_json()
    assert api["inventory_last_checked"] == _day(stale)


def test_car_page_hides_the_line_under_48_hours(car_client, sqlite_inventory):
    client, _state = car_client
    _seed(sqlite_inventory, [(DEALER, _iso(_real_now() - timedelta(days=12)), 1),
                             (DEALER, _iso(_real_now() - timedelta(hours=20)), 1)])
    page = client.get("/car/77")
    assert page.status_code == 200
    assert LINE not in page.get_data(as_text=True)
    assert client.get("/api/cars/77").get_json()["inventory_last_checked"] is None


def test_car_page_hides_the_line_without_a_date(car_client, sqlite_inventory):
    client, state = car_client
    _seed(sqlite_inventory, [(OTHER, _iso(_real_now() - timedelta(days=30)), 1)])
    page = client.get("/car/77")
    assert page.status_code == 200
    assert LINE not in page.get_data(as_text=True)
    # A car with no dealer id has no dealer date to show.
    state["row"]["dealer_id"] = None
    page = client.get("/car/77")
    assert page.status_code == 200
    assert LINE not in page.get_data(as_text=True)
