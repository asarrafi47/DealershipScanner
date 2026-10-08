"""Platform fingerprinting + clustering from discovery records (plan item 3).

backend/scanner/platform_fingerprint.py: features from one discovery_<stamp>.json
(the probe's real field names), a stable signature, own-host / CDN dropping,
clustering by signature with the Jaccard merge. backend/scripts/platform_candidates.py:
the log walk, the recipe-state filter, the markdown under _learning/, the run
hook the pipeline prints from. No HTTP: every record is built here. Recipe-state
lookups are stubs, except the P1B.7 cases at the end, which read a tmp recipe
cache and a tmp SQLite ``dealer_recipes`` store wired through ``recipe_store._conn``.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.scanner import platform_fingerprint as pf
from backend.scripts import platform_candidates as pc

TEMPLATES = ["carscommerce", "typesense", "dealer_on_cosmos", "dealer_dot_com", "sister_tv", "team_velocity",
             "dealer_eprocess", "motive_ridemotive", "overfuel", "nabthat", "chapman", "jazel", "dealermasters",
             "wp_vehicles_index", "autowall", "oneaudi", "html_cards"]

_PATHS = ("/inventory/", "/new-vehicles/", "/used-vehicles/", "/searchnew.aspx", "/searchused.aspx", "/inventory/new/",
          "/inventory/used/", "/new-inventory/", "/used-inventory/", "/cars-for-sale/", "/vehicles/", "/all-inventory/",
          "/inventory.json", "/search/used/")


def _detect(**on: bool | float) -> dict:
    d: dict = {t: False for t in TEMPLATES}
    d.update(on)
    return d


def _paths(origin: str, status: int, *, vins_on: tuple[str, ...] = (), challenge: bool = False) -> list[dict]:
    out = []
    for p in _PATHS:
        out.append({"url": origin + p, "final_url": origin + p, "status": status, "bytes": 1200, "secs": 0.4,
                    "title": None, "error": None, "vins": 40 if p in vins_on else 0, "jsonld_vehicles": 0,
                    "challenge": challenge})
    return out


def record(dealer_id: str, host: str, *, status: int = 200, script_hosts: list[str], api_hints: dict | None = None,
           inline: list[str] | None = None, generator: str | None = None, candidates: list[dict] | None = None,
           detect: dict | None = None, challenge: dict | None = None, server: str | None = None,
           path_status: int = 404, vins_on: tuple[str, ...] = (), classification: str = "unknown_platform",
           stamp: str = "2026-09-28T10:00:00+00:00") -> dict:
    """One discovery_<stamp>.json as discovery_probe.probe_dealer writes it."""
    origin = f"https://{host}"
    return {
        "dealer_id": dealer_id, "url": origin, "stamp": stamp,
        "homepage": {"url": origin, "final_url": origin + "/", "status": status, "bytes": 88000, "secs": 0.9,
                     "title": f"{dealer_id} | Cars", "server": server, "cf_ray": bool(server == "cloudflare"),
                     "content_type": "text/html; charset=UTF-8", "redirects": [f"301 {origin}/"]},
        "challenge": challenge or {"verdict": False, "strong_markers": [], "beacon_markers": []},
        "fingerprint": {"platform": next((k for k, v in (detect or {}).items() if v is True), None),
                        "detect": detect or _detect()},
        "signals": {"script_hosts": script_hosts, "api_hints": api_hints or {"fox": 3, "motive": 4, "cdk": 2},
                    "jsonld_vehicles": 0, "vins_in_html": 0, "inline_json": inline or [], "generator": generator},
        "synth": {"candidates": candidates or [], "synth_log": [], "place": {"dealer_city": "X", "dealer_state": "NC", "source": "manifest"},
                  "site_name": dealer_id},
        "paths": _paths(origin, path_status, vins_on=vins_on, challenge=(challenge or {}).get("verdict", False)),
        "classification": classification,
    }


# ── the five fixtures ────────────────────────────────────────────────────────

SUPA_SCRIPTS = ["www.googletagmanager.com", "cdn.jsdelivr.net", "abcd1234.supabase.co", "app.vitecars.io",
                "connect.facebook.net", "maps.googleapis.com"]
SUPA_INLINE = ["window.__VITE_APP__ = {", "window.supabaseConfig = {"]
SUPA_CANDIDATE = [{"url": "https://abcd1234.supabase.co/rest/v1/public-inventory?limit=1000", "method": "GET",
                   "pagination": "none", "provider_hint": "", "auth_header_names": ["apikey"],
                   "page1": {"url": "…", "status": 401, "type": "json", "keys": ["message"], "len": 1, "rows_parsed": 0,
                             "rows_rejected": 0, "vins": 0}}]


def rec_a() -> dict:
    return record("alpha-motors-com", "www.alpha-motors.com", script_hosts=SUPA_SCRIPTS + ["www.alpha-motors.com"],
                  inline=SUPA_INLINE, candidates=SUPA_CANDIDATE, api_hints={"fox": 2, "motive": 3})


def rec_b() -> dict:
    # same platform, one extra chat widget and a different own host
    return record("beta-autos-com", "beta-autos.com",
                  script_hosts=["beta-autos.com", "cdn.gubagoo.io"] + list(reversed(SUPA_SCRIPTS)),
                  inline=list(reversed(SUPA_INLINE)), candidates=SUPA_CANDIDATE, api_hints={"gubagoo": 1, "fox": 1})


def rec_c() -> dict:
    # unique: the oneaudi renderer, nothing in common with the others
    return record("audi-somewhere-com", "www.audisomewhere.com",
                  script_hosts=["cdn.complyauto.com", "tms.audi.com", "www.googletagmanager.com", "assets.adobedtm.com",
                                "fa-oadd-3pi.cdn.prod.collab.apps.one.audi", "oneaudi-falcon.prod.renderer.one.audi",
                                "www.audisomewhere.com"],
                  inline=["window.featureServiceConfigs = {", "window.oadd.vendorIntegrations = {"],
                  api_hints={"cdk": 7, "motive": 5})


def _cf(dealer_id: str, host: str) -> dict:
    return record(dealer_id, host, status=403, server="cloudflare", script_hosts=[],
                  api_hints={"dealerinspire": 2, "cdk": 5, "fox": 5}, inline=["window._cf_chl_opt = {"],
                  challenge={"verdict": True, "strong_markers": ["checking your browser", "__cf_chl", "enable javascript and cookies",
                                                                 "cf-browser-verification"],
                             "beacon_markers": ["/cdn-cgi/challenge-platform"]},
                  path_status=403, classification="homepage_http_403")


def rec_d() -> dict:
    return _cf("delta-cdjr-com", "www.deltacdjr.com")


def rec_e() -> dict:
    return _cf("echo-toyota-com", "www.echotoyota.com")


ALL = (rec_a, rec_b, rec_c, rec_d, rec_e)


# ── fingerprint: features ────────────────────────────────────────────────────

def test_own_host_and_cdns_are_dropped_widgets_downweighted():
    fp = pf.fingerprint(rec_b())
    names = set(fp.features)
    assert "host:beta-autos.com" not in names                # own host
    assert "host:www.googletagmanager.com" not in names      # tag manager
    assert "host:cdn.jsdelivr.net" not in names              # public CDN
    assert "host:connect.facebook.net" not in names
    assert "host:maps.googleapis.com" not in names
    assert "host:abcd1234.supabase.co" in names              # the platform's backend
    assert "host:app.vitecars.io" in names
    assert "widget:gubagoo.io" in names                      # kept, low weight
    assert fp.features["widget:gubagoo.io"] < fp.features["host:abcd1234.supabase.co"]
    # noisy substring hints never become features; a real vendor keyword does
    assert not any(n in ("hint:fox", "hint:motive", "hint:cdk") for n in names)
    assert "hint:gubagoo" in names


def test_api_pattern_normalises_ids_and_drops_own_host():
    own = {"alpha-motors.com"}
    assert pf.api_pattern("https://websites-search.api.carscommerce.inc/api/v1/listings/6059599/search", own) == \
        "websites-search.api.carscommerce.inc/api/vN/listings/N/search"
    assert pf.api_pattern("https://www.alpha-motors.com/inventory-used.json?x=1", own) == "/inventory-used.json"
    assert pf.api_pattern("https://www.alpha-motors.com/wp-json/v1/vehicles/", own) == "/wp-json/vN/vehicles/"
    assert pf.api_pattern("https://www.alpha-motors.com/", own) is None
    fp = pf.fingerprint(rec_a())
    assert "api:GET abcd1234.supabase.co/rest/vN/public-inventory" in fp.features
    assert fp.features["api:GET abcd1234.supabase.co/rest/vN/public-inventory"] == pf.W_API


def test_capture_endpoints_and_graphql_become_api_features():
    cap = {"endpoints": [{"method": "POST", "url": "https://gql.someplatform.com/graphql", "content_type": "application/json",
                          "vehicle_rows": 24, "total_count": 300, "provider_hint": ""}]}
    fp = pf.fingerprint(rec_a(), cap)
    assert "api:POST gql.someplatform.com/graphql" in fp.features
    assert "api:graphql@gql.someplatform.com" in fp.features


def test_challenge_paths_detect_and_generator_features():
    fp = pf.fingerprint(rec_d())
    assert fp.features["challenge:__cf_chl"] == pf.W_CHALLENGE
    assert "beacon:/cdn-cgi/challenge-platform" in fp.features
    assert "homepage:403" in fp.features and "server:cloudflare" in fp.features
    assert "paths:all_403" in fp.features and "paths:challenge" in fp.features
    assert "inline:window._cf_chl_opt" in fp.features
    r = record("wp-dealer-com", "www.wpdealer.com", script_hosts=["www.wpdealer.com"], generator="WP Rocket 3.23.3.3",
               detect=_detect(wp_vehicles_index=True), path_status=200, vins_on=("/inventory/",))
    fp2 = pf.fingerprint(r)
    assert fp2.features["generator:wp rocket"] == pf.W_GENERATOR
    assert fp2.features["detect:wp_vehicles_index"] == pf.W_DETECT
    assert fp2.platform == "wp_vehicles_index"
    assert "paths:vins_in_html" in fp2.features
    assert "path:/inventory/=200:vins" in fp2.features
    assert "path:/vehicles/=200" in fp2.features
    assert "paths:all_200" in fp2.features


# ── signature ────────────────────────────────────────────────────────────────

def test_signature_is_stable_across_ordering_and_widgets():
    a, b = pf.fingerprint(rec_a()), pf.fingerprint(rec_b())
    assert a.signature == pf.fingerprint(rec_a()).signature
    # b lists the same hosts reversed and adds a chat widget: same signature
    assert a.signature == b.signature
    parts = a.signature.split("|")
    assert 3 <= len(parts) <= pf.SIGNATURE_MAX
    assert parts == sorted(parts)
    assert any(p.startswith("api:") for p in parts)
    assert not any(p.startswith("widget:") for p in parts)


def test_signature_caps_two_per_kind_and_falls_back_when_weak():
    d = pf.fingerprint(rec_d())
    kinds = [p.split(":", 1)[0] for p in d.signature.split("|")]
    assert kinds.count("challenge") == 2
    assert "inline" in kinds
    weak = pf.make_signature({"path:/a=404": 0.5, "path:/b=404": 0.5})
    assert weak == "path:/a=404|path:/b=404"
    assert pf.make_signature({}) == "(none)"


def test_nearest_template_reads_partial_scores_only():
    r = rec_c()
    assert pf.nearest_template(r) is None                    # boolean map
    r["fingerprint"]["detect"]["oneaudi"] = 0.62
    r["fingerprint"]["detect"]["autowall"] = 0.2
    assert pf.nearest_template(r) == ("oneaudi", 0.62)
    fp = pf.fingerprint(r)
    assert fp.nearest_template == ("oneaudi", 0.62)
    assert "detect:oneaudi" not in fp.features               # a partial score is not a detect


# ── clustering ───────────────────────────────────────────────────────────────

def test_cluster_groups_shared_platforms_and_leaves_the_unique_one_alone():
    fps = [pf.fingerprint(r()) for r in ALL]
    clusters = pf.cluster(fps)
    by_size = {frozenset(c.dealer_ids) for c in clusters}
    assert frozenset({"alpha-motors-com", "beta-autos-com"}) in by_size
    assert frozenset({"delta-cdjr-com", "echo-toyota-com"}) in by_size
    assert frozenset({"audi-somewhere-com"}) in by_size
    assert len(clusters) == 3
    supa = next(c for c in clusters if "alpha-motors-com" in c.dealer_ids)
    assert "api:GET abcd1234.supabase.co/rest/vN/public-inventory" in supa.shared_features
    assert "widget:gubagoo.io" not in supa.shared_features   # only b carries it
    assert supa.shared_features[0].startswith("api:")        # strongest first


def test_jaccard_merge_joins_near_duplicates_with_different_signatures():
    base = {f"host:h{i}.example.net": 2.0 for i in range(8)}
    x = pf.Fingerprint("x", "", dict(base))
    y = pf.Fingerprint("y", "", {**base, "host:extra.example.net": 2.0, "inline:window.Extra": 2.5})
    z = pf.Fingerprint("z", "", {"host:other1.net": 2.0, "host:other2.net": 2.0, "host:h0.example.net": 2.0})
    for f in (x, y, z):
        f.signature = pf.make_signature(f.features)
    assert x.signature != y.signature                        # y's inline feature outranks a host
    assert pf.jaccard(x.feature_set, y.feature_set) == pytest.approx(8 / 10)
    assert pf.jaccard(x.feature_set, z.feature_set) < 0.6
    clusters = pf.cluster([x, y, z])
    assert {frozenset(c.dealer_ids) for c in clusters} == {frozenset({"x", "y"}), frozenset({"z"})}
    merged = next(c for c in clusters if "x" in c.dealer_ids)
    assert set(merged.signatures) == {x.signature, y.signature}
    # a stricter threshold keeps them apart
    strict = pf.cluster([x, y, z], threshold=0.9)
    assert len(strict) == 3


def test_suggest_next_step_orders_evidence():
    fps = [pf.fingerprint(r()) for r in ALL]
    clusters = {c.dealer_ids[0]: c for c in pf.cluster(fps)}
    cf = pf.suggest_next_step(clusters["delta-cdjr-com"])
    assert "browser-capture" in cf and "delta-cdjr-com" in cf and "SCANNER_ALLOW_BROWSER=1" in cf
    supa = pf.suggest_next_step(clusters["alpha-motors-com"])
    assert "write template X for `GET abcd1234.supabase.co/rest/vN/public-inventory`" in supa
    detected = pf.cluster([pf.fingerprint(record(f"tv{i}-com", f"www.tv{i}.com", script_hosts=[], detect=_detect(team_velocity=True)))
                           for i in range(2)])[0]
    assert "template `team_velocity` detects every member" in pf.suggest_next_step(detected)


# ── platform_candidates: files, filter, markdown, hook ───────────────────────

def _write_root(tmp_path: Path) -> Path:
    root = tmp_path / "dealer_logs"
    for mk in ALL:
        r = mk()
        d = root / r["dealer_id"]
        d.mkdir(parents=True)
        # an older record that must NOT be picked (different platform, would break the clusters)
        stale = dict(r, stamp="2026-09-20T00:00:00+00:00", classification="replay_ok_nothing")
        (d / "discovery_20260920T000000+0000.json").write_text(json.dumps(stale), encoding="utf-8")
        (d / "discovery_20260928T100000+0000.json").write_text(json.dumps(r), encoding="utf-8")
        (d / "discovery.md").write_text(
            f"## 2026-09-28T10:00:00+00:00 discovery probe — {r['classification']}\n- url: {r['url']}\n\n"
            "## 2026-09-28 11:00 UTC discovery\n- recipes on file: 0 (none)\n"
            "- verdict: NO RECIPE: needs discovery (probe how the site presents data, then rerun)\n\n",
            encoding="utf-8")
    (root / "_learning").mkdir()
    (root / "not-a-dealer").mkdir()  # no discovery json: ignored
    return root


def test_latest_discovery_file_and_last_verdict(tmp_path):
    root = _write_root(tmp_path)
    files = pc.latest_discovery_files(root)
    assert set(files) == {mk()["dealer_id"] for mk in ALL}
    assert files["alpha-motors-com"].name == "discovery_20260928T100000+0000.json"
    assert pc.last_verdict(root, "alpha-motors-com", None).startswith("NO RECIPE: needs discovery")
    (root / "alpha-motors-com" / "discovery.md").write_text("## x discovery probe — unknown_platform\n", encoding="utf-8")
    assert pc.last_verdict(root, "alpha-motors-com", None) == "unknown_platform"
    assert pc.last_verdict(root, "missing-com", {"classification": "homepage_http_403"}) == "homepage_http_403"


def test_build_filters_live_recipes_and_writes_markdown(tmp_path):
    root = _write_root(tmp_path)
    states = {"alpha-motors-com": "none", "beta-autos-com": "rejected:one_condition", "audi-somewhere-com": "none",
              "delta-cdjr-com": "stale", "echo-toyota-com": "live"}
    res = pc.build(root, state_fn=lambda d: states[d])
    assert res["considered"] == 4                            # echo has a live recipe: excluded
    assert [c.dealer_ids for c in res["clusters"]] == [["alpha-motors-com", "beta-autos-com"]]
    assert res["path"] == root / "_learning" / "platform_candidates.md"
    md = res["path"].read_text(encoding="utf-8")
    assert "# platform_candidates" in md
    assert "## Cluster 1 — 2 dealers" in md
    assert "`alpha-motors-com` — recipe none; last verdict: NO RECIPE: needs discovery" in md
    assert "`beta-autos-com` — recipe rejected:one_condition" in md
    assert "shared features: `api:GET abcd1234.supabase.co/rest/vN/public-inventory`" in md
    assert "nearest known template: none scores partially" in md
    assert "next step: write template X for" in md
    assert "delta-cdjr-com" not in md                        # alone once echo is out: no section
    assert res["lines"] == [
        "discovery: 2 dealers share unknown platform " + res["clusters"][0].signature
        + " (alpha-motors-com, beta-autos-com) → workspace/dealer_logs/_learning/platform_candidates.md"]


def test_markdown_without_clusters_and_named_platform_line(tmp_path):
    root = _write_root(tmp_path)
    res = pc.build(root, dealers={"audi-somewhere-com", "alpha-motors-com"}, state_fn=lambda d: "none")
    assert res["clusters"] == [] and res["lines"] == []
    assert "_No cluster of two or more dealers._" in res["path"].read_text(encoding="utf-8")
    tv = pf.cluster([pf.fingerprint(record(f"tv{i}-com", f"www.tv{i}.com", script_hosts=[], detect=_detect(team_velocity=True)))
                     for i in range(2)])[0]
    assert pc.summary_line(tv).startswith("discovery: 2 dealers share platform team_velocity (template detects)")


def test_report_for_run_only_reports_clusters_with_two_run_dealers(tmp_path, monkeypatch):
    root = _write_root(tmp_path)
    monkeypatch.setattr(pc, "recipe_state", lambda d: "none")   # resolved at call time
    lines = pc.report_for_run(["alpha-motors-com", "beta-autos-com", "delta-cdjr-com"], root=root)
    assert len(lines) == 1 and "alpha-motors-com, beta-autos-com" in lines[0]
    assert (root / "_learning" / "platform_candidates.md").exists()
    # delta + echo cluster exists in the file, but only delta failed in this run: not printed
    md = (root / "_learning" / "platform_candidates.md").read_text(encoding="utf-8")
    assert "delta-cdjr-com" in md and "echo-toyota-com" in md
    assert pc.report_for_run([], root=root) == []
    assert pc.report_for_run(["audi-somewhere-com"], root=root) == []


def test_report_for_run_never_raises(tmp_path, monkeypatch):
    def boom(_):
        raise RuntimeError("db down")

    monkeypatch.setattr(pc, "recipe_state", boom)
    root = _write_root(tmp_path)
    lines = pc.report_for_run(["alpha-motors-com", "beta-autos-com"], root=root)
    assert lines == ["discovery: platform clustering failed: db down"]


def test_pipeline_hook_uses_log_root(tmp_path, monkeypatch):
    from backend.scripts import dealer_pipeline as dp
    from backend.tests.pipeline_patch import patch_pipeline

    root = _write_root(tmp_path)
    patch_pipeline(monkeypatch, "LOG_ROOT", root)
    monkeypatch.setattr(pc, "recipe_state", lambda d: "none")
    lines = dp.platform_cluster_lines(["delta-cdjr-com", "echo-toyota-com", "audi-somewhere-com"])
    assert len(lines) == 1
    assert lines[0].startswith("discovery: 2 dealers share unknown platform ")
    assert "(delta-cdjr-com, echo-toyota-com)" in lines[0]
    assert lines[0].endswith("→ workspace/dealer_logs/_learning/platform_candidates.md")
    assert dp.platform_cluster_lines([]) == []


def test_cli_root_override_and_all(tmp_path, capsys, monkeypatch):
    root = _write_root(tmp_path)
    monkeypatch.setattr(pc, "recipe_state", lambda d: "live")   # --all must ignore it
    assert pc.main(["--root", str(root), "--all"]) == 0
    out = capsys.readouterr().out
    assert out.count("discovery: 2 dealers share unknown platform") == 2
    assert "5 dealers considered, 2 clusters of 2+" in out
    assert (root / "_learning" / "platform_candidates.md").exists()
    assert pc.main(["--root", str(root), "--no-write", "--dealers", "alpha-motors-com"]) == 0
    assert "0 dealers considered" in capsys.readouterr().out   # every dealer live -> filtered out


# ── P1B.7: fail closed, census, dated verdicts, no contradictions ──────────────

def _recipe_row(dealer_id: str, *, stale: bool = False, saved_at: float = 0.0) -> dict:
    return {"dealer_id": dealer_id, "url": f"https://api.example.com/{dealer_id}/inventory", "method": "GET",
            "content_type": "application/json", "post_template": None, "stale": stale, "saved_at": saved_at}


def _store(db: Path, dealer_id: str, rows: list | None = None, *, saved: float = 0.0, hints: dict | None = None,
           recipes_json: str | None = None) -> None:
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE IF NOT EXISTS dealer_recipes (dealer_id TEXT PRIMARY KEY, recipes_json TEXT, "
                 "max_saved_at REAL, scan_hints TEXT, updated_at TEXT)")
    payload = recipes_json if recipes_json is not None else json.dumps(rows if rows is not None else [])
    conn.execute("INSERT OR REPLACE INTO dealer_recipes VALUES (?, ?, ?, ?, ?)",
                 (dealer_id, payload, saved, json.dumps(hints) if hints is not None else None, "2026-10-01"))
    conn.commit()
    conn.close()


def _dump(db: Path) -> list:
    if not db.exists():
        return []
    conn = sqlite3.connect(db)
    try:
        return list(conn.iterdump())
    finally:
        conn.close()


@pytest.fixture
def recipe_env(tmp_path, monkeypatch):
    """A tmp recipe cache and a tmp SQLite store behind ``recipe_store._conn``.
    Every recipe-state write path is a tripwire: recipe_state must read only."""
    from backend.scanner import recipe_store
    from backend.scanner import recipes as rcp

    cache = tmp_path / "recipes"
    cache.mkdir()
    db = tmp_path / "store.db"
    monkeypatch.setattr(rcp, "RECIPES_DIR", cache)
    monkeypatch.setenv("RECIPES_DB_DISABLED", "")
    monkeypatch.setattr(recipe_store, "_conn", lambda: sqlite3.connect(db))
    monkeypatch.setattr(pc, "_warned", set())
    writes: list[str] = []

    def tripwire(name):
        def _hit(*a, **k):
            writes.append(name)
            raise AssertionError(f"platform_candidates called {name}")
        return _hit

    for mod, name in ((rcp, "load_recipes"), (rcp, "save_recipes"), (rcp, "mark_stale"), (rcp, "_atomic_write_json"),
                      (recipe_store, "db_save_recipes"), (recipe_store, "set_scan_hints"),
                      (recipe_store, "_ensure_table")):
        monkeypatch.setattr(mod, name, tripwire(f"{mod.__name__}.{name}"))

    def put_file(dealer_id: str, content) -> Path:
        path = cache / f"{dealer_id}.json"
        path.write_text(content if isinstance(content, str) else json.dumps(content), encoding="utf-8")
        return path

    return SimpleNamespace(cache=cache, db=db, writes=writes, put_file=put_file)


def test_recipe_state_reads_file_and_store_without_writing(recipe_env):
    env = recipe_env
    assert pc.recipe_state("nothing-com") == "none"                      # no file, no store table
    env.put_file("live-com", [_recipe_row("live-com"), _recipe_row("live-com", stale=True)])
    assert pc.recipe_state("live-com") == "live"
    env.put_file("stale-com", [_recipe_row("stale-com", stale=True)])
    assert pc.recipe_state("stale-com") == "stale"
    _store(env.db, "dbonly-com", [_recipe_row("dbonly-com")], saved=5.0)
    assert pc.recipe_state("dbonly-com") == "live"                      # DB copy alone answers
    # A newer DB copy wins over the file, as in load_recipes, but the file is not rewritten.
    f = env.put_file("newer-db-com", [_recipe_row("newer-db-com", saved_at=10.0)])
    before = (f.read_bytes(), f.stat().st_mtime_ns)
    _store(env.db, "newer-db-com", [_recipe_row("newer-db-com", stale=True, saved_at=20.0)], saved=20.0)
    # A newer file wins over the DB, and the DB row is not pushed up.
    env.put_file("newer-file-com", [_recipe_row("newer-file-com", saved_at=30.0)])
    _store(env.db, "newer-file-com", [_recipe_row("newer-file-com", stale=True, saved_at=1.0)], saved=1.0)
    _store(env.db, "rejected-com", [], hints={"recipe_status": "rejected:auth_needed"})
    env.put_file("rejected-com", [_recipe_row("rejected-com")])
    db_before = _dump(env.db)
    assert pc.recipe_state("newer-db-com") == "stale"
    assert pc.recipe_state("newer-file-com") == "live"
    assert pc.recipe_state("rejected-com") == "rejected:auth_needed"   # the status outranks a live file
    assert (f.read_bytes(), f.stat().st_mtime_ns) == before
    assert _dump(env.db) == db_before
    # A URL-rekeyed dealer reads its recipes through _aliases.json.
    (env.cache / "_aliases.json").write_text(json.dumps({"old-name-com": "new-name-com"}), encoding="utf-8")
    env.put_file("old-name-com", [_recipe_row("old-name-com")])
    assert pc.recipe_state("new-name-com") == "live"
    assert env.writes == []
    assert sorted(p.name for p in env.cache.iterdir()) == sorted(
        ["_aliases.json", "live-com.json", "stale-com.json", "newer-db-com.json", "newer-file-com.json",
         "rejected-com.json", "old-name-com.json"])


def test_recipe_state_fails_closed(recipe_env, monkeypatch, caplog):
    from backend.scanner import recipe_store

    env = recipe_env
    env.put_file("corrupt-com", "[{\"url\": ")                               # unreadable: does not parse
    env.put_file("empty-com", "[]")                                          # exists, nothing comes back
    env.put_file("drift-com", [{"some_future_field": 1}])                    # rows that are not recipes
    env.put_file("object-com", {"url": "https://x"})                         # not a list
    for did in ("corrupt-com", "empty-com", "drift-com", "object-com"):
        assert pc.recipe_state(did) == "unknown", did
    state, reason = pc.recipe_state_detail("corrupt-com")
    assert state == "unknown" and "JSONDecodeError" in reason
    assert "exists but no recipe came back" in pc.recipe_state_detail("empty-com")[1]
    # A store row holding only hints (recipes "[]") is a real "none", not unknown.
    _store(env.db, "hints-only-com", [], hints={"notes": "needs synth"})
    assert pc.recipe_state("hints-only-com") == "none"
    # A stored payload that does not parse: unknown, not none.
    _store(env.db, "bad-db-com", recipes_json="{oops")
    assert pc.recipe_state("bad-db-com") == "unknown"
    _store(env.db, "bad-hints-com", [_recipe_row("bad-hints-com")])
    conn = sqlite3.connect(env.db)
    conn.execute("UPDATE dealer_recipes SET scan_hints = 'nope' WHERE dealer_id = 'bad-hints-com'")
    conn.commit()
    conn.close()
    assert pc.recipe_state("bad-hints-com") == "unknown"
    # The store is on but unreachable: unknown even with a live file.
    env.put_file("live-com", [_recipe_row("live-com")])

    def down():
        raise sqlite3.OperationalError("unable to open database file")

    monkeypatch.setattr(recipe_store, "_conn", down)
    caplog.set_level(logging.WARNING, logger="scanner.platform_candidates")
    assert pc.recipe_state("live-com") == "unknown"
    assert "recipe lookup raised OperationalError" in caplog.text and "live-com" in caplog.text
    # The store switched off on purpose: the file alone decides.
    monkeypatch.setenv("RECIPES_DB_DISABLED", "1")
    assert pc.recipe_state("live-com") == "live"
    assert env.writes == []


def test_recipe_state_unreadable_file_permission(recipe_env):
    path = recipe_env.put_file("locked-com", [_recipe_row("locked-com")])
    path.chmod(0)
    try:
        try:
            path.read_bytes()
        except PermissionError:
            pass
        else:
            pytest.skip("running with permission to read a mode-000 file (root)")
        assert pc.recipe_state("locked-com") == "unknown"
        assert "PermissionError" in pc.recipe_state_detail("locked-com")[1]
    finally:
        path.chmod(0o600)


def test_unknown_is_counted_not_clustered_and_census_header(tmp_path):
    root = _write_root(tmp_path)
    states = {"alpha-motors-com": "unknown", "beta-autos-com": "none", "audi-somewhere-com": "live",
              "delta-cdjr-com": "rejected:auth_needed", "echo-toyota-com": "stale"}
    res = pc.build(root, state_fn=lambda d: states[d])
    assert res["considered"] == 3                                            # beta, delta, echo
    assert [c.dealer_ids for c in res["clusters"]] == [["delta-cdjr-com", "echo-toyota-com"]]
    assert res["unknown"] == ["alpha-motors-com"]
    assert res["census"] == {"live": 1, "none": 1, "stale": 1, "rejected": 1, "unknown": 1}
    md = res["path"].read_text(encoding="utf-8")
    assert ("Recipe census (5 dealers with a discovery record): "
            "live 1 / none 1 / stale 1 / rejected 1 / unknown 1.") in md
    assert f"Read from: dealer logs `{root}`; recipe cache `" in md
    assert f"cwd `{os.getcwd()}`." in md
    head, _, unknown_part = md.partition("## Recipe state unknown: counted, not clustered (1)")
    assert unknown_part and "`alpha-motors-com` — last verdict: NO RECIPE: needs discovery" in unknown_part
    assert "alpha-motors-com" not in head.split("## Cluster 1", 1)[1]     # never a cluster member
    # --all takes no census and keeps every dealer.
    res_all = pc.build(root, state_fn=lambda d: states[d], only_needing=False, write=False)
    assert res_all["census"] is None and res_all["considered"] == 5


def test_builtin_lookup_puts_the_unknown_reason_in_the_report(recipe_env, tmp_path, capsys):
    root = _write_root(tmp_path)
    recipe_env.put_file("alpha-motors-com", "not json")
    recipe_env.put_file("audi-somewhere-com", [_recipe_row("audi-somewhere-com")])
    assert pc.main(["--root", str(root)]) == 0                             # default state_fn: the real lookup
    out = capsys.readouterr().out
    assert "recipe census live 1 / none 3 / stale 0 / rejected 0 / unknown 1; unknown: alpha-motors-com" in out
    assert "platform_candidates: unknown alpha-motors-com: recipe lookup raised JSONDecodeError" in out
    md = (root / "_learning" / "platform_candidates.md").read_text(encoding="utf-8")
    assert "recipe lookup raised JSONDecodeError" in md.split("## Recipe state unknown", 1)[1]
    assert f"recipe cache `{recipe_env.cache}`" in md
    assert recipe_env.writes == []


def _scan_block(stamp: str, verdict: str, rows: int | str = "-") -> str:
    return (f"## {stamp} scan — verdict **{verdict}** (reason)\n"
            f"- rows written: {rows} (new 0, used {rows}); listed before: 0; active after: 0\n"
            "- recipe: full coverage {}; provider x; 1.0 min\n\n")


def test_member_with_a_newer_good_scan_is_dropped_with_a_note(tmp_path):
    root = _write_root(tmp_path)                                             # every probe: 2026-09-28T10:00:00+00:00
    hdr = "# x — scan_runs\n\nProcess: docs/NETWORK_SCAN_PROCESS.md\n\n"
    (root / "delta-cdjr-com" / "scan_runs.md").write_text(
        hdr + _scan_block("2026-09-27 09:00 UTC", "error") + _scan_block("2026-09-29 13:45 UTC", "inaccurate", 3958)
        + "## 2026-09-30 08:00 UTC retroactive reconcile\n- retired: 0\n\n", encoding="utf-8")
    # newest scan failed: kept, although an earlier one after the probe was ok
    (root / "echo-toyota-com" / "scan_runs.md").write_text(
        hdr + _scan_block("2026-09-28 12:00 UTC", "ok", 40) + _scan_block("2026-09-29 12:00 UTC", "no_rows", 0),
        encoding="utf-8")
    # ok scan older than the probe: kept
    (root / "alpha-motors-com" / "scan_runs.md").write_text(hdr + _scan_block("2026-09-28 09:59 UTC", "ok", 12),
                                                            encoding="utf-8")
    scan = pc.newest_scan(root, "delta-cdjr-com")
    assert (scan["verdict"], scan["stamp"], scan["rows"]) == ("inaccurate", "2026-09-29 13:45 UTC", 3958)
    res = pc.build(root, state_fn=lambda d: "none")
    assert res["dropped"] == ["delta-cdjr-com"]
    assert [c.dealer_ids for c in res["clusters"]] == [["alpha-motors-com", "beta-autos-com"]]
    assert res["considered"] == 4
    md = res["path"].read_text(encoding="utf-8")
    assert "## Dropped: scanned after the probe (1)" in md
    assert ("- `delta-cdjr-com` — probe 2026-09-28 10:00 UTC (homepage_http_403); newest scan 2026-09-29 13:45 UTC "
            "verdict inaccurate, 3,958 rows (workspace/dealer_logs/delta-cdjr-com/scan_runs.md)") in md
    assert "`delta-cdjr-com` — recipe" not in md                             # not listed as a member
    # Without the recipe filter (--all) nothing is dropped.
    assert pc.build(root, state_fn=lambda d: "none", only_needing=False, write=False)["dropped"] == []
    # A newer ok scan excludes the member too; its partner is left alone, so no cluster remains.
    (root / "beta-autos-com" / "scan_runs.md").write_text(hdr + _scan_block("2026-09-28 10:01 UTC", "ok", 55),
                                                          encoding="utf-8")
    res = pc.build(root, state_fn=lambda d: "none")
    assert res["dropped"] == ["beta-autos-com", "delta-cdjr-com"]
    assert res["clusters"] == [] and res["considered"] == 3
    assert "- `beta-autos-com` — probe 2026-09-28 10:00 UTC (unknown_platform); newest scan 2026-09-28 10:01 UTC " \
           "verdict ok, 55 rows" in res["path"].read_text(encoding="utf-8")


def test_last_verdict_is_dated_and_member_lines_show_it(tmp_path):
    root = _write_root(tmp_path)
    assert pc.last_verdict_dated(root, "alpha-motors-com", None) == (
        "NO RECIPE: needs discovery (probe how the site presents data, then rerun)", "2026-09-28 11:00 UTC")
    (root / "beta-autos-com" / "discovery.md").write_text(
        "## 2026-09-26T12:23:19+00:00 discovery probe — homepage_http_403\n- url: x\n", encoding="utf-8")
    assert pc.last_verdict_dated(root, "beta-autos-com", None) == ("homepage_http_403", "2026-09-26 12:23 UTC")
    assert pc.last_verdict_dated(root, "missing-com", {"classification": "homepage_http_403",
                                                       "stamp": "2026-09-20T01:02:03+00:00"}) == (
        "homepage_http_403", "2026-09-20 01:02 UTC")
    md = pc.build(root, state_fn=lambda d: "none")["path"].read_text(encoding="utf-8")
    assert ("`alpha-motors-com` — recipe none; last verdict: NO RECIPE: needs discovery (probe how the site presents "
            "data, then rerun) — dated 2026-09-28 11:00 UTC (probe 2026-09-28T10:00:00+00:00;") in md
    assert "`beta-autos-com` — recipe none; last verdict: homepage_http_403 — dated 2026-09-26 12:23 UTC" in md


def test_parse_stamp_forms():
    utc = "2026-09-26 12:23 UTC"
    assert pc.format_stamp(pc.parse_stamp("2026-09-26 12:23 UTC")) == utc
    assert pc.format_stamp(pc.parse_stamp("2026-09-26T12:23:19+00:00")) == utc
    assert pc.format_stamp(pc.parse_stamp("2026-09-26T08:23:19-04:00")) == utc
    assert pc.format_stamp(pc.parse_stamp("2026-09-26T12:23:19Z")) == utc
    assert pc.format_stamp(pc.parse_stamp("20260926T122319")) == utc
    assert pc.format_stamp(pc.parse_stamp("20260926T122319+0000")) == utc
    assert pc.parse_stamp("") is None and pc.parse_stamp("soon") is None and pc.format_stamp(None) == ""


def test_no_contradictory_template_lines(tmp_path):
    root = _write_root(tmp_path)
    for i in range(2):
        r = record(f"tv{i}-com", f"www.tv{i}.com", script_hosts=["cdn.teamvelocity.example"],
                   detect=_detect(team_velocity=True), path_status=200, vins_on=("/inventory/",))
        d = root / r["dealer_id"]
        d.mkdir()
        (d / "discovery_20260928T100000+0000.json").write_text(json.dumps(r), encoding="utf-8")
    md = pc.build(root, state_fn=lambda d: "none")["path"].read_text(encoding="utf-8")
    sections = {s.split("\n", 1)[0]: s for s in md.split("\n## ")[1:]}
    tv = next(s for s in sections.values() if "`tv0-com`" in s)
    supa = next(s for s in sections.values() if "`alpha-motors-com`" in s)
    assert "- template already detecting: team_velocity (2 of 2 members)" in tv
    assert "nearest known template" not in tv                                # never "none ... all false" here
    assert "- nearest known template: none scores partially (detect map is boolean, all false)" in supa
    assert "template already detecting" not in supa
    # A partial score is still shown next to a detecting template.
    r = record("tv2-com", "www.tv2.com", script_hosts=["cdn.teamvelocity.example"],
               detect=_detect(team_velocity=True, oneaudi=0.4), path_status=200, vins_on=("/inventory/",))
    (root / "tv2-com").mkdir()
    (root / "tv2-com" / "discovery_20260928T100000+0000.json").write_text(json.dumps(r), encoding="utf-8")
    md = pc.build(root, state_fn=lambda d: "none")["path"].read_text(encoding="utf-8")
    tv = next(s for s in md.split("\n## ")[1:] if "`tv0-com`" in s)
    assert "template already detecting: team_velocity (3 of 3 members)" in tv
    assert "nearest known template: `oneaudi` (partial detect score 0.40)" in tv
    assert "all false" not in tv
