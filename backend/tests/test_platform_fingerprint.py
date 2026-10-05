"""Platform fingerprinting + clustering from discovery records (plan item 3).

backend/scanner/platform_fingerprint.py: features from one discovery_<stamp>.json
(the probe's real field names), a stable signature, own-host / CDN dropping,
clustering by signature with the Jaccard merge. backend/scripts/platform_candidates.py:
the log walk, the recipe-state filter, the markdown under _learning/, the run
hook the pipeline prints from. No HTTP, no database: every record is built here
and every recipe-state lookup is a stub.
"""
from __future__ import annotations

import json
from pathlib import Path

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
