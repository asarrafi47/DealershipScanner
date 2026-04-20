"""dealers.json slug + manifest upsert (tmp path; no live dealers.json)."""

from __future__ import annotations

import json

import pytest

from backend import dev_dealers as dd


def test_slug_from_url_matches_scanner_js_hostname_slug() -> None:
    assert dd.slug_from_url("https://WWW.Some-Dealer.COM/path") == "some-dealer-com"
    assert dd.slug_from_url("foo.bar.example.com") == "foo-bar-example-com"


def test_normalize_manifest_url_https_no_trailing_slash() -> None:
    assert dd.normalize_manifest_url("http://x.example/") == "https://x.example"


@pytest.fixture()
def isolated_dealers_json(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "dealers.json"
    p.write_text("[]\n", encoding="utf-8")
    monkeypatch.setattr(dd, "DEALERS_PATH", p)


def test_upsert_inserts_then_updates_by_url(isolated_dealers_json: None) -> None:
    a, did = dd.upsert_dealer_manifest_row(
        name="First",
        website_url="https://inventory.example.com/",
        provider="dealer_dot_com",
    )
    assert a == "inserted"
    assert did == "inventory-example-com"
    rows = dd.load_dealers()
    assert len(rows) == 1
    assert rows[0]["name"] == "First"

    b, did2 = dd.upsert_dealer_manifest_row(
        name="Second",
        website_url="https://inventory.example.com",
        provider="dealer_dot_com",
    )
    assert b == "updated"
    assert did2 == did
    rows = dd.load_dealers()
    assert len(rows) == 1
    assert rows[0]["name"] == "Second"


def test_upsert_updates_by_dealer_id(isolated_dealers_json: None) -> None:
    dd.save_dealers(
        [
            {
                "name": "Old",
                "url": "https://a.example.com",
                "provider": "dealer_dot_com",
                "dealer_id": "shared-slug",
            }
        ]
    )
    action, did = dd.upsert_dealer_manifest_row(
        name="New",
        website_url="https://b.example.com",
        dealer_id="shared-slug",
    )
    assert action == "updated"
    assert did == "shared-slug"
    rows = dd.load_dealers()
    assert len(rows) == 1
    assert rows[0]["url"] == "https://b.example.com"
    assert rows[0]["name"] == "New"


def test_upsert_creates_file_when_missing(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    p = tmp_path / "dealers.json"
    monkeypatch.setattr(dd, "DEALERS_PATH", p)
    assert not p.is_file()
    dd.upsert_dealer_manifest_row(
        name="X",
        website_url="https://z.example.com",
        dealer_id="z-example-com",
    )
    assert p.is_file()
    data = json.loads(p.read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert len(data) == 1


def test_smart_import_scrape_succeeded_scan_vehicle_count() -> None:
    assert dd.smart_import_scrape_succeeded(0, "foo\nSCAN_VEHICLE_COUNT:3\n")
    assert not dd.smart_import_scrape_succeeded(0, "SCAN_VEHICLE_COUNT:0\n")
    assert not dd.smart_import_scrape_succeeded(1, "SCAN_VEHICLE_COUNT:10\n")


def test_smart_import_scrape_succeeded_upsert_line() -> None:
    assert dd.smart_import_scrape_succeeded(0, "Upserted 12 unique vehicles\n")
    assert not dd.smart_import_scrape_succeeded(0, "Upserted 0 unique vehicles\n")


def test_smart_import_manifest_display_name_priority() -> None:
    url = "https://inventory.example.com/"
    assert (
        dd.smart_import_manifest_display_name(
            url,
            resolved={"name": "From Result"},
            error_partial={"name": "Partial"},
            discovery=[{"name": "Disc"}],
        )
        == "From Result"
    )
    assert (
        dd.smart_import_manifest_display_name(
            url,
            resolved=None,
            error_partial={"name": "Partial Only"},
            discovery=[{"name": "Disc"}],
        )
        == "Partial Only"
    )
    assert (
        dd.smart_import_manifest_display_name(
            url,
            resolved=None,
            error_partial={},
            discovery=[{"message": "x"}, {"name": "Last Name"}],
        )
        == "Last Name"
    )
    assert (
        dd.smart_import_manifest_display_name(
            url,
            resolved=None,
            error_partial={},
            discovery=[{"message": "Found name:  Acme Ford  "}],
        )
        == "Acme Ford"
    )
    slug = dd.slug_from_url(url)
    fb = dd.smart_import_manifest_display_name(
        url, resolved=None, error_partial={}, discovery=[]
    )
    assert fb == " ".join(p.capitalize() for p in slug.split("-") if p)


def test_manifest_only_simulated_no_registry_but_scrape_ok(isolated_dealers_json: None) -> None:
    """
    Mirrors dev smart-import manifest-only path: resolved DealerCreate missing,
    but exit 0 + SCAN_VEHICLE_COUNT still yields dealers.json row.
    """
    job_url = "https://www.some-dealer.com/inventory"
    log = "SMART_IMPORT_ERROR: …\nSCAN_VEHICLE_COUNT:161\n"
    assert dd.smart_import_scrape_succeeded(0, log)
    wurl = dd.normalize_manifest_url(job_url)
    disp = dd.smart_import_manifest_display_name(
        job_url,
        resolved=None,
        error_partial={"name": "Some Dealer Auto"},
        discovery=[],
    )
    action, did = dd.upsert_dealer_manifest_row(name=disp, website_url=wurl)
    assert action == "inserted"
    assert did == "some-dealer-com"
    rows = dd.load_dealers()
    assert len(rows) == 1
    assert rows[0]["name"] == "Some Dealer Auto"
    assert rows[0]["url"] == "https://www.some-dealer.com/inventory"
