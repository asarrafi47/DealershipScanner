import pytest


def test_shard_order_knob_changes_first_writers(monkeypatch):
    # N10.8: in an empty DB every weight is 50, so the default order is alphabetical.
    from backend.scripts import fleet_scan

    ids = [f"d{i:02d}" for i in range(12)]
    base = fleet_scan.make_shards(ids, 3, {})
    rev = fleet_scan.make_shards(ids, 3, {}, "reverse")
    seeded = fleet_scan.make_shards(ids, 3, {}, "seed:7")
    assert fleet_scan.make_shards(ids, 3, {}, "seed:7") == seeded
    assert sorted(sum(base, [])) == sorted(sum(rev, [])) == sorted(sum(seeded, [])) == ids
    assert [s[0] for s in rev] != [s[0] for s in base]
    assert rev[0][0] != base[0][0]
    assert seeded != base

    monkeypatch.setenv("SCAN_SHARD_ORDER", "seed:3")
    assert fleet_scan.shard_order() == "seed:3"
    monkeypatch.setenv("SCAN_SHARD_ORDER", "sideways")
    with pytest.raises(SystemExit):
        fleet_scan.shard_order()
