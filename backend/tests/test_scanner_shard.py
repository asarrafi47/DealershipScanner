"""Shard partitioning for parallel scanner workers."""

from backend.scanner.cli import filter_manifest_by_shard


def test_filter_manifest_by_shard_three_way():
    dealers = [{"dealer_id": str(i)} for i in range(10)]
    s0 = filter_manifest_by_shard(dealers, 0, 3)
    s1 = filter_manifest_by_shard(dealers, 1, 3)
    s2 = filter_manifest_by_shard(dealers, 2, 3)
    assert [d["dealer_id"] for d in s0] == ["0", "3", "6", "9"]
    assert [d["dealer_id"] for d in s1] == ["1", "4", "7"]
    assert [d["dealer_id"] for d in s2] == ["2", "5", "8"]
    assert len(s0) + len(s1) + len(s2) == 10


def test_filter_manifest_by_shard_count_one_is_noop():
    dealers = [{"a": 1}, {"a": 2}]
    assert filter_manifest_by_shard(dealers, 0, 1) == dealers
