"""VDP visit queue ordering (rotation tie-break)."""

from __future__ import annotations

import scanner_vdp as sv


def test_queue_sort_key_rotation_hash_varies_with_seed() -> None:
    v = {"vin": "1HGBH41JXMN109185", "_detail_url": "https://example.com/vdp"}
    k1 = sv._vdp_queue_sort_key(v, "seed-one", rotation=True)
    k2 = sv._vdp_queue_sort_key(v, "seed-two", rotation=True)
    assert k1[0] == k2[0] and k1[1] == k2[1]
    assert k1[2] != k2[2]


def test_queue_sort_key_stable_when_rotation_off() -> None:
    v = {"vin": "1HGBH41JXMN109185", "_detail_url": "https://example.com/vdp"}
    k1 = sv._vdp_queue_sort_key(v, "ignored", rotation=False)
    k2 = sv._vdp_queue_sort_key(v, "ignored", rotation=False)
    assert k1 == k2
    assert k1[2] == "1HGBH41JXMN109185"
