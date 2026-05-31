"""VDP visit queue ordering (rotation tie-break)."""

from __future__ import annotations

import backend.scanner.vdp as sv


def test_queue_sort_key_rotation_hash_varies_with_seed() -> None:
    v = {"vin": "1HGBH41JXMN109185", "_detail_url": "https://example.com/vdp"}
    k1 = sv._vdp_queue_sort_key(v, "seed-one", rotation=True)
    k2 = sv._vdp_queue_sort_key(v, "seed-two", rotation=True)
    assert k1[0] == k2[0] and k1[1] == k2[1] and k1[2] == k2[2]
    assert k1[3] != k2[3]


def test_queue_sort_key_prefers_public_incomplete_gaps() -> None:
    thin = {"vin": "1HGBH41JXMN109186", "_detail_url": "https://example.com/a", "transmission": "Auto"}
    incomplete = {
        "vin": "1HGBH41JXMN109185",
        "_detail_url": "https://example.com/b",
        "transmission": "",
        "exterior_color": "",
        "interior_color": "",
    }
    k_thin = sv._vdp_queue_sort_key(thin, "s", rotation=False)
    k_inc = sv._vdp_queue_sort_key(incomplete, "s", rotation=False)
    assert k_inc < k_thin


def test_queue_sort_key_stable_when_rotation_off() -> None:
    v = {"vin": "1HGBH41JXMN109185", "_detail_url": "https://example.com/vdp"}
    k1 = sv._vdp_queue_sort_key(v, "ignored", rotation=False)
    k2 = sv._vdp_queue_sort_key(v, "ignored", rotation=False)
    assert k1 == k2
    assert k1[3] == "1HGBH41JXMN109185"
