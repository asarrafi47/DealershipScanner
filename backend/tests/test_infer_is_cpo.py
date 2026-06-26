"""infer_is_cpo_for_storage — CPO flag from condition/title/URL."""

from __future__ import annotations

from backend.utils.car_serialize import infer_is_cpo_for_storage


def test_infer_from_condition_certified_pre_owned() -> None:
    assert infer_is_cpo_for_storage({"condition": "Certified Pre-Owned"}) == 1


def test_infer_from_title_cpo() -> None:
    assert infer_is_cpo_for_storage({"title": "2022 BMW X5 CPO"}) == 1


def test_infer_from_certified_inventory_url() -> None:
    assert (
        infer_is_cpo_for_storage(
            {"source_url": "https://dealer.com/certified-inventory/vin123"}
        )
        == 1
    )


def test_infer_none_for_used_without_cpo_signal() -> None:
    assert infer_is_cpo_for_storage({"condition": "Used", "title": "2020 Honda Accord"}) is None


def test_infer_respects_explicit_zero() -> None:
    assert infer_is_cpo_for_storage({"is_cpo": 0, "condition": "Certified Pre-Owned"}) == 0
