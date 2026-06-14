"""Tests for subscription catalog and discount scaffolding."""

from backend.billing.catalog import (
    FEATURE_AI_CAR_CHAT,
    FEATURE_NEARBY_DEALERS,
    SUBSCRIPTION_PLANS,
    features_for_plan,
    get_plan,
    minimum_plan_for_feature,
    paid_plan_ids,
    stripe_price_id_for_plan,
)
from backend.billing.discounts import checkout_discount_kwargs, promo_code_valid_format, resolve_discount


def test_paid_plans_include_tiers() -> None:
    ids = paid_plan_ids()
    assert "research" in ids
    assert "assistant" in ids
    assert "complete" in ids


def test_assistant_includes_ai_chat() -> None:
    feats = features_for_plan("assistant")
    assert FEATURE_AI_CAR_CHAT in feats


def test_minimum_plan_for_feature() -> None:
    assert minimum_plan_for_feature(FEATURE_NEARBY_DEALERS) == "research"
    assert minimum_plan_for_feature(FEATURE_AI_CAR_CHAT) == "assistant"
    assert minimum_plan_for_feature("window_sticker") == "complete"


def test_complete_is_superset_of_assistant() -> None:
    a = features_for_plan("assistant")
    c = features_for_plan("complete")
    assert a.issubset(c)


def test_stripe_price_fallback_to_legacy_premium_env(monkeypatch) -> None:
    monkeypatch.delenv("STRIPE_PRICE_COMPLETE", raising=False)
    monkeypatch.setenv("STRIPE_PREMIUM_PRICE_ID", "price_legacy_complete")
    assert stripe_price_id_for_plan("complete") == "price_legacy_complete"


def test_promo_code_format() -> None:
    assert promo_code_valid_format("LAUNCH20")
    assert not promo_code_valid_format("bad code!")


def test_env_promo_maps_to_coupon(monkeypatch) -> None:
    monkeypatch.setenv("BILLING_PROMO_LAUNCH20", "coupon_abc123")
    d = resolve_discount("LAUNCH20")
    assert d.stripe_coupon_id == "coupon_abc123"
    kw = checkout_discount_kwargs(d)
    assert kw["discounts"] == [{"coupon": "coupon_abc123"}]
    assert kw["allow_promotion_codes"] is False


def test_unknown_promo_allows_stripe_entry() -> None:
    d = resolve_discount("UNKNOWN")
    assert d.stripe_coupon_id is None
    assert d.allow_user_promo_entry is True


def test_free_plan_has_no_stripe_env() -> None:
    plan = get_plan("free")
    assert plan is not None
    assert plan.monthly_cents == 0
    assert stripe_price_id_for_plan("free") == ""
