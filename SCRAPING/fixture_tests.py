"""
Unit-style checks for org validation and extraction (no network).

Run: python -m SCRAPING.fixture_tests
"""
from __future__ import annotations

from SCRAPING.inference import extract_candidates_from_text
from SCRAPING.canonical_groups import canonical_group_display, merge_canonical_key
from SCRAPING.org_validation import (
    is_plausible_org_name,
    normalize_group_name,
)
from SCRAPING.redirects import describe_redirect, domains_plausibly_related


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def test_junk_rejected() -> None:
    junk = [
        "this Privacy Policy or the Terms of Use",
        "our job to highlight the best aspects of our new model lineup",
        "the Charlotte community",
        "these Terms of Service by posting updates and changes to our website",
    ]
    for j in junk:
        ok, _ = is_plausible_org_name(normalize_group_name(j))
        _assert(not ok, f"should reject junk: {j!r}")


def test_good_names() -> None:
    good = [
        "Hendrick Automotive Group",
        "Tuttle-Click Automotive Group",
        "Penske Automotive Group",
        "the Tuttle-Click automotive group",
        "a Tuttle-Click Automotive Group",
    ]
    for g in good:
        ok, reason = is_plausible_org_name(normalize_group_name(g))
        _assert(ok, f"should accept {g!r} got {reason}")


def test_canonicalization() -> None:
    _assert(
        canonical_group_display("Tuttle-Click") == "Tuttle-Click Automotive Group",
        "tuttle short",
    )
    _assert(
        canonical_group_display("Tuttle Click Automotive Group")
        == "Tuttle-Click Automotive Group",
        "tuttle long",
    )
    _assert(canonical_group_display("Sonic Automotive") == "Sonic Automotive", "sonic")
    _assert(
        merge_canonical_key("Tuttle-Click")
        == merge_canonical_key("Tuttle-Click Automotive Group"),
        "merge key aligns tuttle variants",
    )


def test_redirect_mismatch() -> None:
    _assert(
        not domains_plausibly_related(
            "https://toyotaofcharlotte.com",
            "https://www.random-health-blog.net",
        ),
        "unrelated domains",
    )
    _assert(
        domains_plausibly_related(
            "https://www.hendrickbmw.com",
            "https://hendrickbmw.com",
        ),
        "same registrable",
    )
    r, mismatch, _ = describe_redirect(
        "https://shop.dealer.com", "https://totally-unrelated-parking.com"
    )
    _assert(mismatch or r, "should flag cross-domain")


def test_extraction_no_policy_sentence() -> None:
    text = (
        "This Privacy Policy or the Terms of Use may be updated. "
        "We are part of Hendrick Automotive Group. "
        "Copyright 2024 Bad Sentence About Cookies."
    )
    found, rej = extract_candidates_from_text(text, "https://x.com/", "about")
    texts = [f[0] for f in found]
    _assert(
        any("Hendrick" in t for t in texts),
        f"expected Hendrick in {texts}",
    )
    _assert(
        not any("Privacy Policy" in t for t in texts),
        "should not extract policy phrase",
    )


def test_tuttle_part_of_article_phrase() -> None:
    text = (
        "Tuttle-Click Ford is part of the Tuttle-Click automotive group. "
        "We look forward to serving you."
    )
    found, _rej = extract_candidates_from_text(text, "https://ford.example/", "homepage")
    texts = [f[0] for f in found]
    _assert(any("Tuttle-Click" in t for t in texts), f"expected Tuttle-Click in {texts}")


def test_threshold_status() -> None:
    from SCRAPING.org_validation import finalize_status

    _assert(finalize_status(0.82, "X") == "assigned", "high")
    _assert(finalize_status(0.60, "Y") == "manual_review", "mid band")
    _assert(finalize_status(0.50, "Z") == "manual_review_low_confidence", "low with name")
    _assert(finalize_status(0.50, None) == "unknown", "low no name")
    _assert(finalize_status(0.30, None) == "unknown", "very low")


def main() -> int:
    test_junk_rejected()
    test_good_names()
    test_canonicalization()
    test_redirect_mismatch()
    test_extraction_no_policy_sentence()
    test_tuttle_part_of_article_phrase()
    test_threshold_status()
    print("SCRAPING.fixture_tests: all passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
