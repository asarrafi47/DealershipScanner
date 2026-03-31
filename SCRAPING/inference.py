"""Window-based phrase extraction + copyright, strict org validation, scoring."""
from __future__ import annotations

import re
from urllib.parse import urlparse

from SCRAPING.constants import (
    COPYRIGHT_RE,
    OWNERSHIP_ANCHOR_PATTERNS,
    PAGE_WEIGHT,
    STANDALONE_ORG_PATTERNS,
    THRESH_ASSIGNED,
)
from SCRAPING.entity_specificity import (
    entity_specificity_score,
    is_department_or_unit_like,
    ownership_signal_strong,
    rank_candidate_entries,
)
from SCRAPING.models import Evidence
from SCRAPING.canonical_groups import canonical_group_display, merge_canonical_key
from SCRAPING.org_validation import (
    has_negative_substring,
    is_plausible_org_name,
    normalize_group_name,
)
from SCRAPING.text_utils import collapse_ws, is_vendor_text

WINDOW_CHARS = 200

_SECOND_PASS_TRIGGERS: list[tuple[re.Pattern[str], str, float]] = [
    (re.compile(r"\bsonic\s+automotive(?:\s+group)?\b", re.I), "second_pass_sonic", 0.84),
    (re.compile(r"\b(?:automotive|auto)\s+group\b", re.I), "second_pass_auto_group", 0.62),
    (re.compile(r"\bcollision\s+group\b", re.I), "second_pass_collision", 0.64),
    (re.compile(r"\bfamily\s+of\s+dealerships?\b", re.I), "second_pass_family", 0.62),
    (re.compile(r"\bparent\s+company\b", re.I), "second_pass_parent", 0.62),
    (re.compile(r"\bowned\s+by\b", re.I), "second_pass_owned", 0.62),
    (re.compile(r"\b(?:is\s+)?part\s+of\b", re.I), "second_pass_partof", 0.62),
    (re.compile(r"\bmember\s+of\b", re.I), "second_pass_member", 0.62),
]


def _host(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _clip_window(s: str) -> str:
    """First clause / segment only."""
    s = s.strip()
    if not s:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", s, maxsplit=1)
    return parts[0][:WINDOW_CHARS]


def extract_org_from_window(window: str) -> list[str]:
    """Pull organization-shaped spans from a short local window."""
    w = _clip_window(window)
    if len(w) < 4:
        return []
    out: list[str] = []

    pat = re.compile(
        r"\b([A-Z][A-Za-z0-9'\-\.]*"
        r"(?:\s+[&]\s+[A-Z][A-Za-z0-9'\-\.]*)*"
        r"(?:\s+[A-Z][A-Za-z0-9'\-\.]*){0,5}\s*"
        r"(?:Automotive\s+|Auto\s+)?"
        r"(?:Group|Motors|Holdings|Family|Corporation|Corp\.?|Inc\.?|LLC|LP)\b)",
        re.I,
    )
    for m in pat.finditer(w):
        out.append(m.group(1).strip())

    pat2 = re.compile(
        r"\b((?:Hendrick|Penske|Lithia|Sonic|Asbury|AutoNation|"
        r"Tuttle-Click|Tuttle\s+Click|Fletcher\s+Jones|Holman|Morgan)\s+"
        r"[A-Za-z0-9'\-\s]{0,40}?)",
        re.I,
    )
    for m in pat2.finditer(w):
        chunk = m.group(1).strip()
        if len(chunk) >= 8 and not has_negative_substring(chunk):
            out.append(chunk)

    dedup: list[str] = []
    seen: set[str] = set()
    for x in out:
        k = re.sub(r"\s+", " ", x.lower())
        if k not in seen:
            seen.add(k)
            dedup.append(x)
    return dedup


def _keyword_page_boost(page_url: str, page_kind: str) -> float:
    u = f"{page_url} {page_kind}".lower()
    if any(
        x in u
        for x in (
            "privacy",
            "about",
            "terms",
            "legal",
            "careers",
            "company",
            "collision",
            "employment",
            "staff",
            "sonic",
        )
    ):
        return 1.06
    return 1.0


def extract_candidates_from_text(
    text: str,
    page_url: str,
    page_kind: str,
    *,
    out_raw_pre_filter: list[str] | None = None,
) -> tuple[
    list[tuple[str, str, float, str]],
    list[dict[str, str]],
]:
    """
    Returns (accepted tuples (raw, signal, weighted_score, snippet), rejected dicts).
    """
    collapsed = collapse_ws(text)
    if len(collapsed) < 20:
        return [], []

    weight = PAGE_WEIGHT.get(page_kind, 0.4)
    found: list[tuple[str, str, float, str]] = []
    rejected: list[dict[str, str]] = []

    def try_accept(raw: str, sig: str, base: float, sn: str) -> None:
        if out_raw_pre_filter is not None:
            out_raw_pre_filter.append(raw)
        raw = normalize_group_name(raw)
        if len(raw) < 4:
            rejected.append({"text": raw, "reason": "too_short_after_norm", "signal": sig})
            return
        if is_department_or_unit_like(raw) and not ownership_signal_strong(sig, sn):
            rejected.append(
                {"text": raw, "reason": "generic_business_unit", "signal": sig}
            )
            return
        ok, reason = is_plausible_org_name(raw)
        if not ok:
            rejected.append({"text": raw, "reason": reason or "implausible", "signal": sig})
            return
        if is_vendor_text(raw):
            rejected.append({"text": raw, "reason": "vendor", "signal": sig})
            return
        w = min(THRESH_ASSIGNED, base * weight)
        found.append((raw, sig, w, sn))

    # Anchored windows
    for rx, sig, base in OWNERSHIP_ANCHOR_PATTERNS:
        for m in rx.finditer(collapsed):
            win = collapsed[m.end() : m.end() + WINDOW_CHARS]
            for cand in extract_org_from_window(win):
                sn = collapsed[max(0, m.start() - 30) : m.end() + 80]
                try_accept(cand, sig, base, sn)

    # Standalone org patterns (small capture groups)
    for rx, sig, base in STANDALONE_ORG_PATTERNS:
        for m in rx.finditer(collapsed):
            raw = m.group(1).strip() if m.lastindex else ""
            sn = collapsed[max(0, m.start() - 40) : m.end() + 40]
            try_accept(raw, sig, base, sn)

    # Copyright — narrow capture, strict filter
    for m in COPYRIGHT_RE.finditer(collapsed):
        raw = m.group(1).strip()
        if out_raw_pre_filter is not None:
            out_raw_pre_filter.append(raw)
        raw = normalize_group_name(raw)
        if len(raw) > 100:
            continue
        base = 0.48
        sn = collapsed[max(0, m.start() - 40) : m.end() + 40]
        if is_department_or_unit_like(raw) and not ownership_signal_strong("copyright", sn):
            rejected.append({"text": raw, "reason": "generic_business_unit", "signal": "copyright"})
            continue
        ok, reason = is_plausible_org_name(raw)
        if not ok:
            rejected.append({"text": raw, "reason": reason or "copyright_implausible", "signal": "copyright"})
            continue
        if has_negative_substring(raw):
            rejected.append({"text": raw, "reason": "copyright_negative", "signal": "copyright"})
            continue
        if is_vendor_text(raw):
            rejected.append({"text": raw, "reason": "vendor", "signal": "copyright"})
            continue
        w = min(THRESH_ASSIGNED, base * weight * (0.88 if page_kind == "footer" else 0.72))
        found.append((raw, "copyright", w, sn))

    seen_k: set[tuple[str, str]] = set()
    deduped: list[tuple[str, str, float, str]] = []
    for t in found:
        k2 = (t[0].lower(), t[1])
        if k2 not in seen_k:
            seen_k.add(k2)
            deduped.append(t)
    return deduped, rejected


def second_pass_extract_candidates(
    blobs: list[tuple[str, str, str]],
    *,
    out_raw_pre_filter: list[str] | None = None,
) -> tuple[list[tuple[str, str, float, str, str, str]], list[dict[str, str]]]:
    """
    When the first pass finds nothing, re-scan for weak but structured group / ownership cues.
    Each accepted tuple ends with (page_url, page_kind) for provenance.
    """
    found: list[tuple[str, str, float, str, str, str]] = []
    rejected: list[dict[str, str]] = []

    def try_accept(raw: str, sig: str, base: float, sn: str, page_url: str, kind: str) -> None:
        if out_raw_pre_filter is not None:
            out_raw_pre_filter.append(raw)
        raw = normalize_group_name(raw)
        if len(raw) < 4:
            rejected.append({"text": raw, "reason": "too_short_after_norm", "signal": sig})
            return
        if is_department_or_unit_like(raw) and not ownership_signal_strong(sig, sn):
            rejected.append(
                {"text": raw, "reason": "generic_business_unit", "signal": sig}
            )
            return
        ok, reason = is_plausible_org_name(raw)
        if not ok:
            rejected.append({"text": raw, "reason": reason or "implausible", "signal": sig})
            return
        if is_vendor_text(raw):
            rejected.append({"text": raw, "reason": "vendor", "signal": sig})
            return
        weight = PAGE_WEIGHT.get(kind, 0.4)
        w = min(THRESH_ASSIGNED, base * weight * _keyword_page_boost(page_url, kind))
        found.append((raw, sig, w, sn, page_url, kind))

    for page_url, kind, text in blobs:
        collapsed = collapse_ws(text)
        if len(collapsed) < 40:
            continue
        blob_l = f"{page_url.lower()} {kind.lower()} {collapsed.lower()}"
        if not any(
            k in blob_l
            for k in (
                " group",
                "collision",
                "automotive",
                "holdings",
                "company",
                "family",
                "owned",
                "part of",
                "member of",
                "sonic",
                "dealership",
                "corporation",
            )
        ):
            continue
        for rx, sig, base in _SECOND_PASS_TRIGGERS:
            for m in rx.finditer(collapsed):
                seg = collapsed[max(0, m.start() - 40) : m.end() + 180]
                for cand in extract_org_from_window(seg):
                    sn = collapsed[max(0, m.start() - 35) : m.end() + 100]
                    try_accept(cand, sig, base, sn, page_url, kind)
        # Re-run standalone patterns on keyword-heavy pages (case / template variants)
        for rx, sig, base in STANDALONE_ORG_PATTERNS:
            for m in rx.finditer(collapsed):
                raw = m.group(1).strip() if m.lastindex else ""
                sn = collapsed[max(0, m.start() - 40) : m.end() + 40]
                try_accept(raw, sig, base * 0.92, sn, page_url, kind)

    seen_k: set[tuple[str, str]] = set()
    deduped: list[tuple[str, str, float, str, str, str]] = []
    for t in found:
        k2 = (t[0].lower(), t[1])
        if k2 not in seen_k:
            seen_k.add(k2)
            deduped.append(t)
    return deduped, rejected


def merge_candidates(
    scored: list[tuple[str, float, Evidence, str]],
) -> tuple[str | None, float, list[str], float, str, str]:
    """
    Rank by composite (specificity × evidence tier × raw confidence), prefer parent orgs over units.
    First returned name is the extraction surface form (before full canonical alias expansion).
    """
    if not scored:
        return None, 0.0, [], 0.0, "", ""
    entries: list[tuple[str, float, object, str, str, bool, str]] = []
    for name, conf, ev, raw_orig in scored:
        entries.append(
            (
                name,
                conf,
                ev,
                raw_orig,
                ev.page_kind,
                ev.cross_domain_evidence,
                ev.snippet,
            )
        )
    ranked = rank_candidate_entries(entries)
    by_canon: dict[str, tuple[float, str, str, str]] = {}
    for name, raw_conf, ev, raw_orig, composite, tier in ranked:
        key_norm = merge_canonical_key(name)
        sig = getattr(ev, "signal", "") or ""
        if key_norm not in by_canon or composite > by_canon[key_norm][0]:
            by_canon[key_norm] = (composite, tier, sig, name)

    best_key = max(by_canon, key=lambda k: by_canon[k][0])
    best_conf, best_tier, best_supporting_signal, best_surface = by_canon[best_key]
    spec = entity_specificity_score(canonical_group_display(best_surface))

    canon_best: dict[str, float] = {}
    for name, raw_conf, ev, raw_orig, composite, tier in ranked:
        kn = merge_canonical_key(name)
        canon_best[kn] = max(composite, canon_best.get(kn, 0.0))

    uniq_keys = sorted(canon_best.keys(), key=lambda k: -canon_best[k])
    uniq: list[str] = []
    for kn in uniq_keys:
        for name, raw_conf, ev, raw_orig, composite, tier in ranked:
            if merge_canonical_key(name) == kn:
                uniq.append(canonical_group_display(name))
                break

    return best_surface, best_conf, uniq, spec, best_tier, best_supporting_signal


def run_inference_on_blobs(
    blobs: list[tuple[str, str, str]],
    *,
    skip_evidence: bool = False,
    dealer_root_domain: str = "",
    out_raw_pre_filter: list[str] | None = None,
) -> tuple[
    list[str],
    str | None,
    str | None,
    float,
    list[Evidence],
    list[dict[str, str]],
    list[str],
    float,
    str,
    str,
    str | None,
]:
    """
    blobs: (url, page_kind, full_text)

    Returns:
        uniq_names, best_raw, best_normalized, confidence, evidence, rejected_all,
        second_pass_candidates (names surfaced only in second pass; may overlap uniq_names)
    """
    if skip_evidence:
        return [], None, None, 0.0, [], [], [], 0.0, "", "", None

    root = dealer_root_domain.strip().lower()
    if not root and blobs:
        root = _host(blobs[0][0])

    all_scored: list[tuple[str, float, Evidence, str]] = []
    evidence: list[Evidence] = []
    rejected_all: list[dict[str, str]] = []
    second_pass_names: list[str] = []

    for page_url, kind, text in blobs:
        cands, rej = extract_candidates_from_text(
            text, page_url, kind, out_raw_pre_filter=out_raw_pre_filter
        )
        rejected_all.extend(rej)
        for raw, sig, w, sn in cands:
            rel = _host(page_url)
            cd = bool(root) and bool(rel) and rel != root
            ev = Evidence(
                snippet=sn[:500],
                page_url=page_url,
                page_kind=kind,
                signal=sig,
                base_score=w,
                weighted_score=w,
                cross_domain_evidence=cd,
                related_domain=rel if cd else None,
            )
            evidence.append(ev)
            penalty = 0.35 if is_vendor_text(raw) else 0.0
            all_scored.append((raw, max(0.0, w - penalty), ev, raw))

    if not all_scored:
        sp_cands, sp_rej = second_pass_extract_candidates(
            blobs, out_raw_pre_filter=out_raw_pre_filter
        )
        rejected_all.extend(sp_rej)
        for raw, sig, w, sn, page_url, kind in sp_cands:
            second_pass_names.append(canonical_group_display(raw))
            rel = _host(page_url)
            cd = bool(root) and bool(rel) and rel != root
            ev = Evidence(
                snippet=sn[:500],
                page_url=page_url,
                page_kind=kind,
                signal=sig,
                base_score=w,
                weighted_score=w,
                cross_domain_evidence=cd,
                related_domain=rel if cd else None,
            )
            evidence.append(ev)
            penalty = 0.35 if is_vendor_text(raw) else 0.0
            all_scored.append((raw, max(0.0, w - penalty), ev, raw))

    if not all_scored:
        return [], None, None, 0.0, evidence, rejected_all, second_pass_names, 0.0, "", "", None

    merge_in: list[tuple[str, float, Evidence, str]] = [
        (t[0], t[1], t[2], t[3]) for t in all_scored
    ]
    best_surface, best_conf, uniq_names, spec_score, ev_tier, best_supporting_signal = (
        merge_candidates(merge_in)
    )
    best_raw = None
    for name, conf, ev, raw_orig in all_scored:
        if merge_canonical_key(name) == merge_canonical_key(best_surface or ""):
            best_raw = raw_orig
            break
    if best_surface and is_vendor_text(canonical_group_display(best_surface)):
        best_conf = max(0.0, best_conf - 0.55)

    best_canon = canonical_group_display(best_surface) if best_surface else None
    return (
        uniq_names,
        best_raw,
        best_surface,
        min(1.0, best_conf),
        evidence,
        rejected_all,
        second_pass_names,
        float(spec_score),
        str(ev_tier),
        str(best_supporting_signal),
        best_canon,
    )
