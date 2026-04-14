"""
Deep BMW USA dealer-locator discovery: HTML/script inspection, interactive Playwright,
expanded network capture with keyword classification, DOM fallback.

Goal: find where dealer data actually lives before committing to a single parser.
"""
from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse

from scrapers.oem.bmw_keyword_sets import (
    BODY_KEYWORDS,
    classify_response_bucket,
    extract_dom_card_guess,
    keyword_hits_in_text,
)
from scrapers.oem.bmw_parse_trace import explain_parse_outcome, top_level_keys

LOCATOR_POSITIVE_TERMS = (
    "dealer",
    "retailer",
    "center",
    "location",
    "zip",
    "postal",
    "map",
    "find a bmw center",
    "local bmw center",
    "dealer locator",
)

GENERIC_UI_TERMS = (
    "keyboard shortcuts",
    "mybmw",
    "site search",
    "global search",
    "menu",
    "account",
    "sign in",
)
GENERIC_RESULT_TERMS = (
    "map view",
    "no results found within 100 miles",
    "mybmw",
    "keyboard shortcuts",
    "choose your local bmw center",
)


def inspect_html_and_inline_scripts(html: str, *, max_html_chars: int = 700_000) -> dict[str, Any]:
    """Extract script-level candidates: keywords, blob hints, snippets (compact)."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {"error": "beautifulsoup4_required", "html_chars": len(html)}

    truncated = len(html) > max_html_chars
    html_use = html[:max_html_chars] if truncated else html
    soup = BeautifulSoup(html_use, "html.parser")
    script_candidates: list[dict[str, Any]] = []

    for i, tag in enumerate(soup.find_all("script")):
        src = (tag.get("src") or "").strip()
        inline = (tag.string or tag.get_text() or "") if tag else ""
        if len(inline) < 100 and not src:
            continue
        kh = keyword_hits_in_text(inline, keywords=BODY_KEYWORDS + ("window.", "__NEXT_DATA__", "hydrat", "graphql", "redux"))
        src_l = src.lower()
        if not kh and not any(
            x in src_l for x in ("bmw", "dealer", "locator", "chunk", "main", "app")
        ):
            continue
        blob_hints: list[str] = []
        if "__NEXT_DATA__" in inline:
            blob_hints.append("__NEXT_DATA__")
        for marker in ('"dealers"', '"locations"', '"outlets"', '"features"', "dealerLocator", "DEALER"):
            if marker in inline:
                blob_hints.append(marker)
        if "window." in inline and re.search(r"dealer|locator|retailer", inline, re.I):
            blob_hints.append("window_global_state_suspected")
        script_candidates.append(
            {
                "index": i,
                "src": src[:800],
                "inline_length": len(inline),
                "keyword_hits": kh[:25],
                "blob_hints": blob_hints[:15],
                "snippet_preview": (inline[:2000] if inline else "")[:2000],
            }
        )

    return {
        "html_chars_total": len(html),
        "html_truncated": truncated,
        "script_candidates": script_candidates[:100],
    }


def _try_parse_json_body(text: str) -> Any | None:
    t = text.strip()
    if not t:
        return None
    if t.startswith(')]}'):
        t = t[4:].lstrip()
    if not t or t[0] not in "{[":
        return None
    try:
        return json.loads(t)
    except Exception:
        return None


def _network_entry_from_response(
    resp: Any,
    *,
    phase: str,
    scraped_at_iso: str,
    parse_fn: Any,
    out_parsed_records: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Single response row with classification + optional parse trace."""
    url = resp.url
    ct = (resp.headers.get("content-type") or "").lower()
    status = resp.status
    try:
        body = resp.body()
    except Exception as e:
        return {
            "phase": phase,
            "url": url,
            "status": status,
            "content_type": ct,
            "outcome": "body_unavailable",
            "error": str(e)[:200],
        }

    if len(body) > 4_000_000:
        return {
            "phase": phase,
            "url": url,
            "status": status,
            "content_type": ct,
            "bytes": len(body),
            "outcome": "skipped_too_large",
        }

    text = body.decode("utf-8", "replace")
    preview = text[:16_000]
    bucket, reason = classify_response_bucket(url, ct, preview)
    kh = keyword_hits_in_text(preview)

    entry: dict[str, Any] = {
        "phase": phase,
        "url": url,
        "status": status,
        "content_type": ct,
        "bytes": len(body),
        "bucket": bucket,
        "classification_reason": reason,
        "keyword_hits_in_body": kh[:20],
    }

    data = _try_parse_json_body(text)
    if data is not None:
        recs, _ = parse_fn(data)
        entry["parsed_row_count"] = len(recs)
        entry["top_level_keys"] = top_level_keys(data)
        entry["parse_rejection_if_empty"] = explain_parse_outcome(data, recs, source_hint=url) if not recs else ""
        if out_parsed_records is not None and recs:
            out_parsed_records.extend(recs)
    else:
        entry["parsed_row_count"] = 0
        entry["not_json_or_parse_failed"] = True
        if kh:
            entry["non_json_but_keyword_hits"] = kh[:15]
        if "protobuf" in ct or "graphql" in ct or "javascript" in ct or "text/plain" in ct:
            entry["body_text_preview"] = preview[:3500]

    return entry


def run_interaction_locator(
    page: Any,
    zip_codes: tuple[str, ...] = ("92606", "90807"),
    screenshot_dir: Path | None = None,
    html_snapshot_dir: Path | None = None,
    selector_overrides: dict[str, str] | None = None,
    ai_selector_adjudicator: Any | None = None,
) -> dict[str, Any]:
    """Cookie accept + ZIP search(es); returns step + coverage diagnostics."""
    log: list[str] = []
    zip_observations: list[dict[str, Any]] = []
    accept_selectors = [
        'button:has-text("Accept All")',
        'button:has-text("Accept")',
        "#onetrust-accept-btn-handler",
        '[id*="accept" i][role="button"]',
        "button.ot-sdk-button",
        'button[aria-label*="Accept" i]',
    ]
    for sel in accept_selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=5000)
                page.wait_for_timeout(1200)
                log.append(f"clicked:{sel}")
                break
        except Exception as e:
            log.append(f"no_click:{sel}:{str(e)[:80]}")

    _expand_locator_ui_if_needed(page, log)
    shell_initial = _detect_locator_shell_state(page)
    initial_controls = _dump_locator_controls(page)
    _html_snapshot(page, html_snapshot_dir, "initial_page_load")
    chosen = _discover_locator_zip_input(page)
    trial_candidates = chosen.get("trial_candidates") or []
    manual_input_selector = str((selector_overrides or {}).get("input_selector") or "").strip()
    manual_result_selector = str((selector_overrides or {}).get("result_selector") or "").strip()
    if manual_input_selector:
        trial_candidates = [{"selector": manual_input_selector, "index": 0, "score": 999, "manual_override": True}] + trial_candidates
        log.append(f"manual_input_selector_override:{manual_input_selector}")
    ai_selector_plan: dict[str, Any] = {}
    if ai_selector_adjudicator is not None:
        try:
            evidence = _build_ai_selector_evidence(
                page=page,
                input_candidates=chosen.get("candidates") or [],
                controls=initial_controls,
            )
            ai_selector_plan = _normalize_ai_selector_plan(ai_selector_adjudicator(evidence))
            log.append("ai_selector_adjudicator_used")
            for c in ai_selector_plan.get("locator_input_candidates") or []:
                sel = str(c.get("selector_or_description") or "").strip()
                if sel:
                    trial_candidates.insert(
                        0,
                        {"selector": sel, "index": 0, "score": 1200, "from_ai": True, "ai_confidence": c.get("confidence")},
                    )
        except Exception as e:
            log.append(f"ai_selector_adjudicator_error:{str(e)[:120]}")
    zip_input_found = False
    zip_selector_used = ""
    input_debug: list[dict[str, Any]] = chosen["candidates"]
    chosen_input_context: dict[str, Any] = chosen.get("chosen_context") or {}
    prev_names: set[str] = set()
    global_start = _now_ms()
    for cand in trial_candidates[:5]:
        sel = str(cand.get("selector") or "")
        idx = int(cand.get("index") or 0)
        candidate_trial_log: list[dict[str, Any]] = []
        if not sel:
            continue
        try:
            loc = page.locator(sel)
            if loc.count() <= idx:
                candidate_trial_log.append({"reason": "selector_index_not_found"})
                continue
            target = loc.nth(idx)
            target.scroll_into_view_if_needed(timeout=2500)
            target.wait_for(state="visible", timeout=8000)
            if not _input_looks_locator_context(target):
                log.append(f"rejected_non_locator_input:{sel}")
                candidate_trial_log.append({"reason": "rejected_non_locator_context"})
                continue
            if not _candidate_is_locator_specific(cand):
                log.append(f"rejected_low_locator_specificity:{sel}")
                candidate_trial_log.append({"reason": "rejected_low_locator_specificity"})
                continue
            zip_input_found = True
            zip_selector_used = f"{sel} ::nth({idx})"
            for zip_code in zip_codes:
                t0 = _now_ms()
                mode_info = _detect_locator_mode(page)
                rec: dict[str, Any] = {
                    "zip": zip_code,
                    "zip_entered": zip_code,
                    "search_triggered": False,
                    "zip_input_not_found": False,
                    "search_submit_not_found": False,
                    "no_ui_change_detected": False,
                    "same_result_set_as_previous": False,
                    "detail_cards_not_detected": False,
                    "interaction_errors": [],
                    "input_selector_found": True,
                    "input_selector_used": zip_selector_used,
                    "typing_succeeded": False,
                    "enter_triggered": False,
                    "search_button_clicked": False,
                    "result_container_appeared": False,
                    "wait_ms": 0,
                    "candidate_inputs_tried": [],
                    "input_tag": cand.get("tag") or "",
                    "input_type": cand.get("type") or "",
                    "input_placeholder": cand.get("placeholder") or "",
                    "input_aria_label": cand.get("aria_label") or "",
                    "input_name": cand.get("name") or "",
                    "input_id": cand.get("id") or "",
                    "input_class": cand.get("class") or "",
                    "input_heading_text": cand.get("heading_text") or "",
                    "input_container_class": cand.get("container_class") or "",
                    "input_container_text_preview": cand.get("container_text_preview") or "",
                    "input_container_attrs": cand.get("container_attrs") or "",
                    "input_dom_path_hint": cand.get("dom_path_hint") or "",
                    "input_own_attrs_summary": cand.get("own_attrs") or "",
                    "autocomplete_selected": False,
                    "commit_mode": "none",
                    "suggestions_detected": False,
                    "suggestion_texts": [],
                    "locator_mode_detected": mode_info.get("active_mode") or mode_info.get("mode_summary") or "unknown",
                    "search_by_location_selected": False,
                    "see_list_results_clicked": False,
                    "locator_empty_state": False,
                    "empty_state_text": "",
                    "dealer_cards_appeared": False,
                    "locator_shell_state_initial": shell_initial,
                    "locator_shell_state_after_submit": "",
                    "controls_before": initial_controls[:40],
                    "controls_after_submit": [],
                    "result_state": "unknown",
                }
                try:
                    _shot(page, screenshot_dir, f"{zip_code}_before_interaction")
                    if _activate_search_by_location(
                        page, log, ai_selector_plan.get("location_mode_candidates") or []
                    ):
                        rec["search_by_location_selected"] = True
                    mode_after_select = _detect_locator_mode(page)
                    rec["locator_mode_detected"] = (
                        mode_after_select.get("active_mode")
                        or mode_after_select.get("mode_summary")
                        or rec["locator_mode_detected"]
                    )
                    target.click(timeout=3000)
                    target.fill("", timeout=3000)
                    target.type(zip_code, delay=70, timeout=5000)
                    rec["typing_succeeded"] = True
                    _shot(page, screenshot_dir, f"{zip_code}_after_zip_entry")
                    sug = _select_autocomplete_suggestion(page, zip_code)
                    rec["suggestions_detected"] = bool(sug["detected"])
                    rec["suggestion_texts"] = sug["texts"][:8]
                    if sug["clicked"]:
                        rec["autocomplete_selected"] = True
                        rec["commit_mode"] = "suggestion_clicked"
                    target.press("Enter")
                    rec["enter_triggered"] = True
                    if rec["commit_mode"] == "none":
                        rec["commit_mode"] = "enter_only"
                    if _click_search_button_if_present(page):
                        rec["search_button_clicked"] = True
                        rec["commit_mode"] = (
                            "search_button_only"
                            if rec["commit_mode"] == "none"
                            else rec["commit_mode"] + "+search_button"
                        )
                        _shot(page, screenshot_dir, f"{zip_code}_after_search_button_click")
                    log.append(f"zip_entered:{zip_code} via {sel}")
                    rec["search_triggered"] = True
                    _shot(page, screenshot_dir, f"{zip_code}_after_submit")
                    _html_snapshot(page, html_snapshot_dir, f"{zip_code}_after_enter")
                    if _click_see_list_results(
                        page, log, ai_selector_plan.get("list_results_candidates") or []
                    ):
                        rec["see_list_results_clicked"] = True
                    _html_snapshot(page, html_snapshot_dir, f"{zip_code}_after_see_list_results")
                    _shot(page, screenshot_dir, f"{zip_code}_after_list_results_click")
                    snap = _dealer_results_snapshot(
                        page,
                        timeout_ms=12_000,
                        preferred_selector=manual_result_selector,
                        ai_preferred_candidates=ai_selector_plan.get("dealer_result_container_candidates") or [],
                    )
                    rec["result_container_appeared"] = snap["accepted"]
                    page.wait_for_timeout(2200)
                    _attempt_marker_clicks(page, log)
                    _shot(page, screenshot_dir, f"{zip_code}_final_state")
                    names = snap["dealer_names"]
                    quality = _zip_quality_snapshot(page)
                    curr = set(n.lower() for n in names)
                    rec["result_container_selector"] = snap["selector"]
                    rec["result_container_text_preview"] = snap["text_preview"]
                    rec["result_container_reason"] = snap["reason"]
                    rec["result_container_accepted"] = snap["accepted"]
                    rec["result_container_candidates_tried"] = snap.get("candidates_tried") or []
                    empty_state = _detect_locator_empty_state(page)
                    rec["locator_empty_state"] = empty_state["present"]
                    rec["empty_state_text"] = empty_state["text"]
                    rec["locator_shell_state_after_submit"] = _detect_locator_shell_state(page)
                    rec["controls_after_submit"] = _dump_locator_controls(page)[:40]
                    rec["dealer_cards_appeared"] = snap["accepted"] and (len(names) > 0)
                    rec["result_state"] = _classify_result_state(
                        page=page,
                        empty_state=rec["locator_empty_state"],
                        dealer_cards_appeared=rec["dealer_cards_appeared"],
                        see_list_clicked=rec["see_list_results_clicked"],
                    )
                    if snap["accepted"]:
                        rec["visible_result_count"] = len(names)
                        rec["visible_dealer_names"] = names[:25]
                    else:
                        rec["visible_result_count"] = 0
                        rec["visible_dealer_names"] = []
                        if rec["locator_empty_state"]:
                            rec["interaction_errors"] = list(rec.get("interaction_errors") or []) + [
                                "locator_empty_state_detected"
                            ]
                        else:
                            rec["interaction_errors"] = list(rec.get("interaction_errors") or []) + [
                                "result_container_rejected_non_dealer"
                            ]
                    rec["results_changed_from_previous_zip"] = (curr != prev_names)
                    rec["same_result_set_as_previous"] = (curr == prev_names)
                    rec["usable_visible_rows_estimate"] = quality["usable"]
                    rec["partial_visible_rows_estimate"] = quality["partial"]
                    rec["detail_cards_not_detected"] = quality["cards_detected"] == 0
                    rec["no_ui_change_detected"] = bool(prev_names) and (curr == prev_names)
                    prev_names = curr
                except Exception as e:
                    log.append(f"zip_fail:{zip_code}:{sel}:{str(e)[:100]}")
                    rec["search_triggered"] = False
                    rec["search_submit_not_found"] = True
                    rec["interaction_errors"] = [str(e)[:180]]
                    if rec["typing_succeeded"] and rec["commit_mode"] == "none":
                        rec["commit_mode"] = "typed_only"
                rec["wait_ms"] = _now_ms() - t0
                rec["candidate_inputs_tried"] = [
                    {
                        "selector": sel,
                        "index": idx,
                        "score": cand.get("score"),
                        "heading_text": cand.get("heading_text"),
                        "container_class": cand.get("container_class"),
                        "reason": "tried",
                    }
                ] + candidate_trial_log
                zip_observations.append(rec)
            break
        except Exception as e:
            log.append(f"zip_input_fail:{sel}:{str(e)[:120]}")
            continue
    if not zip_input_found:
        log.append("no_zip_input_locator_found")
        input_debug = _dump_visible_inputs(page)
        for zip_code in zip_codes:
            zip_observations.append(
                {
                    "zip": zip_code,
                    "zip_entered": zip_code,
                    "search_triggered": False,
                    "zip_input_not_found": True,
                    "search_submit_not_found": True,
                    "no_ui_change_detected": True,
                    "same_result_set_as_previous": False,
                    "detail_cards_not_detected": True,
                    "visible_result_count": 0,
                    "visible_dealer_names": [],
                    "usable_visible_rows_estimate": 0,
                    "partial_visible_rows_estimate": 0,
                    "interaction_errors": ["zip_input_not_found"],
                    "input_selector_found": False,
                    "input_selector_used": "",
                    "typing_succeeded": False,
                    "enter_triggered": False,
                    "search_button_clicked": False,
                    "result_container_appeared": False,
                    "locator_mode_detected": "unknown",
                    "search_by_location_selected": False,
                    "see_list_results_clicked": False,
                    "locator_empty_state": False,
                    "empty_state_text": "",
                    "dealer_cards_appeared": False,
                    "result_state": "unknown",
                    "ai_selector_plan_used": bool(ai_selector_plan),
                    "wait_ms": 0,
                }
            )

    try:
        page.wait_for_load_state("networkidle", timeout=50_000)
    except Exception:
        log.append("networkidle_timeout_after_zip")
    page.wait_for_timeout(6000)
    return {
        "steps": log,
        "zip_observations": zip_observations,
        "zip_input_found": zip_input_found,
        "zip_selector_used": zip_selector_used,
        "zip_input_context": chosen_input_context,
        "zip_input_candidates": input_debug[:40],
        "visible_input_dump": input_debug,
        "screenshot_dir": str(screenshot_dir) if screenshot_dir else "",
        "html_snapshot_dir": str(html_snapshot_dir) if html_snapshot_dir else "",
        "selector_overrides": selector_overrides or {},
        "ai_selector_plan": ai_selector_plan,
        "locator_shell_state_initial": shell_initial,
        "visible_locator_controls_initial": initial_controls[:60],
        "locator_page_structure": _dump_locator_page_structure(page),
        "interaction_elapsed_ms": _now_ms() - global_start,
    }


def _candidate_zip_input_selectors() -> list[str]:
    return [
        'input[placeholder*="ZIP" i]',
        'input[placeholder*="Postal" i]',
        'input[placeholder*="location" i]',
        'input[aria-label*="ZIP" i]',
        'input[aria-label*="location" i]',
        'input[name*="postal" i]',
        'input[name*="zip" i]',
        'input[id*="zip" i]',
        'input[id*="postal" i]',
        '[role="textbox"][aria-label*="zip" i]',
        '[role="textbox"][aria-label*="location" i]',
        '[class*="locator" i] input[type="text"]',
        'input[type="text"]',
    ]


def _candidate_is_locator_specific(cand: dict[str, Any]) -> bool:
    hits = [str(x).lower() for x in (cand.get("positive_hits") or [])]
    if not hits:
        return False
    required = ("zip", "postal", "location", "dealer", "center", "map", "local bmw center")
    return any(any(r in h for r in required) for h in hits)


def _input_looks_locator_context(el: Any) -> bool:
    try:
        ctx = el.evaluate(
            """e => {
                const own = `${e.placeholder||''} ${e.getAttribute('aria-label')||''} ${e.name||''} ${e.id||''} ${e.className||''}`;
                const c = e.closest('[class*="dealer" i],[class*="locator" i],[class*="retailer" i],[class*="location" i],section,form,main,div');
                const ctext = c ? (c.innerText || '') : '';
                return { own, containerText: ctext.slice(0, 800), containerClass: c ? (c.className || '') : '' };
            }"""
        )
    except Exception:
        return False
    own = (ctx.get("own") or "").lower()
    ctext = (ctx.get("containerText") or "").lower()
    cclass = (ctx.get("containerClass") or "").lower()
    blob = f"{own} {ctext} {cclass}"
    pos = sum(1 for t in LOCATOR_POSITIVE_TERMS if t in blob)
    neg = sum(1 for t in GENERIC_UI_TERMS if t in blob)
    if "zip" in own or "postal" in own:
        pos += 2
    return pos >= 2 and neg < pos


def _discover_locator_zip_input(page: Any) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    chosen_selector = ""
    chosen_context: dict[str, Any] = {}
    best_score = -999
    candidates = _enumerate_visible_input_candidates(page)
    for rec in candidates:
        score = int(rec.get("score") or 0)
        if score > best_score and score >= 2:
            best_score = score
            chosen_selector = str(rec.get("selector") or "")
            chosen_context = rec
    trial_candidates = sorted(
        candidates,
        key=lambda x: (
            int(x.get("score") or -999),
            int(x.get("selector_specificity") or 0),
            0 if bool(x.get("generic_selector")) else 1,
        ),
        reverse=True,
    )
    return {
        "selector": chosen_selector,
        "chosen_context": chosen_context,
        "candidates": candidates,
        "trial_candidates": trial_candidates,
    }


def _enumerate_visible_input_candidates(page: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        loc = page.locator("input, [role='textbox']")
        n = min(loc.count(), 120)
    except Exception:
        return out
    for i in range(n):
        try:
            el = loc.nth(i)
            if not el.is_visible():
                continue
            ctx = el.evaluate(
                """e => {
                    const own = `${e.placeholder||''} ${e.getAttribute('aria-label')||''} ${e.name||''} ${e.id||''} ${e.className||''}`;
                    const c = e.closest('section,form,main,div');
                    const ctext = c ? (c.innerText || '') : '';
                    const heading = c ? (c.querySelector('h1,h2,h3,h4,label,[aria-label]')?.innerText || c.querySelector('h1,h2,h3,h4,label,[aria-label]')?.getAttribute('aria-label') || '') : '';
                    const path = [];
                    let p = e;
                    let depth = 0;
                    while (p && depth < 4) {
                        path.push((p.tagName||'') + '#' + (p.id||'') + '.' + ((p.className||'').toString().split(' ').slice(0,2).join('.')));
                        p = p.parentElement;
                        depth++;
                    }
                    return {
                        own,
                        containerText: ctext.slice(0, 900),
                        containerClass: c ? (c.className || '') : '',
                        containerAttrs: c ? (`id=${c.id||''} role=${c.getAttribute('role')||''} data-test=${c.getAttribute('data-testid')||''}`) : '',
                        heading: (heading||'').slice(0, 200),
                        tag: e.tagName || '',
                        type: e.getAttribute('type') || '',
                        placeholder: e.getAttribute('placeholder') || '',
                        aria_label: e.getAttribute('aria-label') || '',
                        name: e.getAttribute('name') || '',
                        id: e.getAttribute('id') || '',
                        className: e.getAttribute('class') || '',
                        dom_path_hint: path.join(' > ')
                    };
                }"""
            )
            own = (ctx.get("own") or "").lower()
            blob = f"{own} {(ctx.get('containerText') or '').lower()} {(ctx.get('containerClass') or '').lower()} {(ctx.get('heading') or '').lower()}"
            pos = sum(1 for t in LOCATOR_POSITIVE_TERMS if t in blob)
            neg = sum(1 for t in GENERIC_UI_TERMS if t in blob)
            if "zip" in own or "postal" in own:
                pos += 2
            score = pos - neg
            selector = "input, [role='textbox']"
            generic_selector = True
            el_id = str(ctx.get("id") or "").strip()
            el_name = str(ctx.get("name") or "").strip()
            if el_id and re.match(r"^[A-Za-z][A-Za-z0-9_-]*$", el_id):
                selector = f"#{el_id}"
                generic_selector = False
            elif el_name and re.match(r"^[A-Za-z][A-Za-z0-9_-]*$", el_name):
                selector = f'input[name="{el_name}"], [role="textbox"][name="{el_name}"]'
                generic_selector = False
            out.append(
                {
                    "selector": selector,
                    "index": i,
                    "score": score,
                    "selector_specificity": 2 if not generic_selector else 0,
                    "generic_selector": generic_selector,
                    "positive_hits": [t for t in LOCATOR_POSITIVE_TERMS if t in blob][:8],
                    "negative_hits": [t for t in GENERIC_UI_TERMS if t in blob][:8],
                    "heading_text": ctx.get("heading") or "",
                    "container_class": (ctx.get("containerClass") or "")[:240],
                    "container_attrs": (ctx.get("containerAttrs") or "")[:240],
                    "container_text_preview": (ctx.get("containerText") or "")[:300],
                    "own_attrs": (ctx.get("own") or "")[:240],
                    "tag": ctx.get("tag") or "",
                    "type": ctx.get("type") or "",
                    "placeholder": ctx.get("placeholder") or "",
                    "aria_label": ctx.get("aria_label") or "",
                    "name": ctx.get("name") or "",
                    "id": ctx.get("id") or "",
                    "class": (ctx.get("className") or "")[:220],
                    "dom_path_hint": (ctx.get("dom_path_hint") or "")[:280],
                    "visible": True,
                    "enabled": True,
                }
            )
        except Exception:
            continue
    return out


def _expand_locator_ui_if_needed(page: Any, log: list[str]) -> None:
    selectors = [
        'button:has-text("Find a Dealer")',
        'button:has-text("Find a BMW Center")',
        'button:has-text("Search")',
        '[aria-label*="Find a Dealer" i]',
        '[class*="locator" i] button',
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            loc.first.scroll_into_view_if_needed(timeout=1500)
            loc.first.click(timeout=1500)
            page.wait_for_timeout(700)
            log.append(f"expanded_locator_ui:{sel}")
            return
        except Exception:
            continue


def _click_search_button_if_present(page: Any) -> bool:
    sels = [
        'button:has-text("Search")',
        'button:has-text("Find")',
        '[aria-label*="search" i]',
        '[type="submit"]',
    ]
    for sel in sels:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            loc.first.click(timeout=1000)
            page.wait_for_timeout(500)
            return True
        except Exception:
            continue
    return False


def _detect_locator_mode(page: Any) -> dict[str, str]:
    text = ""
    try:
        text = (page.inner_text("body", timeout=2000) or "").lower()
    except Exception:
        text = ""
    has_location = "search by location" in text
    has_dealer_name = "search by dealer name" in text
    active_mode = "unknown"
    try:
        active = page.locator(
            ':is(button,a,[role="tab"]):has-text("Search by location"), :is(button,a,[role="tab"]):has-text("Search by dealer name")'
        )
        n = min(active.count(), 4)
        for i in range(n):
            el = active.nth(i)
            cl = (el.get_attribute("class") or "").lower()
            sel = (el.get_attribute("aria-selected") or "").lower()
            val = (el.inner_text(timeout=600) or "").lower()
            if ("active" in cl or sel == "true") and "location" in val:
                active_mode = "location"
                break
            if ("active" in cl or sel == "true") and "dealer name" in val:
                active_mode = "dealer_name"
                break
    except Exception:
        pass
    if active_mode == "unknown":
        if has_location and not has_dealer_name:
            active_mode = "location"
        elif has_dealer_name and not has_location:
            active_mode = "dealer_name"
    return {
        "active_mode": active_mode,
        "mode_summary": f"location_present={has_location},dealer_name_present={has_dealer_name}",
    }


def _activate_search_by_location(page: Any, log: list[str], ai_candidates: list[dict[str, Any]] | None = None) -> bool:
    for c in ai_candidates or []:
        sel = str(c.get("selector_or_description") or "").strip()
        if not sel:
            continue
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=1400)
                page.wait_for_timeout(500)
                log.append(f"selected_mode_location_ai:{sel}")
                return True
        except Exception:
            continue
    sels = [
        ':is(button,a,[role="tab"]):has-text("Search by location")',
        ':is(button,a,[role="tab"]):has-text("Location")',
    ]
    for sel in sels:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 4)
            for i in range(n):
                el = loc.nth(i)
                if not el.is_visible():
                    continue
                el.click(timeout=1200)
                page.wait_for_timeout(500)
                log.append(f"selected_mode_location:{sel}#{i}")
                return True
        except Exception:
            continue
    return False


def _click_see_list_results(page: Any, log: list[str], ai_candidates: list[dict[str, Any]] | None = None) -> bool:
    for c in ai_candidates or []:
        sel = str(c.get("selector_or_description") or "").strip()
        if not sel:
            continue
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=1500)
                page.wait_for_timeout(700)
                log.append(f"clicked_list_results_ai:{sel}")
                return True
        except Exception:
            continue
    sels = [
        ':is(button,a,[role="tab"]):has-text("See List Results")',
        ':is(button,a,[role="tab"]):has-text("List Results")',
        ':is(button,a,[role="tab"]):has-text("List View")',
    ]
    for sel in sels:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 3)
            for i in range(n):
                el = loc.nth(i)
                if not el.is_visible():
                    continue
                el.click(timeout=1500)
                page.wait_for_timeout(700)
                log.append(f"clicked_list_results:{sel}#{i}")
                return True
        except Exception:
            continue
    return False


def _select_autocomplete_suggestion(page: Any, zip_code: str) -> dict[str, Any]:
    sels = [
        '[role="listbox"] [role="option"]',
        '[role="combobox"] + [role="listbox"] [role="option"]',
        '[aria-expanded="true"] [role="option"]',
        '[class*="location" i] [role="option"]',
        '[class*="place" i] [role="option"]',
        '[class*="suggest" i] [role="option"]',
        '[class*="autocomplete" i] li',
        '[class*="location" i] li',
        '[class*="place" i] li',
        '[class*="suggest" i] li',
    ]
    z = (zip_code or "").strip()
    detected = False
    texts: list[str] = []
    for sel in sels:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 8)
            if n == 0:
                continue
            detected = True
            chosen = None
            for i in range(n):
                t = (loc.nth(i).inner_text(timeout=700) or "").lower()
                if t:
                    texts.append(t[:180])
                if z in t or "bmw" in t or "center" in t or "dealer" in t:
                    chosen = loc.nth(i)
                    break
            if chosen is None:
                chosen = loc.first
            chosen.click(timeout=1200)
            page.wait_for_timeout(500)
            return {"detected": detected, "clicked": True, "texts": texts}
        except Exception:
            continue
    return {"detected": detected, "clicked": False, "texts": texts}


def _detect_locator_empty_state(page: Any) -> dict[str, Any]:
    probes = [
        "no results found within 100 miles",
        "no results found",
    ]
    try:
        body = (page.inner_text("body", timeout=1800) or "").strip()
    except Exception:
        body = ""
    low = body.lower()
    for p in probes:
        if p in low:
            idx = low.find(p)
            frag = body[max(0, idx - 80): idx + len(p) + 80]
            return {"present": True, "text": frag[:240]}
    return {"present": False, "text": ""}


def _detect_locator_shell_state(page: Any) -> str:
    try:
        body = (page.inner_text("body", timeout=2000) or "").lower()
    except Exception:
        body = ""
    states: list[str] = []
    if "search by location" in body and "search by dealer name" in body:
        states.append("shell_modes_visible")
    mode = _detect_locator_mode(page).get("active_mode") or "unknown"
    if mode == "location":
        states.append("search_by_location_mode")
    elif mode == "dealer_name":
        states.append("search_by_dealer_name_mode")
    if "map view" in body:
        states.append("map_view_visible")
    if "see list results" in body or "list results" in body:
        states.append("list_toggle_visible")
    if "no results found within 100 miles" in body or "no results found" in body:
        states.append("empty_state")
    if not states:
        states.append("initial_shell_or_unknown")
    return ",".join(states)


def _dump_locator_controls(page: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    sels = [
        "button",
        "a",
        '[role="tab"]',
    ]
    keywords = (
        "search by location",
        "search by dealer name",
        "map view",
        "see list results",
        "dealer locator",
        "bmw center",
    )
    for sel in sels:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 120)
        except Exception:
            continue
        for i in range(n):
            try:
                el = loc.nth(i)
                if not el.is_visible():
                    continue
                txt = (el.inner_text(timeout=300) or "").strip()
                if not txt:
                    continue
                low = txt.lower()
                if not any(k in low for k in keywords):
                    continue
                out.append(
                    {
                        "selector": sel,
                        "index": i,
                        "text": txt[:180],
                        "aria_label": (el.get_attribute("aria-label") or "")[:160],
                        "id": (el.get_attribute("id") or "")[:120],
                        "class": (el.get_attribute("class") or "")[:220],
                        "role": (el.get_attribute("role") or "")[:80],
                        "nearby_text": (el.evaluate(
                            """e => {
                                const c = e.closest('section,main,form,div');
                                return ((c && c.innerText) || '').slice(0, 220);
                            }"""
                        ) or "")[:220],
                        "dom_path_hint": (el.evaluate(
                            """e => {
                                const path=[]; let p=e; let d=0;
                                while (p && d<5){ path.push((p.tagName||'')+'#'+(p.id||'')+'.'+((p.className||'').toString().split(' ').slice(0,2).join('.'))); p=p.parentElement; d++; }
                                return path.join(' > ');
                            }"""
                        ) or "")[:280],
                        "bounding_box": _bounding_box(el),
                    }
                )
            except Exception:
                continue
    return out[:120]


def _wait_for_result_container(page: Any, timeout_ms: int = 12_000) -> bool:
    sels = [
        '[class*="result" i]',
        '[class*="dealer" i]',
        '[class*="location" i]',
        '[class*="retailer" i]',
        "article",
    ]
    start = _now_ms()
    while (_now_ms() - start) < timeout_ms:
        for sel in sels:
            try:
                if page.locator(sel).count() > 0:
                    return True
            except Exception:
                continue
        page.wait_for_timeout(250)
    return False


def _dealer_results_snapshot(
    page: Any,
    timeout_ms: int = 12_000,
    preferred_selector: str = "",
    ai_preferred_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    _wait_for_result_container(page, timeout_ms=timeout_ms)
    selectors = [
        '[class*="list" i] [class*="card" i]',
        '[class*="list" i] article',
        '[class*="results" i] [class*="card" i]',
        '[class*="dealer" i]',
        '[class*="retailer" i]',
        '[class*="location" i]',
        '[class*="result" i]',
        "article",
    ]
    if preferred_selector:
        selectors = [preferred_selector] + selectors
    for c in ai_preferred_candidates or []:
        sel = str(c.get("selector_or_description") or "").strip()
        if sel:
            selectors = [sel] + selectors
    best = {
        "accepted": False,
        "selector": "",
        "reason": "no_candidate_container",
        "text_preview": "",
        "dealer_names": [],
        "candidates_tried": [],
    }
    candidates_tried: list[dict[str, Any]] = []
    for sel in selectors:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 25)
            if n == 0:
                continue
            texts: list[str] = []
            names: list[str] = []
            dealer_like_cards = 0
            for i in range(n):
                t = (loc.nth(i).inner_text(timeout=1000) or "").strip()
                if not t:
                    continue
                texts.append(t[:260])
                nm = t.splitlines()[0].strip() if t.splitlines() else ""
                if _looks_like_dealer_card(t):
                    dealer_like_cards += 1
                    if nm and nm.lower() not in {x.lower() for x in names}:
                        names.append(nm[:160])
            preview = " | ".join(texts[:4])[:700]
            repeated_structure = n >= 2
            accepted = dealer_like_cards >= 1 and repeated_structure and not _is_generic_ui_preview(preview)
            reason = (
                f"accepted_dealer_like_cards:{dealer_like_cards};repeated:{repeated_structure}"
                if accepted
                else f"rejected_dealer_like_cards:{dealer_like_cards};repeated:{repeated_structure}"
            )
            candidates_tried.append(
                {
                    "selector": sel,
                    "dom_path_hint": _dom_path_hint(loc.first),
                    "nearby_heading_text": _nearby_heading_text(loc.first),
                    "dealer_like_cards": dealer_like_cards,
                    "accepted": accepted,
                    "reason": reason,
                    "text_preview": preview[:300],
                    "names_preview": names[:8],
                }
            )
            cand = {
                "accepted": accepted,
                "selector": sel,
                "reason": reason,
                "text_preview": preview,
                "dealer_names": names[:25],
                "candidates_tried": candidates_tried[:40],
            }
            if accepted:
                return cand
            if len(names) > len(best.get("dealer_names", [])):
                best = cand
        except Exception:
            continue
    best["candidates_tried"] = candidates_tried[:40]
    return best


def _looks_like_dealer_card(text: str) -> bool:
    t = (text or "").lower()
    if not t:
        return False
    if _is_generic_ui_preview(t):
        return False
    has_bmw = "bmw" in t
    has_addr = bool(re.search(r"\b[A-Za-z .'-]+,\s*[A-Z]{2}\s+\d{5}\b", text))
    has_phone = bool(re.search(r"\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}", text))
    has_direction = "direction" in t or "map" in t
    return (has_bmw and (has_addr or has_phone)) or (has_addr and has_phone) or (has_bmw and has_direction)


def _is_generic_ui_preview(text: str) -> bool:
    t = (text or "").lower()
    return any(x in t for x in GENERIC_RESULT_TERMS)


def _dump_visible_inputs(page: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        loc = page.locator("input, [role='textbox']")
        n = min(loc.count(), 80)
    except Exception:
        return out
    for i in range(n):
        try:
            el = loc.nth(i)
            visible = el.is_visible()
            if not visible:
                continue
            out.append(
                {
                    "tag": el.evaluate("e => e.tagName"),
                    "type": (el.get_attribute("type") or ""),
                    "placeholder": (el.get_attribute("placeholder") or ""),
                    "aria_label": (el.get_attribute("aria-label") or ""),
                    "id": (el.get_attribute("id") or ""),
                    "name": (el.get_attribute("name") or ""),
                    "class": (el.get_attribute("class") or "")[:220],
                    "dom_path_hint": (el.evaluate(
                        """e => {
                            const path=[]; let p=e; let d=0;
                            while (p && d<5){ path.push((p.tagName||'')+'#'+(p.id||'')+'.'+((p.className||'').toString().split(' ').slice(0,2).join('.'))); p=p.parentElement; d++; }
                            return path.join(' > ');
                        }"""
                    ) or "")[:300],
                    "nearby_text": (el.evaluate(
                        """e => {
                            const c = e.closest('section,main,form,div');
                            return ((c && c.innerText) || '').slice(0, 220);
                        }"""
                    ) or "")[:220],
                    "parent_container_meta": (el.evaluate(
                        """e => {
                            const c = e.closest('section,main,form,div');
                            if (!c) return '';
                            return `id=${c.id||''} class=${(c.className||'').toString().slice(0,120)} role=${c.getAttribute('role')||''}`;
                        }"""
                    ) or "")[:240],
                    "bounding_box": _bounding_box(el),
                }
            )
        except Exception:
            continue
    return out[:80]


def _now_ms() -> int:
    import time

    return int(time.time() * 1000)


def _shot(page: Any, out_dir: Path | None, name: str) -> None:
    if out_dir is None:
        return
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
        page.screenshot(path=str(out_dir / f"{safe}.png"), full_page=False)
    except Exception:
        return


def _bounding_box(el: Any) -> dict[str, float] | None:
    try:
        b = el.bounding_box()
        if not b:
            return None
        return {
            "x": round(float(b.get("x", 0.0)), 1),
            "y": round(float(b.get("y", 0.0)), 1),
            "width": round(float(b.get("width", 0.0)), 1),
            "height": round(float(b.get("height", 0.0)), 1),
        }
    except Exception:
        return None


def _dom_path_hint(el: Any) -> str:
    try:
        return (el.evaluate(
            """e => {
                const path=[]; let p=e; let d=0;
                while (p && d<5){ path.push((p.tagName||'')+'#'+(p.id||'')+'.'+((p.className||'').toString().split(' ').slice(0,2).join('.'))); p=p.parentElement; d++; }
                return path.join(' > ');
            }"""
        ) or "")[:280]
    except Exception:
        return ""


def _nearby_heading_text(el: Any) -> str:
    try:
        return (el.evaluate(
            """e => {
                const c = e.closest('section,main,form,div');
                if (!c) return '';
                const h = c.querySelector('h1,h2,h3,h4,label,[aria-label]');
                if (!h) return '';
                return (h.innerText || h.getAttribute('aria-label') || '').slice(0, 180);
            }"""
        ) or "")[:180]
    except Exception:
        return ""


def _html_snapshot(page: Any, out_dir: Path | None, name: str) -> None:
    if out_dir is None:
        return
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
        html = page.content()
        (out_dir / f"{safe}.html").write_text(html, encoding="utf-8")
    except Exception:
        return


def _classify_result_state(
    *,
    page: Any,
    empty_state: bool,
    dealer_cards_appeared: bool,
    see_list_clicked: bool,
) -> str:
    if dealer_cards_appeared:
        return "list_results"
    if empty_state:
        return "empty_state"
    try:
        body = (page.inner_text("body", timeout=1500) or "").lower()
    except Exception:
        body = ""
    if "map view" in body and "see list results" in body:
        return "generic_shell"
    if "map view" in body and not see_list_clicked:
        return "map_only"
    if "search by location" in body or "search by dealer name" in body:
        return "generic_shell"
    return "unknown"


def _dump_locator_page_structure(page: Any) -> dict[str, Any]:
    return {
        "visible_inputs": _dump_visible_inputs(page)[:120],
        "visible_controls": _dump_locator_controls(page)[:120],
    }


def _build_ai_selector_evidence(
    *,
    page: Any,
    input_candidates: list[dict[str, Any]],
    controls: list[dict[str, Any]],
) -> dict[str, Any]:
    body = ""
    try:
        body = (page.inner_text("body", timeout=1800) or "")[:2400]
    except Exception:
        body = ""
    return {
        "page_state": _detect_locator_shell_state(page),
        "mode_detected": _detect_locator_mode(page),
        "inputs": (input_candidates or [])[:25],
        "controls": (controls or [])[:40],
        "result_containers_preview": (_dealer_results_snapshot(page, timeout_ms=5000).get("candidates_tried") or [])[:20],
        "dom_summary": _dump_locator_page_structure(page),
        "body_text_excerpt": body,
    }


def _normalize_ai_selector_plan(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    keys = (
        "locator_input_candidates",
        "location_mode_candidates",
        "list_results_candidates",
        "dealer_result_container_candidates",
        "notes_on_page_state",
        "likely_wrong_elements_to_avoid",
    )
    out: dict[str, Any] = {}
    for k in keys:
        v = raw.get(k)
        if isinstance(v, list):
            out[k] = v[:12]
        elif isinstance(v, str):
            out[k] = v[:500]
        else:
            out[k] = v
    return out


def _attempt_marker_clicks(page: Any, log: list[str]) -> None:
    marker_selectors = [
        '[aria-label*="BMW" i]',
        '[aria-label*="dealer" i]',
        '[class*="marker" i]',
        'button[title*="map" i]',
    ]
    clicked = 0
    for sel in marker_selectors:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 5)
            for i in range(n):
                try:
                    loc.nth(i).click(timeout=800)
                    page.wait_for_timeout(400)
                    clicked += 1
                except Exception:
                    continue
        except Exception:
            continue
    if clicked:
        log.append(f"map_marker_clicks:{clicked}")


def _zip_quality_snapshot(page: Any) -> dict[str, int]:
    blocks = scrape_dom_dealer_like_blocks(page)[:40]
    usable = 0
    partial = 0
    for b in blocks:
        txt = b.get("text_excerpt") or ""
        links = [x for x in (b.get("http_links") or []) if isinstance(x, str)]
        website, _map = _pick_site_and_map_links(links)
        street, city, state, _zip = _extract_addr_parts(txt)
        name = (extract_dom_card_guess(txt).get("dealer_name_guess") or "").strip()
        if name and website and (street or (city and state)):
            usable += 1
        elif name:
            partial += 1
    return {"usable": usable, "partial": partial, "cards_detected": len(blocks)}


def _visible_dealer_names(page: Any) -> list[str]:
    sels = [
        '[class*="dealer" i]',
        '[class*="retailer" i]',
        '[class*="location" i]',
        '[class*="result" i]',
        "article",
    ]
    out: list[str] = []
    seen: set[str] = set()
    for sel in sels:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 30)
            for i in range(n):
                t = (loc.nth(i).inner_text(timeout=1200) or "").strip()
                if not t:
                    continue
                first = t.splitlines()[0].strip()
                if len(first) < 3:
                    continue
                if "bmw" not in first.lower() and "dealer" not in t.lower():
                    continue
                if first.lower() in seen:
                    continue
                seen.add(first.lower())
                out.append(first[:160])
        except Exception:
            continue
    return out


def scrape_dom_dealer_like_blocks(page: Any) -> list[dict[str, Any]]:
    """Visible text blocks that may be dealer cards (fallback)."""
    selectors = [
        '[class*="dealer" i]',
        '[class*="location" i]',
        '[class*="retailer" i]',
        '[class*="result" i]',
        '[class*="listing" i]',
        '[data-testid*="dealer" i]',
        '[data-testid*="location" i]',
        "article",
        '[role="article"]',
    ]
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for sel in selectors:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 35)
            for i in range(n):
                try:
                    el = loc.nth(i)
                    t = el.inner_text(timeout=2500)
                    if len(t) < 20:
                        continue
                    sig = t[:300]
                    if sig in seen:
                        continue
                    if not re.search(r"bmw|dealer|center|motor|automotive|service", t, re.I):
                        if not re.search(r"\d{5}", t):
                            continue
                    seen.add(sig)
                    links: list[str] = []
                    try:
                        for j in range(min(el.locator('a[href^="http"]').count(), 8)):
                            href = el.locator('a[href^="http"]').nth(j).get_attribute("href")
                            if href:
                                links.append(href[:500])
                    except Exception:
                        pass
                    out.append(
                        {
                            "selector": sel,
                            "text_excerpt": t[:2200],
                            "guess": extract_dom_card_guess(t),
                            "http_links": links,
                        }
                    )
                except Exception:
                    continue
        except Exception:
            continue
        if len(out) >= 45:
            break
    return out


def _is_map_link(u: str) -> bool:
    ul = (u or "").lower()
    return "google.com/maps" in ul or "maps.google." in ul or "destination=" in ul


def _is_social_or_aggregator(u: str) -> bool:
    ul = (u or "").lower()
    return any(
        x in ul
        for x in (
            "facebook.com",
            "instagram.com",
            "twitter.com",
            "x.com/",
            "youtube.com",
            "yelp.com",
            "linkedin.com",
            "dealerrater.com",
            "cars.com",
            "autotrader.com",
        )
    )


def _pick_site_and_map_links(links: list[str]) -> tuple[str, str]:
    website = ""
    map_ref = ""
    for lnk in links:
        if _is_map_link(lnk):
            if not map_ref:
                map_ref = lnk
            continue
        if _is_social_or_aggregator(lnk):
            continue
        host = (urlparse(lnk).netloc or "").lower()
        if not host:
            continue
        if not website:
            website = lnk
    return website, map_ref


def _extract_addr_parts(text: str) -> tuple[str, str, str, str]:
    city = state = zip_ = ""
    street = ""
    m = re.search(r"\b([A-Za-z .'-]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)\b", text)
    if m:
        city, state, zip_ = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines[:12]:
        if re.search(r"^\d+\s+", ln) and len(ln) > 8:
            street = ln[:220]
            break
    return street, city, state, zip_


def extract_detail_rows_from_interaction(page: Any, *, source_locator_url: str, scraped_at_iso: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Click visible result cards / items and scrape expanded detail panels.
    Returns (rows, debug_entries).
    """
    rows: list[dict[str, Any]] = []
    debug_entries: list[dict[str, Any]] = []
    result_selectors = [
        '[class*="dealer" i]',
        '[class*="retailer" i]',
        '[class*="result" i]',
        '[class*="location" i]',
        "article",
    ]
    detail_selectors = [
        '[class*="detail" i]',
        '[class*="drawer" i]',
        '[class*="panel" i]',
        '[role="dialog"]',
        '[class*="modal" i]',
    ]
    seen_sig: set[str] = set()
    for sel in result_selectors:
        try:
            loc = page.locator(sel)
            n = min(loc.count(), 18)
        except Exception:
            continue
        for i in range(n):
            try:
                el = loc.nth(i)
                txt = (el.inner_text(timeout=1500) or "").strip()
                if len(txt) < 12:
                    continue
                sig = txt.splitlines()[0].strip().lower()[:180]
                if sig in seen_sig:
                    continue
                seen_sig.add(sig)
                try:
                    el.click(timeout=2000)
                except Exception:
                    pass
                page.wait_for_timeout(1000)
                detail_txt = ""
                detail_links: list[str] = []
                detail_sel_used = ""
                for dsel in detail_selectors:
                    try:
                        dloc = page.locator(dsel)
                        if dloc.count() == 0:
                            continue
                        d = dloc.first
                        t = (d.inner_text(timeout=1400) or "").strip()
                        if len(t) > len(detail_txt):
                            detail_txt = t
                            detail_sel_used = dsel
                        detail_links = _extract_links_from_detail_node(d)
                    except Exception:
                        continue
                base_text = detail_txt if len(detail_txt) > len(txt) else txt
                name_guess = extract_dom_card_guess(base_text).get("dealer_name_guess", "").strip()
                if not name_guess:
                    continue
                street, city, state, zip_ = _extract_addr_parts(base_text)
                phone = extract_dom_card_guess(base_text).get("phone_guess", "")
                card_links = _extract_links_from_detail_node(el)
                links = detail_links or card_links
                website, map_ref = _pick_site_and_map_links(links)
                row = {
                    "dealer_name": name_guess,
                    "brand": "BMW",
                    "street": street,
                    "city": city,
                    "state": state,
                    "zip": zip_,
                    "phone": phone,
                    "website": website,
                    "map_reference_url": map_ref,
                    "candidate_websites": links[:12],
                    "source_locator_url": source_locator_url,
                    "scraped_at": scraped_at_iso,
                    "source_of_each_field": {
                        "dealer_name": "detail_drawer_or_card_dom_text",
                        "street": "detail_drawer_or_card_dom_text",
                        "city": "detail_drawer_or_card_dom_text",
                        "state": "detail_drawer_or_card_dom_text",
                        "zip": "detail_drawer_or_card_dom_text",
                        "phone": "detail_drawer_or_card_dom_text",
                        "website": "detail_drawer_or_card_anchor_href",
                        "map_reference_url": "detail_drawer_or_card_anchor_href",
                    },
                    "raw_source_payload": {
                        "source": "detail_interaction",
                        "result_selector": sel,
                        "detail_selector": detail_sel_used,
                        "card_excerpt": txt[:500],
                        "detail_excerpt": detail_txt[:700],
                    },
                }
                rows.append(row)
                debug_entries.append(
                    {
                        "result_selector": sel,
                        "detail_selector": detail_sel_used,
                        "name": name_guess,
                        "website_found": bool(website),
                        "address_found": bool(street or (city and state)),
                        "phone_found": bool(phone),
                    }
                )
            except Exception:
                continue
    return rows, debug_entries


def _extract_links_from_detail_node(node: Any) -> list[str]:
    links: list[str] = []
    try:
        a = node.locator('a[href]')
        for j in range(min(a.count(), 15)):
            href = a.nth(j).get_attribute("href")
            if not href:
                continue
            href = href.strip()
            if href.startswith("http"):
                links.append(href[:700])
    except Exception:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for l in links:
        if l in seen:
            continue
        seen.add(l)
        out.append(l)
    return out


def _build_name_to_zip_hint(interaction_data: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for obs in interaction_data.get("zip_observations") or []:
        z = str(obs.get("zip") or "")
        for n in obs.get("visible_dealer_names") or []:
            ns = str(n).strip().lower()
            if ns and ns not in out:
                out[ns] = z
    return out


def _find_zip_hint_for_name(name: str, by_name: dict[str, str]) -> str:
    nl = (name or "").strip().lower()
    if not nl:
        return ""
    if nl in by_name:
        return by_name[nl]
    for k, z in by_name.items():
        if nl in k or k in nl:
            return z
    return ""


def run_deep_locator_discovery(
    *,
    BMW_USA_LOCATOR_URL: str,
    scraped_at_iso: str,
    timeout_ms: int,
    verify_ssl: bool,
    user_agent: str,
    parse_json_to_records_fn: Any,
    zip_codes: tuple[str, ...] = ("92606", "90807"),
    selector_overrides: dict[str, str] | None = None,
    ai_selector_adjudicator: Any | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Returns (dom_derived_records, debug_report).
    dom_derived_records are minimal dicts from DOM (not full intake schema merge here — caller merges).
    """
    from datetime import datetime, timezone

    from playwright.sync_api import sync_playwright

    def _parse(data: Any) -> tuple[list[dict[str, Any]], list]:
        return parse_json_to_records_fn(
            data,
            source_locator_url=BMW_USA_LOCATOR_URL,
            scraped_at_iso=scraped_at_iso,
            source_hint="discovery",
        )

    network_initial: list[dict[str, Any]] = []
    network_after: list[dict[str, Any]] = []
    nav_info: dict[str, Any] = {}
    html_inspection: dict[str, Any] = {}
    interaction_data: dict[str, Any] = {"steps": [], "zip_observations": []}
    dom_blocks: list[dict[str, Any]] = []
    json_parse_records: list[dict[str, Any]] = []
    detail_rows: list[dict[str, Any]] = []
    detail_debug: list[dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=user_agent,
            ignore_https_errors=not verify_ssl,
            locale="en-US",
        )
        page = ctx.new_page()

        state: dict[str, str] = {"phase": "initial"}

        def on_response(resp: Any) -> None:
            ph = state["phase"]
            target = network_initial if ph == "initial" else network_after
            try:
                target.append(
                    _network_entry_from_response(
                        resp,
                        phase=ph,
                        scraped_at_iso=scraped_at_iso,
                        parse_fn=_parse,
                        out_parsed_records=json_parse_records,
                    )
                )
            except Exception as e:
                target.append(
                    {
                        "phase": ph,
                        "url": getattr(resp, "url", ""),
                        "error": str(e)[:200],
                    }
                )

        page.on("response", on_response)
        try:
            resp = page.goto(BMW_USA_LOCATOR_URL, wait_until="load", timeout=timeout_ms)
            nav_info = {
                "final_url": page.url,
                "status": resp.status if resp else None,
                "page_title": page.title(),
            }
        except Exception as e:
            nav_info = {"error": str(e), "error_type": type(e).__name__}

        page.wait_for_timeout(8000)
        try:
            page.wait_for_load_state("networkidle", timeout=60_000)
        except Exception:
            pass

        try:
            html = page.content()
            html_inspection = inspect_html_and_inline_scripts(html)
        except Exception as e:
            html_inspection = {"error": str(e)[:300]}

        state["phase"] = "after_zip_search"
        screenshot_dir = (
            Path.cwd() / "data" / "oem" / "bmw" / "debug" / "screenshots"
        )
        html_snapshot_dir = (
            Path.cwd() / "data" / "oem" / "bmw" / "debug" / "html_snapshots"
        )
        interaction_data = run_interaction_locator(
            page,
            zip_codes=zip_codes,
            screenshot_dir=screenshot_dir,
            html_snapshot_dir=html_snapshot_dir,
            selector_overrides=selector_overrides,
            ai_selector_adjudicator=ai_selector_adjudicator,
        )
        try:
            detail_rows, detail_debug = extract_detail_rows_from_interaction(
                page,
                source_locator_url=BMW_USA_LOCATOR_URL,
                scraped_at_iso=scraped_at_iso,
            )
        except Exception as e:
            interaction_data["steps"] = list(interaction_data.get("steps") or [])
            interaction_data["steps"].append(f"detail_extract_error:{e!s}"[:220])
        try:
            dom_blocks = scrape_dom_dealer_like_blocks(page)
        except Exception as e:
            interaction_data["steps"] = list(interaction_data.get("steps") or [])
            interaction_data["steps"].append(f"dom_scrape_error:{e!s}"[:200])

        try:
            nav_info["title_after_interaction"] = page.title()
            nav_info["url_after_interaction"] = page.url
        except Exception:
            pass

        ctx.close()
        browser.close()

    all_net = network_initial + network_after
    likely_rel = [x for x in all_net if x.get("bucket") == "likely_relevant"]
    likely_noise = [x for x in all_net if x.get("bucket") == "likely_noise"]
    ambiguous = [x for x in all_net if x.get("bucket") == "ambiguous"]

    # Shortlists (cap size for artifact)
    def _trim(lst: list[dict], n: int) -> list[dict]:
        return lst[:n]

    debug_report: dict[str, Any] = {
        "mode": "playwright_deep_discovery",
        "locator_url": BMW_USA_LOCATOR_URL,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "navigation": nav_info,
        "html_script_inspection": html_inspection,
        "interactive_flow": {
            "zip_codes_tried": list(zip_codes),
            "steps": interaction_data.get("steps") or [],
            "zip_observations": interaction_data.get("zip_observations") or [],
            "locator_shell_state_initial": interaction_data.get("locator_shell_state_initial") or "",
            "visible_locator_controls_initial": interaction_data.get("visible_locator_controls_initial") or [],
            "selector_overrides": interaction_data.get("selector_overrides") or {},
            "screenshot_dir": interaction_data.get("screenshot_dir") or "",
            "html_snapshot_dir": interaction_data.get("html_snapshot_dir") or "",
            "locator_page_structure": interaction_data.get("locator_page_structure") or {},
        },
        "shortlist": {
            "likely_relevant": _trim(likely_rel, 120),
            "likely_irrelevant": _trim(likely_noise, 150),
            "ambiguous": _trim(ambiguous, 120),
            "counts": {
                "total_responses_logged": len(all_net),
                "likely_relevant": len(likely_rel),
                "likely_noise": len(likely_noise),
                "ambiguous": len(ambiguous),
            },
        },
        "network_all_phases_sample": _trim(all_net, 400),
        "dom_scrape_candidates": dom_blocks[:50],
        "detail_interaction_candidates": detail_debug[:80],
        "classification_notes": {
            "noise": "URL/host matched tracking, consent, Adobe/Target, mpulse, autointel, or Maps viewport RPC without dealer keywords",
            "relevant": "Multiple dealer/geo keywords in body and/or BMW/API path",
            "ambiguous": "GraphQL, protobuf+json, or partial keyword hits — inspect body_preview manually",
        },
        "json_parser_rows_found": len(json_parse_records),
    }

    name_zip_hint = _build_name_to_zip_hint(interaction_data)
    for r in json_parse_records + detail_rows:
        if not isinstance(r, dict):
            continue
        rsp = r.get("raw_source_payload") if isinstance(r.get("raw_source_payload"), dict) else {}
        if not rsp:
            rsp = {}
            r["raw_source_payload"] = rsp
        if not rsp.get("zip_seed_hint"):
            rsp["zip_seed_hint"] = _find_zip_hint_for_name(str(r.get("dealer_name") or ""), name_zip_hint)

    dom_records: list[dict[str, Any]] = []
    for b in dom_blocks:
        g = b.get("guess") or {}
        name = (g.get("dealer_name_guess") or "").strip()
        if len(name) < 2:
            continue
        links = [x for x in (b.get("http_links") or []) if isinstance(x, str)]
        website, map_ref = _pick_site_and_map_links(links)
        dom_records.append(
            {
                "dealer_name": name,
                "brand": "BMW",
                "street": g.get("address_guess") or "",
                "city": g.get("city_guess") or "",
                "state": g.get("state_guess") or "",
                "zip": g.get("zip_guess") or "",
                "phone": g.get("phone_guess") or "",
                "website": website,
                "map_reference_url": map_ref,
                "candidate_websites": links[:10],
                "source_of_each_field": {
                    "dealer_name": "list_view_dom_text",
                    "street": "list_view_dom_text",
                    "city": "list_view_dom_text",
                    "state": "list_view_dom_text",
                    "zip": "list_view_dom_text",
                    "phone": "list_view_dom_text",
                    "website": "list_view_anchor_href",
                    "map_reference_url": "list_view_anchor_href",
                },
                "source_locator_url": BMW_USA_LOCATOR_URL,
                "scraped_at": scraped_at_iso,
                "raw_source_payload": {"source": "dom_scrape_fallback", "selector": b.get("selector"), "excerpt": b.get("text_excerpt", "")[:500]},
            }
        )
        dom_records[-1]["raw_source_payload"]["zip_seed_hint"] = _find_zip_hint_for_name(
            dom_records[-1].get("dealer_name", ""), name_zip_hint
        )

    for r in json_parse_records:
        if isinstance(r, dict) and "source_of_each_field" not in r:
            r["source_of_each_field"] = {
                "dealer_name": "script_state",
                "street": "script_state",
                "city": "script_state",
                "state": "script_state",
                "zip": "script_state",
                "phone": "script_state",
                "website": "script_state",
                "map_reference_url": "script_state",
            }
    combined = json_parse_records + detail_rows + dom_records
    return combined, debug_report
