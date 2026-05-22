"""
Context-aware AI co-pilot powered by Claude Haiku + EPA / trim verification.
Requires ANTHROPIC_API_KEY.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

_logger = logging.getLogger(__name__)

from backend.db.inventory_db import get_car_by_vin
from backend.enrichment.knowledge_engine import decode_trim_logic, lookup_epa_aggregate, prepare_car_detail_context
from backend.utils.car_serialize import DISPLAY_DASH, build_engine_display, format_display_value
from backend.utils.field_clean import clean_car_row_dict, is_effectively_empty


def _norm_drive_compare(s: str | None) -> str:
    if not s:
        return ""
    u = str(s).strip().upper()
    if any(x in u for x in ("AWD", "4WD", "4X4", "4MATIC", "XDRIVE", "QUATTRO", "ALL-WHEEL", "ALL WHEEL")):
        return "AWD"
    if "FWD" in u or "FRONT-WHEEL" in u or "FRONT WHEEL" in u:
        return "FWD"
    if "RWD" in u or "REAR-WHEEL" in u or "REAR WHEEL" in u:
        return "RWD"
    return u.replace(" ", "")[:12]


def _int_or_none(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def verify_car_data(vin: str) -> dict[str, Any]:
    """
    Fetch listing + EPA aggregate + trim decoder; return matches/mismatches and UI flags.
    Callable as an OpenAI tool and used directly by /api/ai/chat.
    """
    raw_vin = (vin or "").strip()
    out: dict[str, Any] = {
        "vin": raw_vin.upper(),
        "ok": True,
        "dealer": {},
        "epa_summary": {},
        "trim_decoder": {},
        "matches": [],
        "mismatches": [],
        "discrepancy_flags": [],
        "error": None,
    }
    if not raw_vin or raw_vin.upper().startswith("UNKNOWN"):
        out["error"] = "Invalid or unknown VIN in listing."
        out["ok"] = False
        return out

    car = get_car_by_vin(raw_vin) or get_car_by_vin(raw_vin.upper()) or get_car_by_vin(raw_vin.lower())
    if not car:
        out["error"] = "No vehicle found in inventory for this VIN."
        out["ok"] = False
        return out

    year = car.get("year")
    try:
        y = int(year) if year is not None else None
    except (TypeError, ValueError):
        y = None
    make = (car.get("make") or "").strip()
    model = (car.get("model") or "").strip()
    trim = (car.get("trim") or "").strip()
    title = (car.get("title") or "").strip()

    out["dealer"] = {
        "year": year,
        "make": make,
        "model": model,
        "trim": trim,
        "cylinders": car.get("cylinders"),
        "drivetrain": (car.get("drivetrain") or "").strip(),
        "fuel_type": (car.get("fuel_type") or "").strip(),
        "transmission": (car.get("transmission") or "").strip(),
    }

    regex = decode_trim_logic(make, model, trim, title)
    epa = lookup_epa_aggregate(y, make, model)
    out["trim_decoder"] = {k: v for k, v in regex.items() if v is not None}
    out["epa_summary"] = {
        "cylinders": epa.get("cylinders"),
        "drivetrain": epa.get("drivetrain"),
        "displacement": epa.get("displacement"),
        "fuel_type": epa.get("fuel_type"),
        "atv_type": epa.get("atv_type"),
    }

    dealer_cyl = _int_or_none(car.get("cylinders"))
    truth_cyl = regex.get("cylinders")
    if truth_cyl is None:
        truth_cyl = epa.get("cylinders")
    if isinstance(truth_cyl, float):
        truth_cyl = int(truth_cyl)

    if truth_cyl is not None and dealer_cyl is not None:
        if dealer_cyl != truth_cyl:
            msg = (
                f"Dealer lists {dealer_cyl} cylinders; EPA/trim inference suggests {truth_cyl} "
                f"for this year/make/model."
            )
            out["mismatches"].append(
                {"field": "cylinders", "dealer_value": dealer_cyl, "expected": truth_cyl, "message": msg}
            )
            out["discrepancy_flags"].append(
                {
                    "field": "cylinders",
                    "spec_key": "cylinders",
                    "severity": "warning",
                    "message": msg,
                }
            )
            out["ok"] = False
        else:
            out["matches"].append("cylinders")

    truth_drive = regex.get("drivetrain") or epa.get("drivetrain")
    dealer_drive = (car.get("drivetrain") or "").strip()
    if truth_drive and dealer_drive and not is_effectively_empty(dealer_drive):
        d1 = _norm_drive_compare(dealer_drive)
        d2 = _norm_drive_compare(truth_drive)
        if d1 and d2 and d1 != d2:
            msg = (
                f"Dealer lists drivetrain as '{dealer_drive}'; EPA/trim suggests '{truth_drive}' "
                f"(e.g. xDrive/4MATIC usually implies AWD)."
            )
            out["mismatches"].append(
                {
                    "field": "drivetrain",
                    "dealer_value": dealer_drive,
                    "expected": truth_drive,
                    "message": msg,
                }
            )
            out["discrepancy_flags"].append(
                {
                    "field": "drivetrain",
                    "spec_key": "drivetrain",
                    "severity": "warning",
                    "message": msg,
                }
            )
            out["ok"] = False
        elif d1 == d2:
            out["matches"].append("drivetrain")

    return out


# ── Web-research trigger detection ────────────────────────────────────────
# Substrings that signal the user wants external model/market knowledge.
# Checked against lowercased message; kept as substrings so "reliability",
# "reliable", "unreliable" all match "reliab", etc.
# Tight triggers: market / reliability / explicit powertrain-economy questions only.
# Only trigger web scrape for things Claude can't answer from training:
# market prices, live recall data, owner forum reliability threads.
# Spec questions (HP, torque, MPG, 0-60, towing) → Claude answers from training instantly.
_WEB_RESEARCH_TRIGGERS: tuple[str, ...] = (
    "reliab",
    "problem",
    "issue",
    "recall",
    "defect",
    "fault",
    "review",
    "worth it",
    "good deal",
    "bad deal",
    "fair price",
    "market value",
    "market price",
    "going rate",
    "compared to",
    "compare to",
    " vs ",
    "versus",
    "better than",
    "worse than",
    "common problem",
    "known issue",
    "typical problem",
    "maintain",
    "repair cost",
    "ownership cost",
    "cost to own",
    "long.term",
    "depreciat",
    "resale",
    "buy or lease",
    "should i buy",
    "is this a good",
    "lemon",
    "title brand",
)

def _needs_web_research(message: str) -> bool:
    """Return True if *message* contains any web-research trigger substring."""
    low = message.lower()
    return any(t in low for t in _WEB_RESEARCH_TRIGGERS)


# Regex that matches placeholder / garbage values that must NOT appear in search queries.
_QUERY_JUNK_RE = re.compile(r"^(n\/?a|none|null|unknown|[-—]+)$", re.IGNORECASE)


def _clean_spec(v: Any) -> str:
    """
    Return the string value of *v* ready for use inside a search query.
    Returns an empty string for None, empty, or placeholder values like
    'N/A', 'None', 'unknown', '-', '—'.
    """
    s = str(v or "").strip()
    return "" if _QUERY_JUNK_RE.match(s) else s


def _build_search_query(car: dict[str, Any], message: str) -> str:
    """
    Build a focused DuckDuckGo query from car fields + message context.

    Examples
    ────────
    "Is this reliable?"  →  "2021 BMW M3 Competition reliability common problems review"
    "Compare to C63 AMG" →  "2021 BMW M3 Competition vs C63 AMG comparison review"
    "Maintenance costs?" →  "2021 BMW M3 Competition maintenance cost ownership"
    "engine?"            →  "2023 BMW 530e engine specs powertrain"  (N/A trim stripped)
    """
    year  = _clean_spec(car.get("year"))
    make  = _clean_spec(car.get("make"))
    model = _clean_spec(car.get("model"))
    trim  = _clean_spec(car.get("trim"))
    base  = " ".join(filter(None, [year, make, model, trim]))

    low = message.lower()

    if any(t in low for t in ("reliab", "problem", "issue", "defect", "fault", "recall")):
        suffix = "reliability common problems issues"
    elif any(t in low for t in ("review", "is this a good", "should i buy")):
        suffix = "review expert opinion"
    elif any(t in low for t in ("maintain", "repair cost", "ownership cost", "cost to own")):
        suffix = "maintenance cost cost of ownership"
    elif any(t in low for t in ("depreciat", "resale", "worth it", "market value",
                                "fair price", "going rate", "good deal")):
        suffix = "resale value depreciation market price"
    elif any(t in low for t in ("vs ", "versus", "compared to", "compare to",
                                "better than", "worse than")):
        suffix = "comparison review vs alternatives"
    elif any(t in low for t in ("engine", "powertrain", "horsepower", " hp",
                                "torque", "0-60", "quarter mile", "acceleration")):
        suffix = "engine specs powertrain horsepower"
    elif any(t in low for t in ("transmission", "drivetrain", " awd", " fwd", " rwd")):
        suffix = "transmission drivetrain specs"
    elif any(t in low for t in ("mpg", "fuel economy", "range")):
        suffix = "fuel economy mpg efficiency"
    elif any(t in low for t in ("towing", "payload", "tow capacity")):
        suffix = "towing capacity payload specs"
    elif any(t in low for t in ("safety rating", "crash test")):
        suffix = "safety ratings crash test NHTSA IIHS"
    elif any(t in low for t in ("warranty",)):
        suffix = "warranty coverage terms"
    else:
        # Generic fallback: take meaningful words from the user message
        words = re.sub(r"[^\w\s]", "", low).split()
        suffix = " ".join(words[:6]) if words else "specs review"

    return f"{base} {suffix}".strip()


def _evidence_line(label: str, val: Any) -> str:
    s = format_display_value(val)
    if s == DISPLAY_DASH:
        return f"{label}: not shown on this listing"
    return f"{label}: {s}"


def _price_evidence(val: Any) -> str:
    if val is None:
        return "Price: not shown on this listing"
    try:
        p = float(val)
    except (TypeError, ValueError):
        return "Price: not shown on this listing"
    if p > 0:
        return f"Price: ${p:,.0f}"
    return "Price: not shown on this listing"


def _mileage_evidence(val: Any) -> str:
    if val is None:
        return "Mileage: not shown on this listing"
    try:
        mi = int(val)
    except (TypeError, ValueError):
        return "Mileage: not shown on this listing"
    if mi > 0:
        return f"Mileage: {mi:,} mi"
    return "Mileage: not shown on this listing"


def _history_highlights_snippet(car: dict[str, Any]) -> str:
    h = car.get("history_highlights")
    if h is None:
        return ""
    if isinstance(h, list):
        parts: list[str] = []
        for x in h:
            sx = format_display_value(x)
            if sx != DISPLAY_DASH and len(sx) > 1:
                parts.append(sx)
        return " | ".join(parts[:24])
    if isinstance(h, str) and h.strip():
        t = h.strip()[:2000]
        tl = t.lower()
        if "see manufacturer" in tl or "manufacturer specifications" in tl:
            return ""
        return t
    return ""


def _dealer_map_line(c: dict[str, Any]) -> str:
    addr = str(c.get("dealer_address") or "").strip()
    lat = c.get("dealer_lat")
    lon = c.get("dealer_lon")
    if lat and lon:
        gmaps = f"https://maps.google.com/?q={lat},{lon}"
        amaps = f"https://maps.apple.com/?ll={lat},{lon}"
        return f"Dealer map: Google Maps {gmaps} | Apple Maps {amaps}"
    if addr:
        q = addr.replace(" ", "+")
        return f"Dealer map: https://maps.google.com/?q={q}"
    return ""


def run_car_page_chat(
    car: dict[str, Any],
    user_message: str,
    *,
    allow_web_research: bool = True,
) -> dict[str, Any]:
    """
    Car detail chatbot: Claude Haiku via Anthropic API.

    When the question touches reliability, reviews, market value, comparisons,
    or other topics that aren't in inventory.db, we may run a Playwright
    web-research pass (WebResearcher) when ``allow_web_research`` is True,
    then inject the snippet into the system prompt before calling the LLM.
    Model-knowledge cache (pgvector) may still be read when keywords match,
    even when live web research is disabled.

    ``car`` should be the full SQLite row dict from get_car_by_id.
    """
    msg = (user_message or "").strip()
    if not msg:
        return {"reply": "", "error": "empty_message", "discrepancy_flags": []}

    try:
        import anthropic as _anthropic
    except ImportError as e:
        return {"reply": "", "error": f"llm_import:{e}", "discrepancy_flags": []}

    c = clean_car_row_dict(car)
    ctx = prepare_car_detail_context(car)
    verified = ctx.get("verified_specs") or {}

    year = c.get("year")
    make_kw = (c.get("make") or "").strip() or "unknown"
    model_kw = (c.get("model") or "").strip() or "unknown"
    trim_kw = format_display_value(c.get("trim"))
    if trim_kw == DISPLAY_DASH:
        trim_kw = ""

    engine_line = build_engine_display(c, verified)

    heading_parts = [
        x
        for x in (
            format_display_value(year),
            format_display_value(c.get("make")),
            format_display_value(c.get("model")),
            format_display_value(c.get("trim")),
        )
        if x != DISPLAY_DASH
    ]
    listing_head = " ".join(heading_parts) if heading_parts else "Vehicle (listing identifiers incomplete)"

    mpg_line = ""
    mc, mh = c.get("mpg_city"), c.get("mpg_highway")
    if mc and mh:
        mpg_line = f"{mc} city / {mh} hwy"
    elif mc:
        mpg_line = f"{mc} city"
    elif mh:
        mpg_line = f"{mh} hwy"

    lines = [
        f"Listing heading: {listing_head}",
        _price_evidence(c.get("price")),
        _mileage_evidence(c.get("mileage")),
        _evidence_line("VIN", c.get("vin")),
        _evidence_line("Stock #", c.get("stock_number")),
        f"Engine (derived for this prompt): {engine_line}",
        _evidence_line("Fuel type", c.get("fuel_type")),
        _evidence_line("Cylinders", c.get("cylinders")),
        _evidence_line("Transmission", verified.get("transmission_display") or c.get("transmission")),
        _evidence_line("Drivetrain", verified.get("drivetrain_display") or c.get("drivetrain")),
        _evidence_line("MPG", mpg_line or None),
        _evidence_line("Exterior color", c.get("exterior_color")),
        _evidence_line("Interior color", c.get("interior_color")),
        _evidence_line("Body style", c.get("body_style")),
        _evidence_line("Condition", c.get("condition")),
        _evidence_line("CARFAX URL", c.get("carfax_url")),
        _evidence_line("Window sticker URL", c.get("window_sticker_url")),
        _evidence_line("Dealer name", c.get("dealer_name")),
        _evidence_line("Dealer URL", c.get("dealer_url")),
        _evidence_line("Dealer address", c.get("dealer_address")),
        _dealer_map_line(c),
    ]
    desc = (c.get("description") or "").strip() if isinstance(c.get("description"), str) else ""
    if desc and not is_effectively_empty(desc):
        lines.append(_evidence_line("Description excerpt", desc[:900]))

    local_context = "\n".join(lines).strip()

    listing_notes = _history_highlights_snippet(car)
    if desc and not is_effectively_empty(desc):
        listing_notes = (listing_notes + "\n\nDescription:\n" + desc[:2500]).strip()

    pkg_raw = c.get("packages")
    packages_snip = ""
    if isinstance(pkg_raw, str) and pkg_raw.strip():
        packages_snip = pkg_raw.strip()[:1800]

    verified_snip = ""
    if verified:
        verified_snip = json.dumps(verified, indent=1, default=str)[:2500]

    _logger.debug(
        "car_chat listing_preview=%r msg_len=%d allow_web_research=%s",
        (listing_head[:80] + ("…" if len(listing_head) > 80 else "")),
        len(msg),
        allow_web_research,
    )

    research_text = ""
    research_url = ""
    research_used = False
    cache_hit = False
    kw_hit = _needs_web_research(msg)
    add_model_knowledge_fn = None
    get_model_knowledge_fn = None

    if kw_hit:
        _logger.debug("car_chat keyword_trigger=True (cache + optional Playwright)")
        try:
            from backend.vector.pgvector_service import (
                add_model_knowledge as add_model_knowledge_fn,
                get_model_knowledge as get_model_knowledge_fn,
            )
        except ImportError as exc:
            _logger.debug("pgvector knowledge unavailable: %s", exc)

        if get_model_knowledge_fn is not None:
            try:
                cached_text, cached_url = get_model_knowledge_fn(
                    year=year, make=make_kw, model=model_kw
                )
                if cached_text:
                    research_text = cached_text
                    research_url = cached_url or ""
                    research_used = True
                    cache_hit = True
            except Exception as exc:
                _logger.warning("[ai_agent] knowledge cache lookup failed (non-fatal): %s", exc)

        if not cache_hit and allow_web_research:
            try:
                from backend.utils.web_researcher import WebResearcher

                query = _build_search_query(c, msg)
                researcher = WebResearcher(timeout_ms=25_000, max_text_chars=2_000)
                result = researcher.search_and_summarize(query)
                if result and result.text:
                    research_text = result.text
                    research_url = result.url
                    research_used = True
                    if add_model_knowledge_fn is not None:
                        try:
                            add_model_knowledge_fn(
                                year=year,
                                make=make_kw,
                                model=model_kw,
                                text=result.text,
                                source_url=result.url,
                                trim=trim_kw or "",
                            )
                        except Exception as exc_store:
                            _logger.warning("[ai_agent] knowledge cache write failed: %s", exc_store)
            except Exception as exc:
                _logger.warning("[ai_agent] WebResearcher failed: %s", exc)

    system_parts: list[str] = [
        "You answer questions about one dealership listing.\n\n"
        "PRIORITY ORDER: Use block (1) first (dealer-confirmed). Block (4) is EPA/trim inferred — label it as such. "
        "For manufacturer specs not in any block (HP, torque, 0-60, towing capacity, MPG, safety ratings, "
        "dimensions, warranty terms) — answer directly from your training knowledge; do NOT say 'not shown on this listing' "
        "for facts you know about this make/model/year/trim. Only say 'not shown' for listing-specific facts "
        "(VIN options, dealer price, actual mileage, negotiated terms).\n\n"
        "STYLE: 2–4 short sentences. Lead with the direct answer.\n\n"
        "── (1) Local listing (SQLite) ─────────────────────────────────────\n",
        local_context,
        "\n\n── (2) Listing notes / raw text ───────────────────────────────────\n",
        listing_notes or "(none)\n",
        "\n\n── (3) Enrichment packages JSON (may include vision observations) ─\n",
        packages_snip or "(none)\n",
        "\n\n── (4) Trim / EPA inferred specs (not dealer-confirmed) ────────────\n",
        verified_snip or "(none)\n",
    ]

    if research_used:
        label = "[Model knowledge cache]" if cache_hit else "[Internet research]"
        system_parts += [
            f"\n\n── (5) {label} ───────────────────────────────────────────────────\n",
            f"Source URL: {research_url or 'n/a'}\n\n",
            research_text,
            "\nIf you use this block, end your reply with a line: Source: <url>\n",
        ]
    else:
        system_parts.append(
            "\n\n── (5) External research ───────────────────────────────────────────\n"
            "(not fetched — answer from blocks 1–4 only, plus cautious general knowledge "
            "where appropriate; do not invent listing-specific facts.)\n"
        )

    system = "".join(system_parts)

    try:
        _client = _anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        _resp = _client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": msg}],
        )
        reply = _resp.content[0].text
    except Exception as e:
        return {"reply": "", "error": str(e)[:500], "discrepancy_flags": []}

    return {
        "reply": reply,
        "error": None,
        "discrepancy_flags": [],
        "web_research_used": research_used,
        "web_research_url": research_url if research_used else None,
    }
