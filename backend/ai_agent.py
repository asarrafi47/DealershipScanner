"""
Context-aware AI co-pilot: OpenAI GPT-4o + EPA / trim verification (Truth Engine).
Requires OPENAI_API_KEY. Tool: verify_car_data(vin).
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

from backend.db.inventory_db import get_car_by_vin
from backend.knowledge_engine import decode_trim_logic, lookup_epa_aggregate, prepare_car_detail_context
from backend.utils.car_serialize import DISPLAY_DASH, build_engine_display, format_display_value
from backend.utils.field_clean import clean_car_row_dict, is_effectively_empty

OPENAI_MODEL = os.environ.get("OPENAI_CHAT_MODEL", "gpt-4o")


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


def run_ai_chat(
    user_message: str,
    current_vin: str | None,
    page_hint: str | None = None,
) -> dict[str, Any]:
    """
    Call OpenAI with verification context. Returns reply text + discrepancy_flags for UI.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return {
            "reply": (
                "AI co-pilot is not configured. Set the OPENAI_API_KEY environment variable "
                "to enable GPT-4o verification against your EPA database."
            ),
            "discrepancy_flags": [],
            "verification": None,
            "error": "missing_api_key",
        }

    try:
        from openai import OpenAI
    except ImportError:
        return {
            "reply": "Install the `openai` package: pip install openai",
            "discrepancy_flags": [],
            "verification": None,
            "error": "missing_openai_package",
        }

    client = OpenAI(api_key=api_key)
    verification: dict[str, Any] | None = None
    vin = (current_vin or "").strip()
    if vin:
        verification = verify_car_data(vin)

    system_parts = [
        "You are the Dealership Scanner Automotive Co-Pilot — a careful 'Truth Engine'. ",
        "You reduce errors by comparing dealer listing data with EPA (epa_master) and trim-based rules. ",
        "Be concise, friendly, and factual. If verification shows mismatches, explain them clearly ",
        "and recommend confirming with the dealer. Never invent EPA numbers; use only the JSON given. ",
    ]
    if page_hint == "listings":
        system_parts.append("The user is on the search/listings page — help them filter or understand inventory; VIN may be absent. ")
    elif page_hint == "car":
        system_parts.append("The user is viewing a single vehicle detail page — you have VIN context. ")

    if verification:
        system_parts.append(
            "\n\n## Verification JSON (authoritative for this chat turn)\n"
            + json.dumps(verification, indent=2, default=str)
        )
    else:
        system_parts.append("\n\nNo VIN was provided; answer generally or ask for a vehicle context.")

    system_parts.append(
        "\n\nIf asked whether the page is correct, cite the verification mismatches array when non-empty, "
        "otherwise say data looks consistent with EPA/trim inference."
    )

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": "".join(system_parts)},
        {"role": "user", "content": user_message},
    ]

    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        temperature=0.4,
        max_tokens=1200,
    )
    reply = (resp.choices[0].message.content or "").strip()

    flags = (verification or {}).get("discrepancy_flags") or []
    return {
        "reply": reply,
        "discrepancy_flags": flags,
        "verification": verification,
        "error": None,
    }


# ── Web-research trigger detection ────────────────────────────────────────
# Substrings that signal the user wants external model/market knowledge.
# Checked against lowercased message; kept as substrings so "reliability",
# "reliable", "unreliable" all match "reliab", etc.
# Tight triggers: market / reliability / explicit powertrain-economy questions only.
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
    " hp",
    "horsepower",
    "torque",
    "powertrain",
    "fuel economy",
    "mpg",
    "range",
    "0-60",
    "quarter mile",
    "towing",
    "payload",
    "tow capacity",
    "safety rating",
    "crash test",
    "warranty",
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


def run_car_page_chat(car: dict[str, Any], user_message: str) -> dict[str, Any]:
    """
    Car detail chatbot: Ollama (OpenAI-compatible) via llm.providers.ollama_client.

    When the question touches reliability, reviews, market value, comparisons,
    or other topics that aren't in inventory.db, we transparently run a
    Playwright web-research pass (WebResearcher) and inject the snippet into
    the system prompt as [Internet Research Data] before calling the LLM.

    ``car`` should be the full SQLite row dict from get_car_by_id.
    """
    msg = (user_message or "").strip()
    if not msg:
        return {"reply": "", "error": "empty_message", "discrepancy_flags": []}

    try:
        from llm.client import LLMResponseError
        from llm.providers.ollama_client import OpenAICompatibleClient
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
        _evidence_line("Exterior color", c.get("exterior_color")),
        _evidence_line("Interior color", c.get("interior_color")),
        _evidence_line("Body style", c.get("body_style")),
        _evidence_line("Condition", c.get("condition")),
        _evidence_line("Dealer name", c.get("dealer_name")),
        _evidence_line("Dealer URL", c.get("dealer_url")),
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

    print(f"\n[CHAT] message={msg!r}  car={listing_head[:80]!r}")

    research_text = ""
    research_url = ""
    research_used = False
    cache_hit = False
    kw_hit = _needs_web_research(msg)
    add_model_knowledge_fn = None
    get_model_knowledge_fn = None

    if kw_hit:
        print("[CHAT] keyword_trigger=True — knowledge cache / web research allowed")
        try:
            from backend.vector.pgvector_service import (
                add_model_knowledge as add_model_knowledge_fn,
                get_model_knowledge as get_model_knowledge_fn,
            )
        except ImportError as exc:
            print(f"[CHAT] pgvector knowledge import failed (non-fatal): {exc}")

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
                print(f"[CHAT] knowledge cache lookup failed (non-fatal): {exc}")

        if not cache_hit:
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
                            print(f"[CHAT] knowledge cache write EXCEPTION: {exc_store}")
            except Exception as exc:
                import logging as _logging

                _logging.getLogger(__name__).warning("[ai_agent] WebResearcher failed: %s", exc)

    system_parts: list[str] = [
        "You answer questions about one dealership listing. Follow the evidence blocks in order; "
        "never treat cached model text or web snippets as facts about this VIN.\n\n"
        "STYLE: Exactly 2–4 short sentences. Lead with the direct answer. If the listing lines say "
        "'not shown on this listing', repeat that wording — never substitute guessed specs, packages, "
        "or options for this VIN. Label block (4) as inferred from trim/EPA, not dealer-confirmed.\n\n"
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

    client = OpenAICompatibleClient()
    try:
        reply = client.complete_text(
            system=system,
            user=msg,
            temperature=0.28,
            max_tokens=280,
        )
    except LLMResponseError as e:
        return {"reply": "", "error": str(e), "discrepancy_flags": []}
    except Exception as e:
        return {"reply": "", "error": str(e)[:500], "discrepancy_flags": []}

    return {
        "reply": reply,
        "error": None,
        "discrepancy_flags": [],
        "web_research_used": research_used,
        "web_research_url": research_url if research_used else None,
    }
