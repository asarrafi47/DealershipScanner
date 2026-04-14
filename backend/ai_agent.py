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
from backend.knowledge_engine import decode_trim_logic, lookup_epa_aggregate

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
    if truth_drive and dealer_drive and dealer_drive.upper() not in ("N/A", "NA", "—", "-"):
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
_WEB_RESEARCH_TRIGGERS: tuple[str, ...] = (
    "reliab",          # reliability / reliable / unreliable
    "problem",         # problem / problems
    "issue",           # issue / issues
    "recall",          # recall / recalls
    "defect",          # defect / defects
    "fault",
    "review",          # review / reviews / reviewed
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
    "maintain",        # maintain / maintenance
    "repair cost",
    "ownership cost",
    "cost to own",
    "long.term",       # long-term
    "depreciat",       # depreciation / depreciate
    "resale",
    "buy or lease",
    "should i buy",
    "is this a good",
    # ── Spec / powertrain queries ──────────────────────────────────────────
    "engine",          # "what engine does this have?"
    " hp",             # horsepower shorthand (leading space avoids "chip" etc.)
    "horsepower",
    "torque",
    "powertrain",
    "transmission",    # "what transmission?"
    "drivetrain",
    " awd",            # drivetrain questions
    " fwd",
    " rwd",
    "0-60",            # performance
    "quarter mile",
    "towing",
    "payload",
    "tow capacity",
    "fuel economy",
    "mpg",
    "range",           # EV range
    "trim level",
    "trim levels",
    "trim option",
    "spec",            # specs / specifications
    "feature",         # features this model has
    "option",          # optional packages
    "package",
    "safety rating",
    "crash test",
    "warranty",
)

# ── Critical inventory fields — if any are blank/N/A, force web research ──
_CRITICAL_FIELDS = ("engine", "transmission", "drivetrain")


def _field_is_blank(v: Any) -> bool:
    """Return True if an inventory field carries no real data."""
    if v is None:
        return True
    s = str(v).strip().lower()
    return s in ("", "n/a", "na", "unknown", "-", "—", "none", "null")


def _has_missing_critical_fields(car: dict[str, Any]) -> bool:
    """
    Return True if any critical spec field (engine, transmission, drivetrain)
    is absent or a placeholder, which means the LLM would have nothing useful
    to say and should use web data instead.
    """
    checks = {
        "engine": car.get("fuel_type") or car.get("cylinders"),
        "transmission": car.get("transmission"),
        "drivetrain": car.get("drivetrain"),
    }
    return any(_field_is_blank(v) for v in checks.values())


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
    elif any(t in low for t in ("spec", "trim level", "trim option", "feature", "option", "package")):
        suffix = "specs features trim levels options"
    elif any(t in low for t in ("safety rating", "crash test")):
        suffix = "safety ratings crash test NHTSA IIHS"
    elif any(t in low for t in ("warranty",)):
        suffix = "warranty coverage terms"
    else:
        # Generic fallback: take meaningful words from the user message
        words = re.sub(r"[^\w\s]", "", low).split()
        suffix = " ".join(words[:6]) if words else "specs review"

    return f"{base} {suffix}".strip()


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

    # ── Build concise local context from inventory.db row ─────────────────
    year  = car.get("year")  or "Unknown year"
    make  = car.get("make")  or ""
    model = car.get("model") or ""
    trim  = car.get("trim")  or ""
    price = car.get("price")
    mileage = car.get("mileage")
    vin   = car.get("vin")   or ""
    dealer_name = car.get("dealer_name") or ""
    dealer_url  = car.get("dealer_url")  or ""
    ext_color   = car.get("exterior_color") or ""
    int_color   = car.get("interior_color") or ""
    fuel        = car.get("fuel_type")    or ""
    transmission = car.get("transmission") or ""
    drivetrain  = car.get("drivetrain")   or ""
    cylinders   = car.get("cylinders")
    stock       = car.get("stock_number") or ""

    price_str   = f"${price:,.0f}" if price and price > 0 else "price not listed"
    mileage_str = f"{mileage:,} mi" if mileage else "mileage not listed"
    cyl_str     = f"{cylinders}-cylinder" if cylinders else ""

    local_context = (
        f"Vehicle: {year} {make} {model} {trim}\n"
        f"Price: {price_str}  |  Mileage: {mileage_str}\n"
        f"VIN: {vin}  |  Stock #: {stock}\n"
        f"Drivetrain: {drivetrain}  |  Engine: {cyl_str} {fuel}  |  Trans: {transmission}\n"
        f"Exterior: {ext_color}  |  Interior: {int_color}\n"
        f"Dealer: {dealer_name}  ({dealer_url})"
    ).strip()

    # ── Verbose diagnostic trace (visible in Flask terminal) ──────────────
    print(f"\n[CHAT] ── New chat request ─────────────────────────────────────")
    print(f"[CHAT] message     = {msg!r}")
    print(f"[CHAT] car         = {year} {make} {model} {trim!r}")
    print(f"[CHAT] transmission= {transmission!r}  drivetrain={drivetrain!r}")
    print(f"[CHAT] fuel        = {fuel!r}  cylinders={cylinders!r}")
    kw_hit = _needs_web_research(msg)
    miss   = _has_missing_critical_fields(car)
    print(f"[CHAT] keyword_trigger={kw_hit}  missing_critical_fields={miss}")

    # ── Cache-first research: ChromaDB → WebResearcher → write-back ──────
    research_text: str  = ""
    research_url:  str  = ""
    research_used: bool = False
    cache_hit:     bool = False

    if kw_hit or miss:

        # ── Step 1: ChromaDB cache lookup ─────────────────────────────────
        print("[CHAT] Checking ChromaDB car_knowledge cache …")
        try:
            from backend.vector.chroma_service import (
                get_model_knowledge,
                add_model_knowledge,
            )
            cached_text, cached_url = get_model_knowledge(
                year=year, make=make, model=model
            )
            if cached_text:
                research_text = cached_text
                research_url  = cached_url or ""
                research_used = True
                cache_hit     = True
                print(
                    f"[CHAT] ChromaDB CACHE HIT — {len(research_text)} chars "
                    f"from {research_url!r}"
                )
        except Exception as exc:
            print(f"[CHAT] ChromaDB lookup failed (non-fatal): {exc}")
            # define add_model_knowledge as None so the write-back is skipped
            add_model_knowledge = None  # type: ignore[assignment]

        # ── Step 2: Live web research (cache miss only) ───────────────────
        if not cache_hit:
            print("[CHAT] Cache miss — triggering WebResearcher …")
            try:
                from backend.utils.web_researcher import WebResearcher
                query = _build_search_query(car, msg)
                print(f"[CHAT] ACTION: Starting Web Research …")
                print(f"[CHAT] search_query = {query!r}")
                researcher = WebResearcher(timeout_ms=25_000, max_text_chars=2_000)
                result = researcher.search_and_summarize(query)

                if result and result.text:
                    research_text = result.text
                    research_url  = result.url
                    research_used = True
                    print(
                        f"[CHAT] Research SUCCESS: {len(result.text)} chars "
                        f"from {result.url}"
                    )

                    # ── Step 3: Write scraped data back to ChromaDB ───────
                    print("[CHAT] Writing research to ChromaDB car_knowledge …")
                    try:
                        if add_model_knowledge is not None:
                            stored = add_model_knowledge(
                                year=year, make=make, model=model,
                                text=result.text,
                                source_url=result.url,
                                trim=trim,
                            )
                            print(
                                f"[CHAT] ChromaDB write: {'OK ✓' if stored else 'FAILED'}"
                            )
                    except Exception as exc_store:
                        print(f"[CHAT] ChromaDB write EXCEPTION: {exc_store}")

                else:
                    print("[CHAT] Research returned None (no suitable page found).")

            except Exception as exc:
                print(f"[CHAT] Research EXCEPTION: {exc}")
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "[ai_agent] WebResearcher failed: %s", exc
                )

    print(f"[CHAT] research_used={research_used}  cache_hit={cache_hit}")

    # ── Assemble the "Context Sandwich" system prompt ─────────────────────
    system_parts: list[str] = [
        "You are a Dealership Expert Assistant with access to both local inventory "
        "data and live web research.\n\n"
        "RULES:\n"
        "1. Always prioritise the [Local Inventory Data] for specifics "
        "(price, VIN, mileage, stock status, dealer details). "
        "Never contradict the local data.\n"
        "2. Use [Internet Research Data] for general model knowledge, "
        "reliability trends, expert opinions, and market comparisons "
        "that are not available in the inventory record.\n"
        "3. If you use web research data, you MUST cite the source URL at the "
        "end of your answer on its own line, formatted as:\n"
        "   Source: <url>\n"
        "4. If the inventory data and web data conflict, trust the local data "
        "for this specific car and note the discrepancy.\n"
        "5. Keep answers focused and concise (3-5 sentences) unless the user "
        "asks for detail. Never invent specifications.\n\n",

        "── [Local Inventory Data] ──────────────────────────────────────────\n",
        local_context,
        "\n────────────────────────────────────────────────────────────────────\n",
    ]

    if research_used:
        system_parts += [
            "\n── [Internet Research Data] ────────────────────────────────────────\n",
            f"Source URL: {research_url}\n\n",
            research_text,
            "\n────────────────────────────────────────────────────────────────────\n",
            "\nRemember: cite the Source URL above at the end of your response "
            "if you draw on the Internet Research Data.\n",
        ]
    else:
        system_parts.append(
            "\nNo external web data was fetched for this question. "
            "Answer only from the Local Inventory Data and your general knowledge. "
            "If you lack information, say so clearly.\n"
        )

    system = "".join(system_parts)

    # ── Call the LLM ──────────────────────────────────────────────────────
    client = OpenAICompatibleClient()
    try:
        reply = client.complete_text(system=system, user=msg, temperature=0.45)
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
