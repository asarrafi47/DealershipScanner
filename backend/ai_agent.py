"""
Context-aware AI co-pilot: OpenAI GPT-4o + EPA / trim verification (Truth Engine).
Requires OPENAI_API_KEY. Tool: verify_car_data(vin).
"""
from __future__ import annotations

import json
import os
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


def run_car_page_chat(car: dict[str, Any], user_message: str) -> dict[str, Any]:
    """
    Car detail chatbot: Ollama (OpenAI-compatible) via llm.providers.ollama_client.
    ``car`` should be the full SQLite row dict from get_car_by_id.
    """
    msg = (user_message or "").strip()
    if not msg:
        return {"reply": "", "error": "empty_message", "discrepancy_flags": []}
    try:
        from llm.client import LLMResponseError
        from llm.providers.ollama_client import OpenAICompatibleClient
    except ImportError as e:
        return {
            "reply": "",
            "error": f"llm_import:{e}",
            "discrepancy_flags": [],
        }

    client = OpenAICompatibleClient()
    car_blob = json.dumps(car, indent=2, default=str)
    if len(car_blob) > 14000:
        car_blob = car_blob[:14000] + "\n…(truncated)"
    system = (
        "You are a helpful assistant on a dealership vehicle detail page.\n"
        "Use the vehicle JSON as the source of truth. If information is missing, say you do not see it.\n"
        "Keep answers concise (a few sentences unless the user asks for detail).\n\n"
        "Vehicle data (JSON):\n"
        + car_blob
    )
    try:
        reply = client.complete_text(system=system, user=msg, temperature=0.45)
    except LLMResponseError as e:
        return {"reply": "", "error": str(e), "discrepancy_flags": []}
    except Exception as e:
        return {"reply": "", "error": str(e)[:500], "discrepancy_flags": []}
    return {"reply": reply, "error": None, "discrepancy_flags": []}
