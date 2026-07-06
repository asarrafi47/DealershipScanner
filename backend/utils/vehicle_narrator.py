"""
Grounded vehicle narration: turn a structured car row into natural prose.

The model is given ONLY the fields we hold and is instructed to narrate them —
never to invent specs, prices, or claims. This is the cheap, reliable core of the
"we have the data, the LLM just writes sentences" design: accuracy comes from the
row, fluency from the model. Unknown fields are simply omitted, never guessed.
"""
from __future__ import annotations

import re
from typing import Any

from backend.utils.local_llm import generate

# Fields worth narrating, in a sensible order, with human labels.
_NARRATABLE: tuple[tuple[str, str], ...] = (
    ("year", "Year"),
    ("make", "Make"),
    ("model", "Model"),
    ("trim", "Trim"),
    ("body_style", "Body style"),
    ("price", "Price (USD)"),
    ("msrp", "MSRP (USD)"),
    ("mileage", "Mileage"),
    ("exterior_color", "Exterior color"),
    ("interior_color", "Interior color"),
    ("engine_description", "Engine"),
    ("cylinders", "Cylinders"),
    ("fuel_type", "Fuel type"),
    ("transmission", "Transmission"),
    ("drivetrain", "Drivetrain"),
    ("mpg_city", "MPG city"),
    ("mpg_highway", "MPG highway"),
    ("is_cpo", "Certified pre-owned"),
)

_SYSTEM = (
    "You are a factual automotive copywriter for a car marketplace. "
    "You are given the ONLY known facts about one specific vehicle. "
    "Write a natural 2-3 sentence description using ONLY those facts. "
    "Absolute rules:\n"
    "- Never invent or infer any spec, price, feature, or history not listed.\n"
    "- Never claim anything about ownership, maintenance, accident history, "
    "condition, reliability, resale/investment value, or how well-kept it is — "
    "you do not know these.\n"
    "- No hype words (best, flawless, pristine, immaculate, excellent, gem, "
    "collector, investment).\n"
    "- Do not mention absent fields.\n"
    "State only what is given, plainly, as if listing the car to a buyer."
)

# Phrases that signal an unverifiable claim the model must not make.
_BANNED = re.compile(
    r"\b(one[- ]owner|single[- ]owner|original owner|well[- ]maintained|well[- ]kept|"
    r"garage[- ]kept|pristine|immaculate|flawless|mint|excellent condition|great condition|"
    r"like new|collector|investment|no accident|clean history|reliable|dependable|"
    r"best|gem|rare find|must[- ]see|priced to sell|great deal|steal)\b",
    re.I,
)


def _has_unverifiable_claim(text: str) -> bool:
    return bool(_BANNED.search(text or ""))


_HTML_TAG = re.compile(r"<[^>]+>")
_LABELED_LINE = re.compile(r"(?im)^\s*(year|make|model|trim|price|mileage|engine|"
                           r"body style|exterior color|interior color|fuel type|"
                           r"transmission|drivetrain|mpg)\b\s*[:\-]")


def _is_degenerate(text: str) -> bool:
    """
    True when a weak model echoed the fact list instead of writing prose:
    HTML break tags, or 3+ 'Label: value' lines (a listing, not a description).
    """
    if not text:
        return True
    if "<br" in text.lower() or "•" in text:
        return True
    return len(_LABELED_LINE.findall(text)) >= 3


def _sanitize(text: str) -> str:
    return _HTML_TAG.sub("", text or "").strip()


def _template_fallback(row: dict[str, Any], facts: list[str]) -> str:
    """Deterministic, guaranteed-grounded sentence built straight from the facts."""
    head = " ".join(
        str(row.get(k)) for k in ("year", "make", "model", "trim") if row.get(k)
    ).strip()
    tail_bits: list[str] = []
    if row.get("mileage") is not None:
        try:
            tail_bits.append(f"{int(row['mileage']):,} miles")
        except (TypeError, ValueError):
            pass
    for k in ("exterior_color", "drivetrain", "transmission", "engine_description"):
        if row.get(k):
            tail_bits.append(str(row[k]))
    tail = ", ".join(tail_bits)
    return f"{head}. {tail}." if tail else f"{head}."


def _known_facts(row: dict[str, Any]) -> list[str]:
    facts: list[str] = []
    for key, label in _NARRATABLE:
        val = row.get(key)
        if val is None or (isinstance(val, str) and not val.strip()):
            continue
        if key == "is_cpo":
            if val in (True, 1, "1", "true", "True"):
                facts.append("Certified pre-owned: yes")
            continue
        if key in ("price", "msrp"):
            try:
                facts.append(f"{label}: ${int(float(val)):,}")
                continue
            except (TypeError, ValueError):
                pass
        if key == "mileage":
            try:
                facts.append(f"{label}: {int(val):,} miles")
                continue
            except (TypeError, ValueError):
                pass
        facts.append(f"{label}: {val}")
    return facts


def build_prompt(row: dict[str, Any]) -> str:
    facts = _known_facts(row)
    body = "\n".join(f"- {f}" for f in facts)
    return (
        "Known facts about this vehicle:\n"
        f"{body}\n\n"
        "Write the description now."
    )


def narrate_vehicle(row: dict[str, Any], *, model: str | None = None) -> str:
    """
    Return a grounded 2-3 sentence description of *row* (auto-picks model tier).

    Small models occasionally slip in unverifiable claims (ownership, condition,
    "investment"); we regenerate once at temperature 0 with a stricter reminder,
    then fall back to a deterministic fact-only sentence rather than ship a claim
    we can't stand behind.
    """
    facts = _known_facts(row)
    if len(facts) < 2:
        return _template_fallback(row, facts) or "Vehicle details unavailable."
    prompt = build_prompt(row)

    def _bad(t: str) -> bool:
        return (not t) or _has_unverifiable_claim(t) or _is_degenerate(t)

    out = _sanitize(generate(prompt, system=_SYSTEM, model=model, temperature=0.2, max_tokens=180))
    if _bad(out):
        out = _sanitize(generate(
            prompt + "\n\nWrite flowing prose sentences, not a list. State ONLY the "
            "listed facts. No claims about ownership, condition, history, or value.",
            system=_SYSTEM, model=model, temperature=0.0, max_tokens=180,
        ))
    if _bad(out):
        # Deterministic, guaranteed-grounded — never ship a hallucination or a list dump.
        return _template_fallback(row, facts)
    return out
