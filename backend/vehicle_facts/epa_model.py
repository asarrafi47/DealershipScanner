"""
Listing model -> EPA catalog model names. One owner for what used to be four maps:

* ``knowledge_engine._model_epa_fallbacks`` (per-make rules + generic noise
  shedding; also read by ``catalog.resolver._model_variants``)
* ``trim_ladder_knowledge.naming._EPA_MODEL_SEARCH_NAMES`` /
  ``epa_model_search_name`` (ladder-family label)
* ``epa_master_store._bmw_epa_model`` / ``_mini_epa_model`` /
  ``_model_search_variants`` (series maps + separator stripping)
* ``model_specs_dictionary.iter_model_lookup_variants`` (powertrain suffix strips)

Each of those names is still importable from its old module and returns what it
returned before (they live here now). Every consumer asks
:func:`epa_model_candidates` for its OWN named list (``strategy=``): the lists
are not interchangeable. The knowledge-engine aggregate lookup averages across
every row of the model it matches, so a series fallback that is fine for row
fetching ("M" for an M2, MINI "Cooper" for a Cooper SE) is wrong there -- a
single merged list (phase 4, reverted 2026-10-01) put V8s on M2s and gas engines
on a Cooper SE.

Guards added on top of the pre-merge lists (each fixes a real wrong answer):

* A heavy-duty truck never sheds to its light-duty name ("Silverado 1500 HD" ->
  "Silverado", "Silverado 2500 HD" -> "Silverado" served 1500 specs/rows).
* In the knowledge-engine lookups an "EV" nameplate never sheds to its gas
  sibling via a per-make rule (Kia "Niro EV" -> "Niro"), nor a Volvo "... Pure
  Electric" to the bare base (-> "XC40"). The generic rule already guarded "EV".

Known and left: ``epa_model_search_name`` is a trim-ladder FAMILY label (S5 ->
"A4"), and the "fetch_rows" list still tries it as an EPA model, as it always did.
"""
from __future__ import annotations

import re
from functools import lru_cache

# ---------------------------------------------------------------------------
# (1) trim-ladder search names (was trim_ladder_knowledge/naming.py)
# ---------------------------------------------------------------------------
_EPA_MODEL_SEARCH_NAMES: dict[tuple[str, str], str] = {
    ("mini", "cooper"): "Cooper",
    ("mini", "countryman"): "Countryman",
    ("toyota", "crownsignia"): "Crown Signia",
    ("toyota", "grsupra"): "GR Supra",
    ("toyota", "corolla"): "Corolla",
    ("toyota", "bz"): "bZ",
    ("bmw", "2series"): "2 Series",
    ("bmw", "4series"): "4 Series",
    ("bmw", "8series"): "8 Series",
    ("jeep", "wagoneer"): "Wagoneer",
    ("jeep", "grandwagoneer"): "Grand Wagoneer",
    ("audi", "a8"): "A8",
    ("audi", "r8"): "R8",
    ("audi", "r8spyder"): "R8 Spyder",
    ("audi", "q6etron"): "Q6 e-tron",
    ("audi", "q8etron"): "Q8 e-tron",
    ("audi", "q5"): "Q5",
    ("audi", "a4"): "A4",
    ("audi", "a6"): "A6",
    ("audi", "a3"): "A3",
    ("audi", "q7"): "Q7",
    ("audi", "q8"): "Q8",
    ("audi", "rs6"): "RS 6",
    ("mercedesbenz", "glc"): "GLC-Class",
    ("mercedesbenz", "gle"): "GLE-Class",
    ("mercedesbenz", "gla"): "GLA-Class",
    ("mercedesbenz", "glb"): "GLB-Class",
    ("mercedesbenz", "gls"): "GLS-Class",
    ("mercedesbenz", "sl"): "SL-Class",
    ("mercedesbenz", "slclass"): "SL-Class",
    ("mercedesbenz", "slc"): "SLC-Class",
    ("mercedesbenz", "slk"): "SLK-Class",
    ("mercedesbenz", "cclass"): "C-Class",
    ("mercedesbenz", "eclass"): "E-Class",
    ("mercedesbenz", "cle"): "CLE-Class",
}


def epa_model_search_name(make: str, model: str | None) -> str:
    """Model label used when locating EPA CSV files (trim-ladder FAMILY label)."""
    from backend.enrichment.trim_ladder_knowledge.naming import _resolve_trim_model_key

    mk, mod = _resolve_trim_model_key(make, model)
    return _EPA_MODEL_SEARCH_NAMES.get((mk, mod), (model or "").strip())


# ---------------------------------------------------------------------------
# (2) BMW / MINI series maps (was epa_master_store.py)
# ---------------------------------------------------------------------------
_BMW_SERIES_MAP: dict[str, str] = {
    "2": "2 Series",
    "3": "3 Series",
    "4": "4 Series",
    "5": "5 Series",
    "6": "6 Series",
    "7": "7 Series",
    "8": "8 Series",
    "M2": "M",
    "M3": "M",
    "M4": "M",
    "M5": "M",
    "M6": "M",
    "M8": "M",
}

_MINI_MODEL_MAP: dict[str, str] = {
    "2 door": "Cooper",
    "4 door": "Cooper",
    "hardtop 2 door": "Cooper",
    "hardtop 4 door": "Cooper",
    "hardtop": "Cooper",
    "cooper hardtop": "Cooper",
    "convertible": "Cooper",
    "cooper roadster": "Roadster",
}


def _bmw_epa_model(model: str) -> str | None:
    m = model.strip()
    if m in _BMW_SERIES_MAP:
        return _BMW_SERIES_MAP[m]
    m_perf = re.match(r"^(M\d)(\d{2}[a-z]?)$", m, re.IGNORECASE)
    if m_perf:
        return _BMW_SERIES_MAP.get(m_perf.group(1)[1])
    digit_match = re.match(r"^([2-9])(\d{2}[a-z]?)", m, re.IGNORECASE)
    if digit_match:
        return _BMW_SERIES_MAP.get(digit_match.group(1))
    return None


def _mini_epa_model(model: str) -> str | None:
    return _MINI_MODEL_MAP.get(model.strip().lower())


#: Separators ``_model_search_variants`` cuts the model at (prefix kept).
_STORE_VARIANT_SEPARATORS: tuple[str, ...] = (
    " 3500 HD Chassis Cab", " 3500 HD", " 2500 HD", " 1500", " i-FORCE MAX", " PHEV",
    " Plug-In Hybrid", " Hybrid", " Energi", " GT", " GTS", " N Line", " L", " XL", " LS",
    " LT", " Limited", " Pro", " Sport",
)


def _model_search_variants_legacy(make_canonical: str, model: str) -> list[str]:
    """``epa_master_store._model_search_variants`` before the merge (parity view)."""
    md = (model or "").strip()
    if not md:
        return []
    out: list[str] = []
    seen: set[str] = set()

    def add(m: str) -> None:
        key = m.strip().lower()
        if m and key not in seen:
            seen.add(key)
            out.append(m.strip())

    mk = make_canonical
    add(md)
    add(epa_model_search_name(mk, md))
    if mk.upper() == "BMW":
        series = _bmw_epa_model(md)
        if series:
            add(series)
    if mk.upper() == "MINI":
        mini = _mini_epa_model(md)
        if mini:
            add(mini)
    for sep in _STORE_VARIANT_SEPARATORS:
        if sep.lower() in md.lower():
            add(md[: md.lower().index(sep.lower())].strip())
    return out


# ---------------------------------------------------------------------------
# (3) powertrain suffix strips (was model_specs_dictionary.py)
# ---------------------------------------------------------------------------
_VARIANT_SUFFIX_RES = (
    re.compile(r"\s+(plug[\s-]?in\s+)?hybrid\s*$", re.I),
    re.compile(r"\s+plug[\s-]?in\s+hybrid\s*$", re.I),
    re.compile(r"\s+4xe\s*$", re.I),
    re.compile(r"\s+phev\s*$", re.I),
    re.compile(r"\s+electric\s*$", re.I),
)


def iter_model_lookup_variants(model: str) -> list[str]:
    """Original model plus stripped suffixes (e.g. Elantra Hybrid → Elantra)."""
    m = (model or "").strip()
    if not m:
        return []
    seen: set[str] = set()
    out: list[str] = []
    for candidate in (m, m.lower(), m.title()):
        c = candidate.strip()
        if c and c.lower() not in seen:
            seen.add(c.lower())
            out.append(c)
    lower = m.lower()
    cur = lower
    for rx in _VARIANT_SUFFIX_RES:
        nxt = rx.sub("", cur).strip()
        if nxt and nxt != cur:
            for candidate in (nxt, nxt.title()):
                if candidate.lower() not in seen:
                    seen.add(candidate.lower())
                    out.append(candidate)
            cur = nxt
    return out


# ---------------------------------------------------------------------------
# (4) per-make EPA fallbacks (was knowledge_engine.py)
# ---------------------------------------------------------------------------
_EPA_MODEL_NOISE_SUFFIX_RE = re.compile(
    r"\s+(?:Sedan|Coupe|Hatchback|Wagon|Convertible|Cabriolet|Roadster|SUV|Minivan|"
    r"Van|Cargo(?:\s+Van)?|Passenger(?:\s+Van)?|Crew\s+Cab|Cutaway|Chassis(?:\s+Cab)?|"
    r"2WD|4WD|AWD|RWD|FWD|4X4|4X2|4xe|Max)\s*$",
    re.I,
)


def _model_epa_fallbacks(make: str | None, model: str | None) -> list[str]:
    """
    Alternative EPA model strings to try when exact model match fails.

    Many dealers store extended model names (e.g. "Silverado 1500", "GLC 300") while
    EPA uses base names ("Silverado", "GLC-Class"). Returns a list ordered most→least specific.
    """
    mk = (make or "").strip().upper()
    mo = (model or "").strip()
    mo_u = mo.upper()
    fallbacks: list[str] = []

    # --- Chevrolet: Silverado 1500 → Silverado (1500 ONLY — EPA has no >8500-GVWR
    # trucks, so 2500 HD/3500 HD must never inherit light-duty Silverado specs) ---
    if mk == "CHEVROLET":
        if re.match(r"^Silverado\s+1500\b", mo, re.I):
            fallbacks.append("Silverado")
        m = re.match(r"^(Colorado)\s+\S", mo, re.I)
        if m:
            fallbacks.append(m.group(1))
        # Corvette Stingray / Corvette Z06 → Corvette
        if re.match(r"^Corvette\s+\S", mo, re.I):
            fallbacks.append("Corvette")

    # --- GMC: Sierra 1500 → Sierra (1500 ONLY — no EPA data for the HDs, see
    # Silverado above; skip Sierra EV — different arch) ---
    if mk == "GMC":
        if re.match(r"^Sierra\s+1500\b", mo, re.I):
            fallbacks.append("Sierra")
        m = re.match(r"^(Canyon|Yukon|Acadia)\s+\S", mo, re.I)
        if m and "EV" not in mo_u:
            fallbacks.append(m.group(1))
        # HUMMER EV SUV → Hummer EV
        if re.match(r"^HUMMER\s+EV\s+SUV\b", mo, re.I):
            fallbacks.append("Hummer EV")

    # --- Buick: Encore GX → Encore ---
    if mk == "BUICK":
        if re.match(r"^Encore\s+GX\b", mo, re.I):
            fallbacks.append("Encore")

    # --- Kia: Sportage Hybrid / Niro PHEV → base model ---
    if mk == "KIA":
        stripped = re.sub(r"\s+(Hybrid|Plug[-\s]?In\s+Hybrid|PHEV|EV)\s*$", "", mo, flags=re.I).strip()
        if stripped.lower() != mo.lower():
            fallbacks.append(stripped)

    # --- Toyota: "Tundra Hybrid" → "Tundra", "Tacoma i-FORCE MAX" → "Tacoma", etc. ---
    if mk == "TOYOTA":
        stripped = re.sub(r"\s+(Hybrid|Plug[-\s]?In\s+Hybrid|i-FORCE\s+MAX)\s*$", "", mo, flags=re.I).strip()
        if stripped.lower() != mo.lower():
            fallbacks.append(stripped)

    # --- Hyundai: Kona N → Kona, Ioniq 5 N → Ioniq 5 ---
    if mk == "HYUNDAI":
        stripped = re.sub(r"\s+N\s*$", "", mo, flags=re.I).strip()
        if stripped.lower() != mo.lower():
            fallbacks.append(stripped)

    # --- Ram: 1500 Classic → 1500; ProMaster City variants ---
    if mk == "RAM":
        m = re.match(r"^(1500|2500|3500)\s+Classic\b", mo, re.I)
        if m:
            fallbacks.append(m.group(1))
        if re.match(r"^ProMaster\s+City\b", mo, re.I):
            fallbacks.append("Promaster City")

    # --- Chrysler: "Town & Country" → "Town and Country" ---
    if mk == "CHRYSLER":
        if re.match(r"^Town\s*&\s*Country\b", mo, re.I):
            fallbacks.append("Town and Country")

    # --- Volvo: "XC40 Recharge Pure Electric" / "XC60 Recharge" → base model ---
    if mk == "VOLVO":
        m = re.match(r"^(XC40|XC60|XC90|S60|S90|V60|V90)\s+\S", mo, re.I)
        if m:
            fallbacks.append(m.group(1))

    # --- Nissan: Kicks Play → Kicks; NV200 Compact Cargo → NV200 Cargo Van ---
    if mk == "NISSAN":
        if re.match(r"^Kicks\s+Play\b", mo, re.I):
            fallbacks.append("Kicks")
        if re.match(r"^NV200\s+Compact\s+Cargo\b", mo, re.I):
            fallbacks.append("NV200 Cargo Van")

    # --- Jeep: Wrangler Unlimited / Wrangler JK Unlimited → Wrangler; Wagoneer S → Wagoneer ---
    if mk == "JEEP":
        if re.match(r"^Wrangler\s+(Unlimited|JK\s+Unlimited)\b", mo, re.I):
            fallbacks.append("Wrangler")
        if re.match(r"^Wagoneer\s+S\b", mo, re.I):
            fallbacks.append("Wagoneer")

    # --- Mercedes-Benz: "GLC 300" → "GLC-Class", "E 350" → "E-Class", "AMG GLC63" → "GLC-Class" ---
    if "MERCEDES" in mk:
        seg_map = {
            "A": "A-Class", "C": "C-Class", "CLA": "CLA-Class", "CLE": "CLE-Class",
            "CLS": "CLS-Class", "E": "E-Class", "G": "G-Class",
            "GLA": "GLA-Class", "GLB": "GLB-Class", "GLC": "GLC-Class",
            "GLE": "GLE-Class", "GLS": "GLS-Class", "S": "S-Class", "SL": "SL-Class",
        }
        m = re.match(r"^([A-Z]+)\s+\d{3}\b", mo_u)
        if m:
            epa_cls = seg_map.get(m.group(1))
            if epa_cls:
                fallbacks.append(epa_cls)
        # "AMG GLC 63" form
        m2 = re.match(r"^AMG\s+([A-Z]+)\s*\d{2}", mo_u)
        if m2:
            epa_cls = seg_map.get(m2.group(1))
            if epa_cls:
                fallbacks.append(epa_cls)
        # Bare series ("GLC", "GLE") — dealer feeds often omit the number entirely
        m3 = re.match(r"^([A-Z]{1,3})\b", mo_u)
        if m3:
            epa_cls = seg_map.get(m3.group(1))
            if epa_cls and epa_cls.lower() != mo.lower():
                fallbacks.append(epa_cls)

    # --- Mitsubishi: Outlander Phev / Outlander Sport → Outlander ---
    if mk == "MITSUBISHI":
        if re.match(r"^Outlander\s+(Phev|Sport)\b", mo, re.I):
            fallbacks.append("Outlander")

    # --- Volkswagen: Atlas Cross Sport → Atlas ---
    if mk == "VOLKSWAGEN":
        if re.match(r"^Atlas\s+Cross\s+Sport\b", mo, re.I):
            fallbacks.append("Atlas")

    # --- Mazda: "MX-5 MIATA" → "MX-5"; "Mazda CX-9" / "Mazda3" → strip "Mazda" prefix ---
    if mk == "MAZDA":
        if re.match(r"^MX-5\s+MIATA\b", mo, re.I):
            fallbacks.append("MX-5")
        else:
            m = re.match(r"^Mazda\s+(.+)", mo, re.I)
            if m:
                fallbacks.append(m.group(1).strip())

    # --- BMW: numeric sedan/coupe → Series name (330i → "3 Series", M3 → "M", etc.) ---
    if mk == "BMW":
        mo_stripped = mo.strip()
        mo_s_u = mo_stripped.upper()
        # Standalone M models: M3, M4, M5, M8
        if re.match(r"^M[3458]\b", mo_s_u) and " " not in mo_stripped:
            fallbacks.append("M")
        # X3 M / X5 M / X6 M → try "M" then base X-series
        elif re.match(r"^X([3-6])\s+M\b", mo_s_u):
            fallbacks.append("M")
            base = re.match(r"^(X[3-6])\b", mo_s_u).group(1).title()
            fallbacks.append(base)
        # Standard numeric: 228i, 330i, 435i, 530e, 740i, 840i → "{n} Series"
        elif re.match(r"^[2-8]\d{2}[iIeE]?\b", mo_s_u) and " " not in mo_stripped:
            fallbacks.append(f"{mo_s_u[0]} Series")
        # M-prefix numeric: M235i, M340i, M440i → "{n} Series"
        elif re.match(r"^M([2-8])\d{2}[iI]\b", mo_s_u) and " " not in mo_stripped:
            s = re.match(r"^M([2-8])", mo_s_u).group(1)
            fallbacks.append(f"{s} Series")

    # --- Audi: dealer model strings vs EPA base names ---
    if mk == "AUDI":
        mo_compact = mo_u.replace("-", "")
        if re.match(r"^A8\b", mo, re.I):
            fallbacks.extend(["A8", "A8 L"])
        if re.match(r"^Q6\b", mo, re.I) and "ETRON" in mo_compact:
            fallbacks.extend(["Q6 e-tron", "Q6"])
        if re.match(r"^Q8\b", mo, re.I) and "ETRON" in mo_compact:
            fallbacks.extend(["Q8 e-tron", "Q8"])
        for base in ("A3", "A4", "A5", "A6", "A7", "A8", "Q3", "Q5", "Q6", "Q7", "Q8"):
            if re.match(rf"^{base}\b", mo, re.I):
                fallbacks.append(base)
        stripped = re.sub(r"\s+(Sportback|Sedan|Coupe|allroad)\s*$", "", mo, flags=re.I).strip()
        if stripped and stripped.lower() != mo.lower():
            fallbacks.append(stripped)

    # --- Generic (all makes): dealer feeds decorate models with sale status
    # ("New 2026 Hyundai IONIQ 5 SEL"), body style ("Accord Sedan"), and
    # drivetrain ("Tacoma 2WD") tokens EPA never uses. Candidates go most→least
    # specific; the caller stops at the first hit, so shorter (riskier) forms
    # only fire when everything longer missed. ---
    generic: list[str] = []
    cleaned = re.sub(
        r"^(?:New|Used|Certified(?:\s+Pre[-\s]?Owned)?)\s+(?:\d{4}\s+)?", "", mo, flags=re.I
    ).strip()
    if mk and cleaned.upper().startswith(mk + " "):
        cleaned = cleaned[len(mk):].strip()
    if cleaned:
        generic.append(cleaned)
        cur = cleaned
        while True:
            stripped = _EPA_MODEL_NOISE_SUFFIX_RE.sub("", cur).strip()
            if not stripped or stripped.lower() == cur.lower():
                break
            cur = stripped
            generic.append(cur)
        # Last resort: shed trailing words (trim levels like "SEL" / "Limited"),
        # at most three, never below one word.
        words = cur.split()
        for i in range(len(words) - 1, max(0, len(words) - 4), -1):
            generic.append(" ".join(words[:i]))

    # A heavy-duty truck ("Silverado 2500HD") must never shed down to the
    # light-duty base name (EPA has no >8500-GVWR trucks — that fallback would
    # serve 1500 specs on an HD listing), and an EV nameplate ("Silverado EV")
    # must never shed down to its gas sibling.
    _hd_re = re.compile(r"([2-5]500|\bHD\b|\bEV\b)")
    is_hd = bool(_hd_re.search(mo_u))
    seen = {mo.lower()} | {f.lower() for f in fallbacks}
    for cand in generic:
        c = cand.strip()
        if is_hd and not _hd_re.search(c.upper()):
            continue
        if len(c) >= 2 and c.lower() not in seen:
            seen.add(c.lower())
            fallbacks.append(c)

    return fallbacks


# ---------------------------------------------------------------------------
# (5) catalog resolver powertrain suffixes (was catalog/resolver.py)
# ---------------------------------------------------------------------------
# Powertrain suffixes dealers append to model names that EPA folds into trims
_MODEL_SUFFIXES = (
    " plug-in hybrid electric vehicle", " hybrid electric vehicle",
    " electric vehicle", " plug-in hybrid", " i-force max", " hybrid max",
    " hybrid", " prime", " phev", " ev",
)


def _resolver_model_variants_legacy(make: str, model: str) -> list[str]:
    """``catalog.resolver._model_variants`` before the merge (parity view)."""
    out = [model.strip()]
    try:
        from backend.utils.model_aliases import alias_spellings

        for alt in alias_spellings(make, model):
            if alt and alt not in out:
                out.append(alt)
    except ImportError:
        pass
    low = model.lower()
    for suf in _MODEL_SUFFIXES:
        if low.endswith(suf):
            base = model[: len(model) - len(suf)].strip()
            if base and base not in out:
                out.append(base)
    for fb in _model_epa_fallbacks(make, model):
        if fb and fb not in out:
            out.append(fb)
    return out


# ---------------------------------------------------------------------------
# Per-consumer candidate lists (named strategies)
# ---------------------------------------------------------------------------
# Phase 4 first merged the four maps into one list for every consumer. That was a
# regression: the knowledge-engine aggregate / by-trim lookups AVERAGE over every
# row of the model they match, so the series fallbacks built for row fetching
# ("M" for an M2, MINI "Cooper" for a Cooper SE, "4 Series" for a 428i Gran Coupe)
# filled V8s into M2s, gas engines into a Cooper SE and so on, and the merged EV
# guard cost the Macan Electric / CLA 350 Electric / C40 Recharge their correct
# EV rows. Each consumer now gets back exactly the list it had before the merge,
# plus only the guards proven to fix a real wrong answer (see _TRIM_LOOKUP_GUARD,
# _FETCH_ROWS_GUARD).

#: The consumers. "by_trim" and "aggregate" are the two knowledge-engine EPA
#: lookups (``_lookup_epa_by_trim_uncached`` / ``_lookup_epa_aggregate_uncached``,
#: read by knowledge_engine_specs, spec_backfill and scripts/backfill_specs);
#: "fetch_rows" is ``epa_master_store._model_search_variants`` (fetch_epa_rows,
#: dictionary_options_store); "catalog" is ``catalog.resolver._model_variants``
#: (scores powertrain on every matched row); "model_specs" is the
#: ``model_specs`` dictionary lookup.
STRATEGIES: tuple[str, ...] = ("by_trim", "aggregate", "fetch_rows", "catalog", "model_specs")

# by_trim / aggregate: ``_model_epa_fallbacks`` already refused to shed a
# heavy-duty or "EV" nameplate in its GENERIC candidates; its per-make rules did
# not, so "Silverado 1500 HD" -> "Silverado" (light-duty specs on an HD) and Kia
# "Niro EV" -> "Niro" (gas rows on an EV). The same guard now covers every
# candidate. The pattern is exactly the generic one (no bare "Electric": the
# generic "Macan Electric" -> "Macan" / "C40 Recharge" -> "C40" sheds land on the
# correct EV rows because those lookups pick by trim).
_TRIM_LOOKUP_GUARD = re.compile(r"([2-5]500|\bHD\b|\bEV\b)")
# Volvo's per-make rule turns "XC40 Recharge Pure Electric" into the bare "XC40"
# (gas / mild-hybrid rows). For a Volvo nameplate that says Electric that bare
# base is dropped; the generic sheds stay ("C40 Recharge Pure Electric" -> "C40"
# is EV-only in EPA and was already right).
_VOLVO_ELECTRIC_RE = re.compile(r"\bElectric\b", re.I)
_VOLVO_RULE_BASE_RE = re.compile(r"^(XC40|XC60|XC90|S60|S90|V60|V90)$", re.I)
# fetch_rows: the " 2500 HD" / " 3500 HD" separators served Silverado/Sierra
# light-duty rows to HD trucks; an HD nameplate keeps only HD candidates.
_FETCH_ROWS_GUARD = re.compile(r"[2-5]500|\bHD\b", re.I)


def _canonical_make(make: str) -> str:
    try:
        from backend.enrichment.dictionary_catalog import canonical_make

        return canonical_make(make)
    except Exception:
        return (make or "").strip()


def _trim_lookup_candidates(make: str, model: str) -> list[str]:
    """``knowledge_engine._model_epa_fallbacks`` with the HD / EV guard on every rule."""
    fbs = _model_epa_fallbacks(make, model)
    mo_u = model.strip().upper()
    if _TRIM_LOOKUP_GUARD.search(mo_u):
        fbs = [c for c in fbs if _TRIM_LOOKUP_GUARD.search(c.upper())]
    if (make or "").strip().upper() == "VOLVO" and _VOLVO_ELECTRIC_RE.search(model):
        fbs = [c for c in fbs if not _VOLVO_RULE_BASE_RE.match(c.strip())]
    return fbs


def _fetch_rows_candidates(make: str, model: str) -> list[str]:
    """``epa_master_store._model_search_variants`` with the HD guard."""
    out = _model_search_variants_legacy(_canonical_make(make), model)
    if out and _FETCH_ROWS_GUARD.search(out[0]):
        out = [out[0]] + [c for c in out[1:] if _FETCH_ROWS_GUARD.search(c)]
    return out


@lru_cache(maxsize=16384)
def _candidates_cached(make: str, model: str, strategy: str) -> tuple[str, ...]:
    if strategy == "catalog":  # the resolver's list always starts with the model, even ""
        return tuple(_resolver_model_variants_legacy(make, model))
    if not model:
        return ()
    if strategy in ("by_trim", "aggregate"):
        return tuple(_trim_lookup_candidates(make, model))
    if strategy == "fetch_rows":
        return tuple(_fetch_rows_candidates(make, model))
    if strategy == "model_specs":
        return tuple(iter_model_lookup_variants(model))
    raise ValueError(f"unknown epa_model_candidates strategy {strategy!r}; one of {STRATEGIES}")


def epa_model_candidates(
    make: str | None,
    model: str | None,
    trim: str | None = None,
    *,
    strategy: str,
) -> list[str]:
    """EPA model names to try for a listing, for one named consumer (*strategy*).

    * ``"by_trim"`` / ``"aggregate"`` -- the knowledge-engine EPA lookups'
      fallbacks (tried after the listing model itself; NOT including it). These
      lookups average or trim-match within the model they hit, so they never get
      series fallbacks ("M", "4 Series", MINI "Cooper") or suffix strips.
    * ``"fetch_rows"`` -- listing model first, same-name ladder label, BMW/MINI
      series, separator strips (``fetch_epa_rows`` takes whole-series rows).
    * ``"catalog"`` -- the resolver's list (listing model, alias spellings,
      powertrain suffix strips, the per-make fallbacks); it scores fuel and
      electrification on every matched row, so an EV may shed to its base name.
    * ``"model_specs"`` -- listing model + case variants + powertrain suffix strips.

    *trim* is accepted for signature symmetry; no list depends on it.
    """
    del trim
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown epa_model_candidates strategy {strategy!r}; one of {STRATEGIES}")
    return list(_candidates_cached((make or "").strip(), (model or "").strip(), strategy))


def clear_cache() -> None:
    _candidates_cached.cache_clear()
