"""Model-name equivalence across the three vocabularies we compare:
the dealer feed, the NHTSA vPIC decode, and the EPA catalog.

Why: 31 of 1,611 rows in the 2026-09-23 lab were flagged "model differs from VIN
decode" for names that mean the same car:

  dealer "Prius Plug-in Hybrid"   vPIC "Prius Prime (PHEV)"   EPA "Prius Prime" (<=2025) / "Prius Plug-in Hybrid" (2026+)
  dealer "RAV4 Plug-In Hybrid"    vPIC "RAV4 Prime (PHEV)"
  dealer "2 Series" + trim "228 xDrive Gran Coupe"   vPIC "228i"   (BMW: vPIC names the variant, dealers the series)
  dealer "Silverado 2500HD"       vPIC "Silverado HD"
  dealer "Sierra 1500 Limited"    vPIC "Sierra Limited"
  NOT an alias: Toyota bZ4X vs Subaru Solterra. Same Motomachi line, different makes,
  and vPIC tells them apart by VIN position 7 (JTMABA*C*A = bZ4X, JTMABA*B*A = Solterra).
  A "Toyota bZ4X" whose VIN decodes as Solterra is a dealer listing error, keep the flag.

Two entry points:
  canonical_model(make, model)  -> one spelling per car line, used for matching
  models_equivalent(make, dealer_model, dealer_trim, other_model) -> bool
"""
from __future__ import annotations

import re

# Explicit twins, keyed by (make upper, normalized model). Value is the canonical
# normalized spelling. Keep this table small: generic rules below cover the
# common patterns (series numbers, HD suffixes, Prime/Plug-in).
_ALIASES: dict[tuple[str, str], str] = {
    ("TOYOTA", "priusprime"): "priusplug-inhybrid",
    ("TOYOTA", "priuspluginhybrid"): "priusplug-inhybrid",
    ("TOYOTA", "priusplug-inhybrid"): "priusplug-inhybrid",
    ("TOYOTA", "rav4prime"): "rav4plug-inhybrid",
    ("TOYOTA", "rav4pluginhybrid"): "rav4plug-inhybrid",
    ("TOYOTA", "rav4plug-inhybrid"): "rav4plug-inhybrid",
}

_PHEV_TAG_RE = re.compile(r"\s*\((?:phev|hev|bev|ev)\)\s*$", re.I)
_HD_RE = re.compile(r"\b(\d{4})\s*hd\b", re.I)        # "2500HD" / "2500 HD" -> "HD"
_HALF_TON_RE = re.compile(r"\b1500\b\s*", re.I)        # "Sierra 1500 Limited" -> "Sierra Limited"
_BMW_VARIANT_RE = re.compile(r"^(?:m)?(\d)\d{2}(?:[a-z]{1,2})?$", re.I)  # 228i, 330e, M440i, 740i -> series digit


def _norm(s: str | None) -> str:
    return re.sub(r"[^a-z0-9-]", "", str(s or "").lower())


def canonical_model(make: str | None, model: str | None) -> str:
    """Normalized, alias-folded model spelling for matching (not for display)."""
    mk = str(make or "").strip().upper()
    m = _PHEV_TAG_RE.sub("", str(model or "").strip())
    m = _HD_RE.sub("HD", m)
    m = _HALF_TON_RE.sub("", m)
    key = _norm(m)
    return _ALIASES.get((mk, key), key)


# Display spellings per canonical key, for catalog lookups that need the other
# vocabulary's exact model string (EPA: "Prius Prime" through 2025).
_SPELLINGS: dict[str, tuple[str, ...]] = {
    "priusplug-inhybrid": ("Prius Prime", "Prius Plug-in Hybrid"),
    "rav4plug-inhybrid": ("RAV4 Prime", "RAV4 Plug-in Hybrid"),
}


def alias_spellings(make: str | None, model: str | None) -> list[str]:
    """Other spellings of the same car line, for catalog candidate queries."""
    key = canonical_model(make, model)
    out = [s for s in _SPELLINGS.get(key, ()) if s.lower() != str(model or "").strip().lower()]
    m = str(model or "").strip()
    hd = _HD_RE.sub("HD", m)
    if hd != m:
        out.append(hd)
    half = _HALF_TON_RE.sub("", m).strip()
    if half != m and half:
        out.append(half)
    return out


# Make spellings that mean the same manufacturer in the vocabularies we compare:
# vPIC files GM medium-duty trucks under "GM" (Silverado 5500HD), Stellantis'
# Wagoneer under "JEEP"; dealers file them under the showroom brand.
_MAKE_TWINS: tuple[frozenset[str], ...] = (
    frozenset({"gm", "chevrolet", "gmc"}),
    frozenset({"jeep", "wagoneer"}),
    frozenset({"ram", "dodge"}),
    frozenset({"mercedes-benz", "mercedes", "mercedesbenz"}),
)


def makes_equivalent(a: str | None, b: str | None) -> bool:
    x, y = _norm(a), _norm(b)
    if not x or not y:
        return True
    if x == y or x in y or y in x:
        return True
    return any(x in t and y in t for t in _MAKE_TWINS)


# vPIC names the family or the variant where dealers name the trim line:
#   "Super Duty" (dealer) vs "F-250" / "F-350" (vPIC); "Cooper S" vs "Hardtop";
#   "GLC 300" vs "GLC-Class"; a bare GM chassis code ("GM515") for a 6500HD.
_MODEL_FAMILIES: tuple[tuple[str, ...], ...] = (
    ("superduty", "f-250", "f-350", "f-450", "f-550", "f-600", "f250", "f350", "f450", "f550"),
    ("cooper", "coopers", "hardtop", "hardtop2door", "hardtop4door", "clubman", "countryman", "convertible"),
)
_VPIC_CODE_RE = re.compile(r"^[a-z]{1,3}\d{3,}$")  # "gm515": a platform code, says nothing


def _family(key: str) -> tuple[str, ...] | None:
    for fam in _MODEL_FAMILIES:
        if any(key == f or key.startswith(f) for f in fam):
            return fam
    return None


def bmw_series_of(model: str | None) -> str | None:
    """'228i' / 'M440i' / '740i' -> '2 Series' / '4 Series' / '7 Series'; else None."""
    m = str(model or "").strip()
    mm = _BMW_VARIANT_RE.match(m)
    return f"{mm.group(1)} Series" if mm else None


def models_equivalent(make: str | None, dealer_model: str | None, dealer_trim: str | None, other_model: str | None) -> bool:
    """True when *other_model* (vPIC or EPA spelling) names the same car line as
    the dealer's model (+trim). Conservative: containment after canonicalization,
    plus the BMW series rule."""
    a = canonical_model(make, dealer_model)
    b = canonical_model(make, other_model)
    if not a or not b:
        return True  # nothing to compare
    if a == b or a in b or b in a:
        return True
    # vPIC platform codes ("GM515" for a Silverado 6500HD) name nothing a dealer would
    if _VPIC_CODE_RE.match(b) or _VPIC_CODE_RE.match(a):
        return True
    # family names: "GLC-Class" vs "GLC 300", "E-Class" vs "E 450"
    fa, fb = a.replace("-class", ""), b.replace("-class", "")
    if fa and fb and (fa == fb or fa.startswith(fb) or fb.startswith(fa)):
        return True
    # dealer trim line vs vPIC variant of one family: "Super Duty" vs "F-350", "Cooper S" vs "Hardtop"
    if _family(a) is not None and _family(a) is _family(b):
        return True
    mk = str(make or "").strip().upper()
    if mk == "BMW":
        series = bmw_series_of(other_model)
        if series and canonical_model(make, series) == a:
            return True
        # the variant may sit in the dealer's trim: "2 Series" + "228 xDrive Gran Coupe" vs "228i"
        digits_other = re.sub(r"\D", "", str(other_model or ""))
        digits_trim = re.sub(r"\D", "", str(dealer_trim or ""))
        if digits_other and digits_trim and digits_other[:3] == digits_trim[:3]:
            return True
    # model+trim combined ("Sierra" + "1500 Limited")
    combo = canonical_model(make, f"{dealer_model or ''} {dealer_trim or ''}")
    return bool(b) and (b in combo)
