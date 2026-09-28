"""Rooftop attribution as a scored matcher.

The gate in ``backend.parsers`` decides, for every row of a group-feed payload, whether the
car belongs to the store being scanned. It does so by a ladder of unique-winner tiers
(``_pick_target``) followed by three keep rules (same feed source, own street block,
unstamped rows under the store's own source). That procedure grew one rule per incident
(2026-08-03 .. 2026-09-26) and its verdicts are only visible in log lines.

This module is the same decision procedure written as a scorer: every signal that speaks
for a rooftop is listed with its weight, the winning signal keeps the tier name the gate has
always logged (``name_exact``, ``street_address``, ``city_state`` …), and a row's result
says what it was kept or refused on. The decision procedure is deliberately IDENTICAL to
the legacy gate when no extra evidence is given — the regression corpus
(``backend/tests/test_rooftop_corpus.py``) holds the proof — and the gate calls it behind
``SCANNER_ROOFTOP_SCORER`` (default on; ``0`` restores the legacy path for one release).

Signals
-------
Strong signals prove IDENTITY (a unique winner keeps the store's rows, refuses the siblings
with ``sibling_rooftop`` — evidence enough to un-list a car — and lets the store's own feed
sources vouch for unnamed / unstamped rows):

* ``site_host``                          the rooftop's site is this store's host (only when the payload shows >1 host)
* ``identity_feed`` / ``page_oem_code``  the row's feed source is one the synthesis replay verified as this store's
                                         (evidence only: ``evidence["identity_sources"]`` / ``evidence["page_oem_code"]``)
* ``name_*`` / ``roster_name_alias``     the feed names the store (exact, tokens, superset, Stellantis brand alias, contains, recorded alias)
* ``store_label_*``                      the free-text store label (CarsCommerce ``custom_location``) names the store — MATCH-ONLY:
                                         it never creates a rooftop, never refuses a payload (2026-09-25, four dealers zeroed)
* ``dealer_id_host`` / ``rooftop_slug_*`` the host stem or the feed's own rooftop slug names the store
* ``location_facet_value``               the site's Location facet on the row spells the store's name (evidence only)
* ``street_address*``                    the rooftop's street is the store's street (registry, or the page's JSON-LD street);
                                         demoted to weak when the registry street is a free-text scrape (``site_text``)
* ``own_street_block`` / ``same_source_feed`` / ``unstamped_under_own_source``
                                         corroboration keeps beside a strong target

Weak signals prove LOCALITY only — ``zip_code`` and ``city_state``. Two rooftops of one
group can share a zip and certainly a city, and this gate also runs per page, so a weak
unique winner can be an artefact of which rows landed on the page (bmwofmurrieta-com,
2026-08: 1,697 of 1,941 cars queued to be un-listed). A weak match keeps the store's own
rows (keeping can only add inventory) but is NEVER SUFFICIENT for anything else: siblings
are refused with ``sibling_rooftop_weak_tier`` (refuse the write, never un-list), and no
feed source is trusted to vouch for unnamed or unstamped rows (hendrickhonda-com kept 73
foreign rows under ``RHendrickUsed`` on 2026-09-26). ``strong`` is False on such results.

A store label (``alt_name``) alone is never sufficient either: a row whose only stamp is a
``custom_location`` has no rooftop at all (``carscommerce.rooftop_of`` returns none) and
is an unstamped row — kept only under the store's own source.

Scores are for reading, not for deciding: ``score`` = the winning signal's weight plus a
small corroboration bonus, so a triage list sorts strongest-first; the decision is the
unique-winner ladder above, unchanged.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any

_log = logging.getLogger(__name__)

FLAG_ENV = "SCANNER_ROOFTOP_SCORER"

# signal -> (weight, strong). Order here is documentation; the ladder order is
# tier_table()'s, which mirrors backend.parsers._pick_target exactly.
SIGNAL_WEIGHTS: dict[str, tuple[float, bool]] = {
    "feed_scoped": (10.0, True),
    "site_host": (10.0, True),
    "identity_feed": (9.8, True),
    "page_oem_code": (9.8, True),
    "name_exact": (9.5, True),
    "name_tokens": (9.2, True),
    "name_token_subset": (9.0, True),
    "name_brand_alias": (8.8, True),
    "name_brand_alias_subset": (8.6, True),
    "name_contains": (8.4, True),
    "roster_name_alias": (8.2, True),
    "store_label_exact": (7.6, True),
    "store_label_tokens": (7.4, True),
    "store_label_token_subset": (7.2, True),
    "store_label_alias": (7.0, True),
    "dealer_id_host": (6.8, True),
    "rooftop_slug_host": (6.6, True),
    "rooftop_slug_name_tokens": (6.4, True),
    "location_facet_value": (6.2, True),
    "street_address": (6.0, True),
    "street_address_partial": (5.8, True),
    "street_address_suffix": (5.6, True),
    "street_address_suffix_partial": (5.4, True),
    "own_street_block": (5.0, True),
    "same_source_feed": (4.5, True),
    "unstamped_under_own_source": (4.0, True),
    "zip_code": (2.5, False),
    "city_state": (2.0, False),
    # kept for lack of evidence, not on evidence
    "single_rooftop": (0.0, False),
    "no_rooftop_evidence": (0.0, False),
}
# A keep on evidence needs at least this much AND a strong signal; below it the keep is
# a weak (locality) keep or a no-evidence keep, and ``strong`` says so.
STRONG_THRESHOLD = 3.0
CORROBORATION_BONUS = 0.25
CORROBORATION_CAP = 1.0

# Never sufficient on their own: these can find the store but cannot vouch or un-list.
NEVER_SUFFICIENT_ALONE = frozenset({"zip_code", "city_state", "store_label_alone"})

REJECT_SIBLING = "sibling_rooftop"
REJECT_SIBLING_WEAK = "sibling_rooftop_weak_tier"
REJECT_SINGLE = "single_rooftop_is_not_this_store"
REJECT_UNSTAMPED = "unstamped_row_in_group_feed"
REJECT_UNIDENTIFIED = "target_rooftop_unidentified"


def scorer_enabled() -> bool:
    """``SCANNER_ROOFTOP_SCORER`` — on unless set to 0 / false / off / no."""
    raw = str(os.environ.get(FLAG_ENV, "1")).strip().lower()
    return raw not in {"0", "false", "off", "no", ""}


def _parsers():
    # Imported at call time: backend.parsers imports this module lazily from inside
    # the gate, and the tests monkeypatch ``backend.parsers.roster_name_aliases``.
    from backend import parsers as p

    return p


def _signal(name: str, detail: str = "", *, unique: bool | None = None) -> dict[str, Any]:
    weight, strong = SIGNAL_WEIGHTS.get(name, (0.0, False))
    out: dict[str, Any] = {"signal": name, "weight": weight, "strong": strong}
    if detail:
        out["detail"] = detail
    if unique is not None:
        out["unique"] = unique
    return out


def _score(winning: str, signals: list[dict[str, Any]]) -> float:
    base = SIGNAL_WEIGHTS.get(winning, (0.0, False))[0]
    others = [s for s in signals if s["signal"] != winning]
    return round(base + min(CORROBORATION_CAP, CORROBORATION_BONUS * len(others)), 2)


def _result(decision: str, tier: str, signals: list[dict[str, Any]], *, strong: bool | None = None) -> dict[str, Any]:
    if strong is None:
        strong = decision == "keep" and SIGNAL_WEIGHTS.get(tier, (0.0, False))[1] and _score(tier, signals) >= STRONG_THRESHOLD
    return {"decision": decision, "tier": tier, "score": _score(tier, signals), "strong": bool(strong), "signals": signals}


# ── the tier ladder, as a table ──────────────────────────────────────────────

def tier_table(rooftops: list, roster: dict[str, Any], place: tuple[str, str, str, str], aliases: tuple[str, ...]) -> list[tuple[str, list]]:
    """Every tier of ``backend.parsers._pick_target`` in order, with the rooftops it
    matches. The target is the first tier with exactly one match. Guards, order and
    predicates are the legacy gate's, kept in lockstep on purpose."""
    p = _parsers()
    dealer_name = str(roster.get("name") or "")
    dealer_id = str(roster.get("dealer_id") or "")
    table: list[tuple[str, list]] = []

    target_host = p._host(roster.get("url"))
    host_evidence = {h for rt in rooftops for h in rt.hosts}
    if target_host and len(host_evidence) > 1:
        table.append(("site_host", [rt for rt in rooftops if target_host in rt.hosts]))

    target_nrm = p._nrm(dealer_name)
    target_tokens = p._tokens(dealer_name)
    if target_nrm:
        table.append(("name_exact", [rt for rt in rooftops if any(p._nrm(n) == target_nrm for n in rt.names)]))
        if target_tokens:
            table.append(("name_tokens", [rt for rt in rooftops if any(p._tokens(n) == target_tokens for n in rt.names)]))
            table.append(("name_token_subset", [rt for rt in rooftops if any(target_tokens < p._tokens(n) for n in rt.names)]))
            target_brand = p._brand_canonical_tokens(dealer_name)
            if target_brand != target_tokens:
                table.append(("name_brand_alias", [rt for rt in rooftops if any(p._brand_canonical_tokens(n) == target_brand for n in rt.names)]))
                table.append(("name_brand_alias_subset", [rt for rt in rooftops if any(target_brand < p._brand_canonical_tokens(n) for n in rt.names)]))
        table.append(("name_contains", [
            rt for rt in rooftops
            if any(p._nrm(n) and (p._nrm(n) in target_nrm or target_nrm in p._nrm(n)) for n in rt.names)
        ]))

    for alias in aliases:
        alias_nrm = p._nrm(alias)
        if alias_nrm:
            table.append(("roster_name_alias", [rt for rt in rooftops if any(p._nrm(n) == alias_nrm for n in rt.names)]))

    if target_nrm and any(rt.alt_names for rt in rooftops):
        for probe_fn, tier_name in (
            (lambda n: p._nrm(n) == target_nrm, "store_label_exact"),
            (lambda n: bool(target_tokens) and p._tokens(n) == target_tokens, "store_label_tokens"),
            (lambda n: bool(target_tokens) and target_tokens < p._tokens(n), "store_label_token_subset"),
        ):
            table.append((tier_name, [rt for rt in rooftops if any(probe_fn(n) for n in rt.alt_names)]))
        for alias in aliases:
            alias_nrm = p._nrm(alias)
            if alias_nrm:
                table.append(("store_label_alias", [rt for rt in rooftops if any(p._nrm(n) == alias_nrm for n in rt.alt_names)]))

    id_nrm = p._nrm(re.sub(r"-(com|net|org)$", "", dealer_id))
    if id_nrm:
        table.append(("dealer_id_host", [
            rt for rt in rooftops
            if any(p._nrm(n) and (id_nrm == p._nrm(n) or id_nrm in p._nrm(n)) for n in rt.names)
        ]))

    if any(rt.slugs for rt in rooftops):
        host_stem = p._nrm(target_host.split(".")[0]) if target_host else ""
        for probe in (host_stem, id_nrm):
            if len(probe) < 8:
                continue
            table.append(("rooftop_slug_host", [rt for rt in rooftops if any(probe in s for s in rt.slugs)]))
        name_parts = [t for t in target_tokens if len(t) >= 4]
        if name_parts:
            table.append(("rooftop_slug_name_tokens", [
                rt for rt in rooftops if any(all(part in s for part in name_parts) for s in rt.slugs)
            ]))

    street, street_key, locale, postal = place
    if street:
        table.append(("street_address", [rt for rt in rooftops if street in rt.streets]))
        table.append(("street_address_partial", [rt for rt in rooftops if any(s and (s in street or street in s) for s in rt.streets)]))
    if street_key:
        table.append(("street_address_suffix", [rt for rt in rooftops if street_key in rt.street_keys]))
        table.append(("street_address_suffix_partial", [
            rt for rt in rooftops if any(s and (s in street_key or street_key in s) for s in rt.street_keys)
        ]))
    if postal:
        table.append(("zip_code", [rt for rt in rooftops if postal in rt.zips]))
    if locale:
        table.append(("city_state", [rt for rt in rooftops if locale in rt.locales]))
    return table


def pick_target(table: list[tuple[str, list]]) -> tuple[Any, str]:
    """First tier with a unique winner, else ``(None, "target_rooftop_unidentified")``."""
    for tier, matches in table:
        if len(matches) == 1:
            return matches[0], tier
    return None, REJECT_UNIDENTIFIED


def _rooftop_signals(rt, table: list[tuple[str, list]]) -> list[dict[str, Any]]:
    out = []
    seen: set[str] = set()
    for tier, matches in table:
        if rt in matches and tier not in seen:
            seen.add(tier)
            out.append(_signal(tier, unique=(len(matches) == 1)))
    return out


# ── the decision ──────────────────────────────────────────────────────────────

def _place_tuple(roster: dict[str, Any]) -> tuple[str, str, str, str]:
    p = _parsers()
    city, state = roster.get("city"), roster.get("state")
    return (
        p._nrm(roster.get("street")),
        p._street_key(roster.get("street")),
        (p._nrm(city) + p._nrm(state)) if city and state else "",
        re.sub(r"\D", "", str(roster.get("zip") or ""))[:5],
    )


def _evidence_extras(row: dict, rooftop_sources: set[str], evidence: dict[str, Any]) -> list[dict[str, Any]]:
    """Signals only an evidence-carrying caller can produce (synthesis facts)."""
    out: list[dict[str, Any]] = []
    src = str(row.get("_feed_source") or "").strip()
    identity = {str(s) for s in (evidence.get("identity_sources") or []) if s}
    oem = str(evidence.get("page_oem_code") or "").strip()
    if src and src in identity:
        out.append(_signal("identity_feed", src))
    elif rooftop_sources and rooftop_sources <= identity:
        out.append(_signal("identity_feed", ",".join(sorted(rooftop_sources))))
    if src and oem and src == oem:
        out.append(_signal("page_oem_code", src))
    facet = evidence.get("location_facet") or {}
    key, value = str(facet.get("key") or ""), str(facet.get("value") or "")
    if key and value:
        extra = row.get("_custom_facets") if isinstance(row.get("_custom_facets"), dict) else {}
        if extra and str(extra.get(key) or "").strip().lower() == value.strip().lower():
            out.append(_signal("location_facet_value", f"{key}={value}"))
    return out


def score_rows(rows: list[dict], roster_store: dict[str, Any], evidence: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Score every row of one payload. Returns a list parallel to ``rows``.

    ``roster_store``: ``dealer_id``, ``name``, ``url``, ``street``, ``city``, ``state``,
    ``zip``, ``street_source`` (V003 provenance), optional ``aliases`` (else the roster
    alias table + learned hints are read).

    ``evidence`` (all optional): ``trust_feed_scope`` (the recipe is filtered to this
    store's own feed ids: keep all, ``feed_scoped``), ``identity_sources``,
    ``page_oem_code``, ``jsonld_street`` (used when the roster has no street),
    ``location_facet`` ``{"key": "custom_text_9", "value": "Grand Hyundai of Canton"}``.
    Without evidence the result equals the legacy gate's row for row.
    """
    p = _parsers()
    evidence = dict(evidence or {})
    if not rows:
        return []
    if evidence.get("trust_feed_scope"):
        return [_result("keep", "feed_scoped", [_signal("feed_scoped")], strong=True) for _ in rows]

    roster = dict(roster_store)
    if not roster.get("street") and evidence.get("jsonld_street"):
        roster["street"] = evidence["jsonld_street"]
        roster["street_source"] = roster.get("street_source") or "site_jsonld"
    aliases = roster.get("aliases")
    if aliases is None:
        aliases = p.roster_name_aliases(str(roster.get("dealer_id") or ""))
    aliases = tuple(str(a) for a in aliases if a)

    order: list[str] = []
    groups: dict[str, Any] = {}
    unmarked: list[int] = []
    row_group: dict[int, str] = {}
    for i, row in enumerate(rows):
        rooftop = p._rooftop_of(row)
        identity = p._rooftop_identity(rooftop) if rooftop else ""
        if not identity:
            unmarked.append(i)
            continue
        if identity not in groups:
            groups[identity] = p._Rooftop(identity)
            order.append(identity)
        groups[identity].add(rooftop, row)
        row_group[i] = identity

    results: list[dict[str, Any] | None] = [None] * len(rows)

    if not groups:
        for i, row in enumerate(rows):
            extras = _evidence_extras(row, set(), evidence)
            results[i] = _result("keep", "no_rooftop_evidence", extras, strong=False)
        return [r for r in results if r is not None]

    rooftops = [groups[k] for k in order]
    place = _place_tuple(roster)
    table = tier_table(rooftops, roster, place, aliases)
    target, tier = pick_target(table)

    def signals_for(i: int) -> list[dict[str, Any]]:
        rt = groups.get(row_group.get(i, ""), None)
        sig = _rooftop_signals(rt, table) if rt is not None else []
        return sig + _evidence_extras(rows[i], set(rt.sources) if rt is not None else set(), evidence)

    def evidence_keep(i: int) -> str | None:
        """An evidence-only signal that keeps a row the ladder refused."""
        for s in signals_for(i):
            if s["signal"] in ("identity_feed", "page_oem_code", "location_facet_value"):
                return s["signal"]
        return None

    if len(groups) == 1:
        if target is not None or not rooftops[0].names:
            label = tier if target is not None else "single_rooftop"
            for i in range(len(rows)):
                results[i] = _result("keep", label, signals_for(i), strong=(target is not None and _strong_tier(tier, roster)))
            return [r for r in results if r is not None]
        for i in range(len(rows)):
            ek = evidence_keep(i)
            results[i] = _result("keep", ek, signals_for(i), strong=True) if ek else _result("refuse", REJECT_SINGLE, signals_for(i), strong=False)
        return [r for r in results if r is not None]

    if target is None:
        for i in range(len(rows)):
            ek = evidence_keep(i)
            results[i] = _result("keep", ek, signals_for(i), strong=True) if ek else _result("refuse", tier, signals_for(i), strong=False)
        return [r for r in results if r is not None]

    strong_tier = _strong_tier(tier, roster)
    weak_street_provenance = (
        tier in p._STREET_TIERS and str(roster.get("street_source") or "").strip().lower() in p._WEAK_STREET_SOURCES
    )
    sibling_reject = REJECT_SIBLING if (tier not in p._LOCALITY_ONLY_TIERS and not weak_street_provenance) else REJECT_SIBLING_WEAK
    own_street_key = place[1]
    shared_sources = {
        src for rt in rooftops
        if rt is not target and (rt.names or (rt.street_keys and not (own_street_key and rt.street_keys <= {own_street_key})))
        for src in rt.sources
    }
    own_sources = (target.sources - shared_sources) if tier not in p._LOCALITY_ONLY_TIERS else set()

    for i in range(len(rows)):
        if i in unmarked:
            continue
        rt = groups[row_group[i]]
        if rt is target:
            results[i] = _result("keep", tier, signals_for(i), strong=strong_tier)
        elif not rt.names and rt.sources and own_sources and rt.sources <= own_sources:
            results[i] = _result("keep", "same_source_feed", signals_for(i) + [_signal("same_source_feed", ",".join(sorted(rt.sources)))], strong=True)
        elif not rt.names and own_street_key and rt.street_keys and rt.street_keys <= {own_street_key}:
            results[i] = _result("keep", "own_street_block", signals_for(i) + [_signal("own_street_block", own_street_key)], strong=True)
        else:
            ek = evidence_keep(i)
            results[i] = _result("keep", ek, signals_for(i), strong=True) if ek else _result("refuse", sibling_reject, signals_for(i), strong=False)
    for i in unmarked:
        src = str(rows[i].get("_feed_source") or "").strip()
        if src and src in own_sources:
            results[i] = _result("keep", "unstamped_under_own_source", [_signal("unstamped_under_own_source", src)] + _evidence_extras(rows[i], set(), evidence), strong=True)
            continue
        ek = evidence_keep(i)
        results[i] = _result("keep", ek, signals_for(i), strong=True) if ek else _result("refuse", REJECT_UNSTAMPED, signals_for(i), strong=False)
    return [r for r in results if r is not None]


def _strong_tier(tier: str, roster: dict[str, Any]) -> bool:
    p = _parsers()
    if tier in p._LOCALITY_ONLY_TIERS:
        return False
    if tier in p._STREET_TIERS and str(roster.get("street_source") or "").strip().lower() in p._WEAK_STREET_SOURCES:
        return False
    return SIGNAL_WEIGHTS.get(tier, (0.0, False))[1]


def score_row(row: dict, roster_store: dict[str, Any], evidence: dict[str, Any] | None = None) -> dict[str, Any]:
    """Score ONE row: ``{decision, tier, score, strong, signals}``.

    A row's verdict depends on the rest of its payload (unique winners, shared feed
    sources), so pass the page's rows as ``evidence["payload_rows"]``; without them the
    row is judged as a payload of one.
    """
    evidence = dict(evidence or {})
    payload = evidence.pop("payload_rows", None) or [row]
    if not any(r is row for r in payload):
        payload = list(payload) + [row]
    results = score_rows(payload, roster_store, evidence)
    for r, res in zip(payload, results):
        if r is row:
            return res
    return results[-1]


__all__ = ["FLAG_ENV", "SIGNAL_WEIGHTS", "STRONG_THRESHOLD", "NEVER_SUFFICIENT_ALONE", "scorer_enabled",
           "tier_table", "pick_target", "score_rows", "score_row"]
