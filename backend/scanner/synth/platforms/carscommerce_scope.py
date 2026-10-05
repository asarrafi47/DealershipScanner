"""
CarsCommerce group-account store scoping: facet census, rooftop signatures,
replay-verified store filter (``_carscommerce_store_filter``).
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

from backend.scanner.recipes import _replay_request, EndpointRecipe
from backend.scanner.synth.common import _origin

logger = logging.getLogger("scanner")


# Group accounts (Hendrick: one ccid, 10,569 cars, 15 rooftops) tag each store's own
# cars in a custom_text facet — custom_text_11 = "Buford, GA" on Mall of Georgia
# Mazda (309 of 10,569); custom_text_4 / custom_text_2 hold every rooftop's city.
# The browser SRP sends that as facetFilters; over HTTP we learn it from the facet
# values on page 1 and the store's own place, then filter server-side instead of
# walking 106 pages and refusing 97% of the rows.
# custom_text_N semantics differ per ACCOUNT: on 2172862 (Mall of Georgia Mazda)
# custom_text_11 is the single store tag "Buford, GA"; on 5363312 (Hendrick's
# 11,651-car group) custom_text_2 is "City, ST" per rooftop, custom_text_3 the city,
# custom_text_11 an unrelated 1/2/3 code and custom_text_4 the certification
# program. So no facet name can be trusted by itself: every candidate value that
# spells this store's place is REPLAYED, and it is chosen only when every returned
# listing's own rooftop stamp (``dealer.location``, the field the attribution gate
# reads) is this store. Measured 2026-09-24: an unverified custom_text_2="Cary"
# kept 2 of Hendrick Buick GMC Cary's 390 cars.
_CC_CUSTOM_FACETS = tuple(f"custom_text_{i}" for i in range(1, 21))
# The Dealer Inspire page embeds the account's field map, e.g.
#   "Location":"custom_text_4","buford_location":"custom_text_11"   (Mall of Georgia Mazda)
#   "Location":"custom_text_2"                                      (Rick Hendrick Chevy Naples)
#   "Location":"custom_text_25","meta_location":"custom_text_11"    (Hendrick Buick GMC Cary)
# so the store facet is read from the site itself, not guessed; custom_text_25 is
# outside any blind 1..20 census.
_CC_LOCATION_MAP_RE = re.compile(r'"([A-Za-z_.]*[Ll]ocation[A-Za-z_]*)"\s*:\s*"(custom_text_\d+)"')


def _cc_location_facets(html: str) -> list[tuple[str, str]]:
    """[(field_name, facet)] the site maps to a location, exact "Location" first."""
    seen: dict[str, str] = {}
    for m in _CC_LOCATION_MAP_RE.finditer(html or ""):
        seen.setdefault(m.group(2), m.group(1))
    ranked = sorted(seen.items(), key=lambda kv: (kv[1] != "Location", "meta" in kv[1].lower(), kv[0]))
    return [(name, facet) for facet, name in ranked]
_CC_PLACE_ONLY_RE = re.compile(r"[A-Za-z .'\-]{2,40},\s*[A-Za-z]{2}(?:\s+\d{5})?")  # "Buford, GA" is a place, not a store name
_CC_SINGLE_STORE_MAX_UNSTAMPED = 1500  # every feed stampless: Knight Claremont CDJR, 1,045 cars on one site (2026-09-26)
_CC_SINGLE_STORE_MAX = 1000  # an account larger than this is a group even when its stamps say nothing
_CC_FACET_CHUNK = 7  # the API 400s on unknown facet names; small chunks keep one bad name from blanking the census


def _cc_facet_census(recipe: EndpointRecipe, body: dict, origin: str, facets: tuple[str, ...] = _CC_CUSTOM_FACETS) -> tuple[int, dict[str, list[tuple[str, int]]]]:
    total = 0
    values: dict[str, list[tuple[str, int]]] = {}
    for i in range(0, len(facets), _CC_FACET_CHUNK):
        probe = dict(body)
        probe.pop("facetFilters", None)
        probe.update({"page": 1, "perPage": 1, "facets": list(facets[i:i + _CC_FACET_CHUNK])})
        status, parsed = _replay_request(recipe, probe, origin)
        if status != 200 or not isinstance(parsed, dict):
            logger.info("carscommerce facet census chunk %d: status %s", i // _CC_FACET_CHUNK, status)
            continue
        data = parsed.get("data") or {}
        total = total or int(data.get("total_vehicle_count") or 0)
        for f in data.get("facets") if isinstance(data.get("facets"), list) else []:
            if not isinstance(f, dict):
                continue
            name = f.get("name") or f.get("field") or f.get("key")
            vals = f.get("values") if isinstance(f.get("values"), list) else None
            if name and vals:
                values[str(name)] = [(str(v.get("key")), int(v.get("doc_count") or 0)) for v in vals if isinstance(v, dict) and v.get("key") is not None]
    return total, values


def _cc_rooftop_sig(rt: dict) -> tuple[str, str]:
    """What identifies this rooftop stamp: ("street", <street key + city>) when
    it carries a street, ("name", <store name>) when it names a store, else
    ("place", <city state>). The phone line is left out on purpose: Stevenson
    Hendrick Honda's two feeds spell the same store's phone "396-1116" and
    "395-1116" (2026-09-24)."""
    from backend.parsers import _nrm, _street_key

    if rt.get("address") and re.match(r"\d", str(rt["address"]).strip()):
        return ("street", _street_key(rt["address"]) + "|" + _nrm(rt.get("city")) + _nrm(rt.get("state")))
    if rt.get("address"):
        # rooftop_of files a name-only label ("loaner", "none") under address too
        return ("tag", _nrm(rt["address"]))
    name = str(rt.get("alt_name") or rt.get("name") or "").strip()
    if name and not _CC_PLACE_ONLY_RE.fullmatch(name) and "<" not in name:
        return ("name", _nrm(name))
    return ("place", _nrm(rt.get("city")) + _nrm(rt.get("state")))


def _cc_rooftop_place_of(rt: dict) -> str:
    if rt.get("city") and rt.get("state"):
        return f"{rt['city']}, {rt['state']}".lower()
    return str(rt.get("name") or "").lower()


def _cc_rooftop_place(listing: dict) -> str:
    """"city, st" from the listing's own rooftop stamp (what the gate matches on)."""
    from backend.parsers.carscommerce import rooftop_of

    rt = rooftop_of(listing) or {}
    if rt.get("city") and rt.get("state"):
        return f"{rt['city']}, {rt['state']}".lower()
    return str(rt.get("name") or "").lower()


def _cc_verify_store_filter(recipe: EndpointRecipe, body: dict, origin: str, facet: str, key: str, label: str,
                            *, identity: bool = False, site_name: str = "") -> dict[str, Any]:
    """Replay page 1 under the candidate filter and describe what came back.

    ``ok`` means: rows came back, every one is stamped in this store's city and
    they all carry ONE rooftop identity. Same city is not enough: Mall of Georgia
    Mazda's account tags Mazda, MINI and a Hendrick store all "Buford, GA" under
    its Location facet (738 cars) while ``buford_location`` tags the Mazda store
    alone (309); Hendrick's "Cary, NC" Location value covers three Cary stores
    (713) while ``source_id`` 178465 — the page's own ``oem_code`` — is the Buick
    GMC store's feed (439). 2026-09-24."""
    from backend.parsers.carscommerce import rooftop_of

    probe = dict(body)
    probe.pop("facets", None)
    probe.update({"page": 1, "perPage": 60, "facetFilters": {facet: [key]}})
    status, parsed = _replay_request(recipe, probe, origin)
    if status != 200 or not isinstance(parsed, dict):
        return {"ok": False, "rows": 0, "places": [f"status {status}"], "rooftops": [], "names": []}
    listings = [x for x in ((parsed.get("data") or {}).get("listings") or []) if isinstance(x, dict)]
    stamps = [rooftop_of(x) or {} for x in listings]
    places = sorted({_cc_rooftop_place(x) for x in listings})
    rooftops = sorted({str(rt.get("key") or "") for rt in stamps})
    sigs = sorted({_cc_rooftop_sig(rt) for rt in stamps if rt})
    names = sorted({str(rt.get("alt_name") or rt.get("name") or "") for rt in stamps
                    if (rt.get("alt_name") or rt.get("name")) and rt.get("city")
                    and not _CC_PLACE_ONLY_RE.fullmatch(str(rt.get("alt_name") or rt.get("name")).strip())
                    and "<" not in str(rt.get("alt_name") or rt.get("name"))})
    # An identity feed (the page's own OEM code / store name) whose single
    # rooftop carries NO locale is still this store: Tutton CDJR's feed 27250
    # stamps only the store name, Group 1 Toyota North Austin's 42409 only a
    # name with no city (2026-09-26). A Location-facet candidate never gets
    # that benefit — a bare tag could be any store.
    placeless = bool(listings) and all(not (rt.get("city") and rt.get("state")) for rt in stamps)
    # rows the feed does not stamp at all (Group 1 Toyota North Austin 42409, Lenoir
    # City CDJR 45544): nothing contradicts the identity, and nothing to group by
    stampless = bool(listings) and not any(stamps)
    # For an identity-backed filter the stamps only VETO when one of them says
    # another place or another store: lot tags ("ALL", "SPC", "TOW/JORGE R/…")
    # and the store's own name are not contradictions.
    from backend.parsers import _looks_like_address as _addr_like
    from backend.parsers import _looks_like_store_name as _store_like
    from backend.parsers import _nrm as _pnrm

    def _contradicts(rt: dict) -> bool:
        if rt.get("city") and rt.get("state") and _cc_rooftop_place_of(rt) != label.lower():
            return True
        nm = str(rt.get("alt_name") or rt.get("name") or "").strip()
        # rooftop_of files a name-only label under "address" too; a label that
        # reads as a store name (and not as a street) is a store name
        junk = "/" in nm or sum(ch.isdigit() for ch in nm) > len(nm) * 0.3  # "TOW/JORGE R/367676": a lot note, not a store
        if nm and not junk and _store_like(nm) and not _CC_PLACE_ONLY_RE.fullmatch(nm) and "<" not in nm and not _addr_like(nm):
            own = _pnrm(site_name)
            return not (own and (_pnrm(nm) == own or own.startswith(_pnrm(nm)) or _pnrm(nm).startswith(own)))
        return False

    no_contradiction = bool(listings) and not any(_contradicts(rt) for rt in stamps if rt)
    consistent = identity and no_contradiction
    ok = bool(listings) and ((len(sigs) == 1 and (places == [label.lower()] or (identity and placeless))) or (identity and stampless) or consistent)
    return {"ok": ok, "rows": len(listings), "places": places, "rooftops": rooftops, "sigs": sigs, "names": names, "no_contradiction": no_contradiction,
            "total": int((parsed.get("data") or {}).get("total_vehicle_count") or 0)}


def _nrm_facet(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _carscommerce_store_filter(recipe: EndpointRecipe, dealer_id: str, dealer_url: str, html: str) -> list[EndpointRecipe]:
    """Scope *recipe* to this store in place; returns EXTRA recipes to replay
    alongside it (a verified store-name Location facet that covers cars the
    identity feeds do not: Group 1 Toyota North Austin's 42409 holds 259 new
    cars, its used cars sit in group pools reachable only through
    custom_text_13="Group 1 Toyota North Austin", 2026-09-26)."""
    from backend.parsers.rooftop_aliases import record_rooftop_alias, roster_name_aliases
    from backend.scanner.dealer_place import learn_place, name_from_html, oem_code_from_html, place_label

    body = json.loads(recipe.post_template or "{}")
    origin = _origin(dealer_url)
    mapped = _cc_location_facets(html)
    facets = tuple(dict.fromkeys(["source_id"] + [f for _n, f in mapped] + list(_CC_CUSTOM_FACETS)))
    total, values = _cc_facet_census(recipe, body, origin, facets)
    if not values:
        return []
    oem_code = oem_code_from_html(html)
    site_name = name_from_html(html)
    if mapped or oem_code:
        logger.info("carscommerce [%s]: site says dealername=%r oem_code=%r; location facets %s", dealer_id, site_name, oem_code,
                    ", ".join(f"{n}={f}" for n, f in mapped) or "none")
    loc_values = [values.get(f) or [] for _n, f in mapped if (values.get(f) or [])]
    mapped_values_present = bool(loc_values)
    # A Location facet whose single value names THIS store on (nearly) every car
    # settles the account: Group 1 Ford of South Austin's meta_location facet
    # reads "Group 1 Ford of South Austin" on 748 of 748 cars while the sort
    # facet (custom_text_4: 1 / 2) and a partial Location (252) kept the
    # all-single-valued rule from firing; the OEM feed then kept 74 (2026-09-26).
    for (_n, facet), v in zip([m for m in mapped if values.get(m[1])], loc_values):
        if len(v) == 1 and total and v[0][1] >= 0.95 * total and site_name and _nrm_facet(v[0][0]) == _nrm_facet(site_name):
            # Filter on that facet anyway: it returns the whole account and marks
            # the recipe store-scoped (recipe_is_store_scoped), so the gate trusts
            # the rows instead of refusing the lot-code stamps ("MAIN", "SPC")
            # the feed writes into dealer.location (Group 1 Ford: 69 of 748 kept).
            body["facetFilters"] = {facet: [v[0][0]]}
            recipe.post_template = json.dumps(body)
            logger.info("carscommerce [%s]: single-store account (%d cars): location facet %s=%r names this store on %d cars; scoped on it",
                        dealer_id, total, facet, v[0][0], v[0][1])
            return []
    if mapped and loc_values and all(len(v) == 1 for v in loc_values):
        # Napleton Honda of Morton Grove (393 cars, feeds 207385 + MP21386): the
        # site's Location facets each hold ONE value, so the whole account is this
        # store; scoping to the OEM-code feed alone kept 49 of 393 (2026-09-26).
        logger.info("carscommerce [%s]: single-store account (%d cars): location facets %s hold one value each; no store filter",
                    dealer_id, total, ", ".join(f"{f}={v[0][0]!r}" for (_n, f), v in zip([m for m in mapped if values.get(m[1])], loc_values)))
        return []
    if mapped:
        logger.info("carscommerce [%s]: location facet values: %s", dealer_id,
                    "; ".join(f"{f}: " + ", ".join(f"{k}={n}" for k, n in (values.get(f) or [])[:6]) for _n, f in mapped))
    place = learn_place(dealer_id, dealer_url, html)
    label = place_label(place)
    if not label:
        logger.info("carscommerce [%s]: group account (%d cars) but no place evidence for this store; no store filter", dealer_id, total)
        return []
    city = label.split(",")[0].strip().lower()
    # Identity-backed feed ids: a source_id that IS the page's OEM dealer code
    # (178465 = Hendrick Buick GMC Cary's GM BAC) or spells the store's own name
    # ("MallofGeorgiaMazda" = "Mall of Georgia Mazda"). A store files under
    # several (dealer code + marketplace feeds: Mall of Georgia Mazda = 23978 with
    # 115 cars + MallofGeorgiaMazda with 194), so EVERY verified one is taken.
    def _nrm(s: str) -> str:
        return re.sub(r"[^a-z0-9]", "", str(s or "").lower())

    id_stem = _nrm(re.sub(r"-(com|net|org)$", "", dealer_id))
    identity = {x for x in (oem_code.lower() if oem_code else "", _nrm(site_name), id_stem) if x}

    def _is_identity(k: str) -> bool:
        nk = _nrm(k)
        if re.search(r"staging|test|sandbox|demo", k, re.I):
            return False  # "60503-staging" on Lexus of Greenwood Village: a staging feed, 62 of 484 cars
        if k in identity or nk in identity:
            return True
        # a named feed id is the store name without its town: "TuttonChryslerDodgeJeepRam"
        # (341 cars) for "Tutton Chrysler Dodge Jeep RAM of Jasper" (2026-09-26)
        site = _nrm(site_name)
        return len(nk) >= 10 and not nk.isdigit() and (site.startswith(nk) or (len(id_stem) >= 8 and nk.startswith(id_stem)))

    id_feeds: list[tuple[int, str]] = []
    name_facets: list[tuple[int, str, str]] = []
    mapped_rank = {f: i for i, (_n, f) in enumerate(mapped)}
    candidates: list[tuple[int, int, str, str]] = []
    for facet, vals in values.items():
        for key, n in vals:
            k = key.strip().lower()
            if n <= 0 or (total and n >= total):
                continue
            if facet == "source_id":
                if _is_identity(key):
                    id_feeds.append((n, key))
            elif k == label.lower() or k == city:
                candidates.append((1 + mapped_rank.get(facet, len(mapped_rank)), -n, facet, key))
            elif facet in mapped_rank and site_name and (_nrm(k) == _nrm(site_name) or (len(id_stem) >= 8 and _nrm(k) == id_stem)):
                # the site's own Location facet spelled as the store name
                # ("Group 1 Toyota North Austin" under custom_text_13, 2026-09-26)
                name_facets.append((n, facet, key))
    candidates.sort()
    tried: list[str] = []
    keys: list[str] = []
    verdicts: list[dict[str, Any]] = []
    for n, key in sorted(id_feeds, reverse=True):
        v = _cc_verify_store_filter(recipe, body, origin, "source_id", key, label, identity=True)
        tried.append(f"source_id={key!r} ({n} cars, identity): " + ("one rooftop, store-only" if v["ok"] else f"{len(v['rooftops'])} rooftops, places {v['places'][:4]}, stamps {[r[:50] for r in v['rooftops'][:3]]}"))
        if v["ok"]:
            keys.append(key)
            verdicts.append(v)
    if keys:
        # The OEM-code feed is often the SMALL one: Stevenson Hendrick Honda's
        # 208763 holds 45 cars while 9048741 — same "6720 Market St" rooftop —
        # holds 402 (2026-09-24). Every other feed id that returns exactly the
        # identity feeds' rooftop is that store's too. Only a rooftop that names
        # a store or a street qualifies; a bare "City, ST" stamp would merge a
        # same-town sibling.
        # An identity feed that stamps only "City, ST" (Stevenson Hendrick Mazda's
        # 24009) cannot vouch for a sibling feed by itself; the store's own street
        # (registry, or the page's JSON-LD streetAddress) can: the other feed's
        # single rooftop must sit at that street.
        from backend.parsers import _nrm as _pnrm
        from backend.parsers import _street_key

        own_sigs = {sg for v in verdicts for sg in v.get("sigs") or []}
        own_street = (_street_key(place.get("dealer_address")) + "|" + _pnrm(place.get("dealer_city")) + _pnrm(place.get("dealer_state"))) if place.get("dealer_address") else ""
        acceptable = {sg for sg in own_sigs if sg[0] != "place"}
        if own_street:
            acceptable.add(("street", own_street))
        others_consistent = True
        others_seen = 0
        other_verdicts: list[dict[str, Any]] = []
        if not acceptable:
            tried.append("no street or store name known for this store; sibling feed ids not merged")
        for n, key in sorted(((n, k) for k, n in values.get("source_id") or [] if k not in keys and n > 0 and not (total and n >= total)), reverse=True):
            v = _cc_verify_store_filter(recipe, body, origin, "source_id", key, label, site_name=site_name)
            others_seen += 1
            other_verdicts.append(v)
            # a feed stamped with a STREET is only "this store" when it is our street
            # (Stevenson Hendrick Honda's siblings sit at other Wilmington streets)
            others_consistent = others_consistent and bool(v.get("no_contradiction")) and all(
                sg[0] != "street" or sg in acceptable for sg in (v.get("sigs") or []))
            same = bool(acceptable) and v["ok"] and bool(v.get("sigs")) and set(v["sigs"]) <= acceptable
            tried.append(f"source_id={key!r} ({n} cars): " + ("same rooftop, merged" if same else f"{len(v['rooftops'])} rooftops, places {v['places'][:3]}, not this store"))
            if same:
                keys.append(key)
                verdicts.append(v)
        bare_place_only = bool(own_sigs) and all(sg[0] == "place" for sg in own_sigs)  # "Naples, FL": a same-town sibling would look identical
        # A group account stamps something somewhere (Hendrick's street blocks);
        # an account where no feed stamps any rooftop at all is one store even a
        # little above the cap: Knight Claremont CDJR's 27302 (78 new) + MP15994
        # (967, no stamps) = 1,045 cars, all on the site's own SRP (671 new + 374
        # used), and the OEM-code filter kept 78 (2026-09-26).
        all_unstamped = not own_sigs and all(not [r for r in (v.get("rooftops") or []) if r] for v in other_verdicts)  # unstamped rows report one "" key
        cap = _CC_SINGLE_STORE_MAX_UNSTAMPED if all_unstamped else _CC_SINGLE_STORE_MAX
        if others_seen and others_consistent and total <= cap and not mapped_values_present and not bare_place_only:
            # Napleton Honda of Morton Grove: OEM feed 207385 (49) + marketplace
            # feed MP21386 (344, no stamps at all), Location facets empty, 393
            # cars in the account. No feed contradicts the store and the account
            # is store-sized: it IS the store. Scoping would keep 49 of 393.
            logger.info("carscommerce [%s]: single-store account by feed consistency (%d cars <= %d, %d other feed(s) with no contradicting stamp%s); no store filter",
                        dealer_id, total, cap, others_seen, ", none stamped" if all_unstamped else "")
            return []
    if not keys and place.get("dealer_address"):
        # No feed id carries the store's identity (Greenway CDJR of Rome: page
        # oem_code 45584, feeds 50377 / 45549 / a sibling's slug). A feed whose
        # rows all sit at the store's own STREET is the store's: the street is
        # the strongest locale evidence the gate itself accepts, and a same-town
        # sibling cannot share it. City alone is never enough here.
        from backend.parsers import _nrm as _pnrm
        from backend.parsers import _street_key

        own_street = _street_key(place.get("dealer_address")) + "|" + _pnrm(place.get("dealer_city")) + _pnrm(place.get("dealer_state"))
        for n, key in sorted(((n, k) for k, n in values.get("source_id") or [] if n > 0 and not (total and n >= total)), reverse=True)[:8]:
            v = _cc_verify_store_filter(recipe, body, origin, "source_id", key, label)
            at_street = v["ok"] and v.get("sigs") == [("street", own_street)]
            tried.append(f"source_id={key!r} ({n} cars, street check): " + ("at this store's street" if at_street else f"stamps {[r[:40] for r in v['rooftops'][:2]]}"))
            if at_street:
                keys.append(key)
                verdicts.append(v)
    # the facet the site itself calls "Location" outranks size: meta_location on
    # Group 1 Toyota North Austin tags 1,000 of the group's 1,787 cars with the
    # store's name while custom_text_13 (Location) is the store's own set
    name_facets.sort(key=lambda c: (mapped_rank.get(c[1], len(mapped_rank)), -c[0]))
    if not keys:
        # By elimination (Benson's Ingram Park Nissan, 2026-09-26): no feed carries
        # the page's OEM code, but every feed except ONE is stamped with another
        # store's name ("Ingram Park Chrysler Jeep Dodge", "Ingram Park Mazda",
        # "IPAC Pre-Owned Outlet") and that one carries no stamp at all. On a
        # store-sized account the unstamped feed is this store's.
        feeds = [(n, k) for k, n in values.get("source_id") or [] if n > 0 and not (total and n >= total)]
        if 2 <= len(feeds) <= 8 and total <= _CC_SINGLE_STORE_MAX * 2:
            clean: list[tuple[int, str, dict[str, Any]]] = []
            named_other = 0
            for n, key in feeds:
                v = _cc_verify_store_filter(recipe, body, origin, "source_id", key, label, identity=True, site_name=site_name)
                if v.get("no_contradiction") and v["rows"]:
                    clean.append((n, key, v))
                else:
                    named_other += 1
            if len(clean) == 1 and named_other == len(feeds) - 1:
                n, key, v = clean[0]
                keys.append(key)
                verdicts.append(v)
                tried.append(f"source_id={key!r} ({n} cars): the only feed not stamped with another store — this store's by elimination")
    chosen: tuple[str, list[str]] | None = ("source_id", keys) if keys else None
    if not chosen:
        for n, facet, key in name_facets:
            v = _cc_verify_store_filter(recipe, body, origin, facet, key, label, identity=True, site_name=site_name)
            tried.append(f"{facet}={key!r} ({n} cars, store-name facet): " + ("accepted" if v["ok"] else f"{len(v['rooftops'])} rooftops, places {v['places'][:4]}, stamps {[r[:40] for r in v['rooftops'][:3]]}"))
            if v["ok"]:
                chosen, verdicts = (facet, [key]), [v]
                break
    if not chosen:
        for _rank, negn, facet, key in candidates[:8]:
            v = _cc_verify_store_filter(recipe, body, origin, facet, key, label)
            tried.append(f"{facet}={key!r} ({-negn} cars): " + ("one rooftop, store-only" if v["ok"] else f"{len(v['rooftops'])} rooftops, places {v['places'][:4]}"))
            if v["ok"]:
                chosen, verdicts = (facet, [key]), [v]
                break
    if not chosen:
        census = ", ".join(f"{k}={n}" for k, n in sorted(values.get("source_id") or [], key=lambda kv: -kv[1])[:12])
        logger.info("carscommerce [%s]: group account (%d cars); no verified store filter for %r (tried: %s); source_id census: %s; gate filters per row",
                    dealer_id, total, label, "; ".join(tried) or "no facet value spells this place", census or "none")
        return []
    if chosen[0] == "source_id":
        # the feed's own spelling of this store, for the attribution gate
        for v in verdicts:
            for nm in v.get("names") or []:
                if nm and nm not in roster_name_aliases(dealer_id):
                    record_rooftop_alias(dealer_id, nm, evidence=f"source_id {chosen[1]} is this store's own feed id (page oem_code {oem_code!r} / name {site_name!r}); {v['rows']} rows one rooftop in {label}",
                                         observed=datetime.now(timezone.utc).date().isoformat())
                    logger.info("carscommerce [%s]: feed names this store %r (recorded as roster alias)", dealer_id, nm)
    extras: list[EndpointRecipe] = []
    if chosen[0] == "source_id" and name_facets:
        # the facet may cover a DIFFERENT subset than the feed ids (the used pool
        # vs the new feed), so its size says nothing; VIN dedupe absorbs overlap
        for n, facet, key in name_facets:
            v = _cc_verify_store_filter(recipe, body, origin, facet, key, label, identity=True, site_name=site_name)
            tried.append(f"{facet}={key!r} ({n} cars, store-name facet, extra): " + ("accepted" if v["ok"] else f"stamps {[r[:40] for r in v['rooftops'][:3]]}"))
            if v["ok"]:
                extra_body = dict(body)
                extra_body["facetFilters"] = {facet: [key]}
                extras.append(EndpointRecipe(
                    dealer_id=recipe.dealer_id, url=recipe.url, method=recipe.method, content_type=recipe.content_type,
                    post_template=json.dumps(extra_body), auth_headers=dict(recipe.auth_headers), pagination=recipe.pagination,
                    provider_hint=recipe.provider_hint, vehicle_rows=v["rows"], total_count=int(v.get("total") or 0),
                ))
                break
    body["facetFilters"] = {chosen[0]: chosen[1]}
    recipe.post_template = json.dumps(body)
    logger.info("carscommerce [%s]: group account (%d cars); store filter %s=%r verified on %d rows, %s cars%s (tried: %s)",
                dealer_id, total, chosen[0], chosen[1], sum(v["rows"] for v in verdicts), sum(int(v.get("total") or 0) for v in verdicts),
                f"; extra recipe {json.loads(extras[0].post_template)['facetFilters']} ({extras[0].total_count} cars)" if extras else "", "; ".join(tried))
    return extras
