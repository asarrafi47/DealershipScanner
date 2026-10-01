"""
Feed sticker prices read off photographs into the observed-price registry.

The registry (`package_observations` / `package_values`, see
backend/enrichment/package_registry.py) is a self-improving price book: every time we see
an option priced on a real document, it records the sighting and folds it into a per
year/make/model/trim value. It is what lets the generated build sheet quote a real number
for "Premium Package" instead of leaving it blank.

Until now it had two feeds: parsed OEM sticker PDFs, and dealer listing text. The vision
backfill adds a third -- options read directly off photographs of Monroneys that dealers
publish in their galleries -- covering cars for which no sticker PDF exists.

    .venv/bin/python -m backend.scripts.feed_package_registry_from_vision --dry-run
    .venv/bin/python -m backend.scripts.feed_package_registry_from_vision

Why authority 2 and not 3
-------------------------
`oem_sticker` (a parsed PDF) sits at authority 3, `dealer_listing` at 2. Photographed
stickers go in at **2**, deliberately below the PDF, for one specific reason: a photo taken
at an angle shifts the price column against the label column, so an option can inherit its
neighbour's price. That failure is documented three times over in this corpus (car 883479's
$1,450 "First Aid Kit", car 887269's centre caps, car 888415's "Decoding additional
functions"). A parsed PDF cannot fail that way.

The guards catch most of it -- and everything fed from here has already passed the VIN
gate, the original-Monroney gate, the USD gate, the skew filter and the corpus-wide
corroboration scrub -- but "mostly caught" is not the same as "structurally impossible",
and the registry's authority ladder exists precisely to encode that difference. If these
observations prove out against the PDF-sourced ones over time, raising the tier is a
one-line change.

Only rows whose provenance is *provable* are fed. Rows recorded before the provenance
guards existed are marked `unverified_pre_guard` and are skipped -- not because they are
presumed wrong, but because we cannot show they are right.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.db.connect import connect as db_connect, inventory_dsn  # noqa: E402

_log = logging.getLogger("vision_to_registry")

# Declared in backend/enrichment/package_registry.py, in BOTH the authority and
# confidence maps. This script only asserts it is there.
VISION_SOURCE = "sticker_photo"


# One canonical spelling per marque, keyed on the name with all non-letters stripped.
#
# Two distinct problems live here. Case and punctuation ("RAM"/"Ram", "LEXUS"/"Lexus",
# "Mercedes-benz") account for 4,812 rows across 30 marques and fall out of a squash.
# ALIASES do not: a feed that writes "Mercedes", "Chevy" or "VW" produces a name that no
# amount of case folding will join to "Mercedes-Benz". Both are already present -- this
# corpus holds a bare "Mercedes" and a bare "Land" -- and both split the evidence the
# price book needs, on top of the model-level split the rejection log exposed.
#
# Shorthands are listed even where the corpus has none yet, because the cost of an
# unused entry is zero and the cost of a missed one is silent evidence loss.
_MAKE_CANON = {
    # stylised
    "bmw": "BMW", "gmc": "GMC", "mini": "MINI", "infiniti": "INFINITI",
    "ineos": "INEOS", "brightdrop": "BrightDrop", "mg": "MG",
    # hyphens and spaces
    "mercedesbenz": "Mercedes-Benz", "landrover": "Land Rover",
    "alfaromeo": "Alfa Romeo", "rollsroyce": "Rolls-Royce",
    "astonmartin": "Aston Martin", "harleydavidson": "Harley-Davidson",
    # aliases and truncations
    "mercedes": "Mercedes-Benz", "merc": "Mercedes-Benz", "benz": "Mercedes-Benz",
    "mb": "Mercedes-Benz",
    "chevy": "Chevrolet", "chev": "Chevrolet",
    "vw": "Volkswagen", "volkswagon": "Volkswagen",
    "land": "Land Rover", "range": "Land Rover", "rangerover": "Land Rover",
    "ram": "Ram", "dodgeram": "Ram",
    "vette": "Chevrolet", "caddy": "Cadillac",
    # Intercaps marques. A naive title-case turns these into "Mclaren"/"Rawmaxx", which is
    # exactly what the first run of this normaliser did to 31 McLarens before the
    # already-mixed-case rule below was added.
    "mclaren": "McLaren", "canam": "Can-Am", "rawmaxx": "RawMaxx",
}


def _canon_make(make: str) -> str:
    """
    One spelling per marque.

    `cars.make` carries 30 marques under two or three spellings -- 'RAM' 1,484 rows and
    'Ram' 1,739, 'LEXUS' 2,656 and 'Lexus' 2,063 -- because different dealer feeds shout
    differently. 4,812 rows sit on a non-majority spelling.

    For the price book this splits evidence a third way, on top of the model-level split
    the rejection log already exposed: a Ram accessory seen on 3 'RAM' cars and 2 'Ram'
    cars corroborates as neither. Normalising here costs nothing and recovers those.
    """
    raw = (make or "").strip()
    if not raw:
        return raw
    import re as _re

    squashed = _re.sub(r"[^a-z]", "", raw.lower())
    if squashed in _MAKE_CANON:
        return _MAKE_CANON[squashed]
    # Already mixed case? The feed styled it deliberately -- leave it alone. Only ALL CAPS
    # and all-lower are ambiguous enough to restyle. Skipping this test is how "McLaren"
    # became "Mclaren" and "RawMaxx" became "Rawmaxx".
    if raw != raw.upper() and raw != raw.lower():
        return raw
    # Title-case each hyphen-separated part, so "ROLLS-ROYCE" -> "Rolls-Royce" rather
    # than "Rolls-royce".
    return " ".join(
        "-".join(part.capitalize() for part in word.split("-"))
        for word in raw.split()
    )


def _corroborated_prices(cur, min_agree: int = 2) -> set:
    """
    Which (year, make, model, option) prices more than one car agrees on.

    Photographed Monroneys shot at an angle slide the price column against the label
    column, so an option inherits a neighbour's price. Per-car checks cannot see it: the
    arithmetic gate is blind because shuffling prices AMONG options leaves the total
    unchanged, the outlier filter passes because each individual figure is plausible, and
    the keyword scan missed a real case sitting under its own threshold.

    Across cars it is obvious. Measured over this corpus: of 298 option prices seen on two
    or more cars, **114 disagree** -- and they disagree in the tell-tale pattern, e.g. on
    the 2026 BMW X5 "Black All Weather Floor Mats" reads 50/125/175/225 while "Fitted
    Luggage Compartment" reads 125/175/225/250. Overlapping ladders, each car offset by a
    different amount.

    So a price earns its way into the shared registry by being seen, identically, on at
    least ``min_agree`` different cars. That drops roughly 85% of price claims, which is the
    correct trade: the registry is consumed by the build-sheet generator and quoted to
    shoppers, and a confidently wrong number there is worth far less than a blank.

    Unpriced package NAMES are unaffected -- they carry no figure to get wrong.
    """
    from collections import defaultdict

    cur.execute(
        """
        SELECT c.year, c.make, c.model, t.summary
        FROM car_image_text t JOIN cars c ON c.id = t.car_id
        WHERE t.version = 100 AND t.summary IS NOT NULL
        """
    )
    seen: dict[tuple, list[float]] = defaultdict(list)
    for year, make, model, summary in cur.fetchall():
        s = summary if isinstance(summary, dict) else json.loads(summary)
        if not (year and make and model):
            continue
        for opt in s.get("priced_options") or []:
            name = str(opt.get("name") or "").strip().lower()
            try:
                price = round(float(opt.get("price")))
            except (TypeError, ValueError):
                continue
            if name:
                seen[(int(year), _canon_make(make), str(model).strip(), name)].append(price)

    ok = set()
    for key, prices in seen.items():
        counts: dict[float, int] = {}
        for p in prices:
            counts[p] = counts.get(p, 0) + 1
        for price, n in counts.items():
            if n >= min_agree:
                ok.add(key + (price,))

    # Second pass: corroborate ACROSS models within a make.
    #
    # The model-level key splits evidence that belongs together. "BMW First Aid Kit" costs
    # $50 whichever BMW it is bolted into, and the rejection log showed it refused NINE
    # times as "uncorroborated" while the identical item at the identical price sat in the
    # registry with 63 cars agreeing -- because those 63 were spread across X3, X5, i4 and
    # 330i, and each model counted alone. Same for wheel locks, floor mats, block heaters
    # and paint colours: accessories and paints are priced per make, not per model.
    #
    # Packages are deliberately NOT included. "Premium Package" is a different bundle at a
    # different price on an X3 than on a 7 Series, so pooling those would manufacture
    # agreement rather than find it.
    by_make: dict[tuple, list[float]] = {}
    for (year, make, model, name), prices in seen.items():
        low = name.lower()
        if "package" in low or "pkg" in low:
            continue
        by_make.setdefault((make.lower(), low), []).extend(prices)

    cross = 0
    for (make_l, name), prices in by_make.items():
        counts = {}
        for p in prices:
            counts[p] = counts.get(p, 0) + 1
        for price, n in counts.items():
            if n < min_agree:
                continue
            # Admit this price for every (year, model) that reported it.
            for (year, make, model, nm) in seen:
                if make.lower() == make_l and nm.lower() == name:
                    key = (year, make, model, nm, price)
                    if key not in ok and price in seen[(year, make, model, nm)]:
                        ok.add(key)
                        cross += 1
    if cross:
        _log.info("%d price(s) corroborated across models within a make "
                  "(accessories and paint are priced per make, not per model)", cross)
    return ok


def _self_reconciling_cars(cur) -> set[int]:
    """
    Cars whose own arithmetic closes: base + every itemized option == printed total.

    This is the escape hatch for RARE options, and it is not a weakening of the
    corroboration rule -- it is a different, stronger proof.

    Cross-car corroboration asks "do other cars agree?", which works for a BMW First Aid
    Kit seen 63 times and fails completely for an option that appears once in the whole
    corpus. Roughly half of all captured prices were being refused on that basis, and the
    refusals fall hardest on exactly the rare trims and one-off packages the build sheet
    most needs, because common options are the ones with second sightings.

    Self-reconciliation asks a different question: does this sticker's own arithmetic
    close? A photograph with a shifted price column CANNOT sum to the printed total --
    that is the whole reason the shift is detectable at all. So when base + options lands
    on the total to the dollar, every price on that document is verified by the document,
    and one car is enough.

    Requires ``base_msrp``, which has only been recorded since the arithmetic gate was
    added, so older rows simply fall back to needing corroboration.

    The test itself lives in ``image_batch.reconciles_exactly`` rather than here. This
    function used to carry its own copy, and the copy had the same defect the original did:
    it demanded ``base + options == total`` exactly, which a sticker that itemizes its
    destination charge separately can never satisfy -- and nearly all of them do. The result
    was that of 5,467 candidate cars exactly ONE qualified, so the escape hatch built for
    rare options was, in practice, closed. Two implementations of one rule drifted apart
    silently; one implementation cannot.
    """
    from backend.scripts.image_batch import reconciles_exactly

    cur.execute(
        """
        SELECT car_id, summary FROM car_image_text
        WHERE version = 100 AND sticker_msrp IS NOT NULL
          AND summary->>'base_msrp' IS NOT NULL
        """
    )
    ok: set[int] = set()
    for car_id, summary in cur.fetchall():
        s = summary if isinstance(summary, dict) else json.loads(summary)
        if reconciles_exactly(s):
            ok.add(int(car_id))
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report what would be fed, write nothing")
    ap.add_argument("--limit", type=int, help="cap cars processed")
    ap.add_argument("--min-agree", type=int, default=2,
                    help="cars that must independently report the same price for an "
                         "option before it may enter the registry (0 disables)")
    ap.add_argument(
        "--include-unverified", action="store_true",
        help="also feed rows recorded before the provenance guards existed",
    )
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Export the DSN before touching repo internals: package_registry and the rest of
    # backend.db read os.environ, so without this they raise "INVENTORY_DATABASE_URL
    # must be set" when the URL came from DATABASE_URL alone.
    inventory_dsn(export=True)

    from backend.enrichment import package_registry

    # The source is declared inside package_registry itself, in both the authority and
    # confidence maps. It was briefly patched in from here instead, which registered it in
    # one map and not the other -- a bare KeyError that surfaced as "unknown source".
    if VISION_SOURCE not in package_registry._SOURCE_AUTHORITY:
        raise SystemExit(
            f"{VISION_SOURCE!r} is not registered in backend/enrichment/package_registry.py"
        )

    conn = db_connect(autocommit=True)
    cur = conn.cursor()

    # Fail fast if the rejection ledger is missing: registry observations are
    # written per-car under autocommit, so discovering the table only at the
    # ledger write at the end would leave a partially-fed registry behind and
    # no logged refusals.
    if not args.dry_run:
        cur.execute("SELECT to_regclass('option_rejections')")
        if cur.fetchone()[0] is None:
            raise SystemExit(
                "option_rejections table missing — apply "
                "migrations/V014__option_rejections.sql before feeding the registry"
            )

    sql = """
        SELECT t.car_id, t.summary, c.vin, c.year, c.make, c.model, c.trim
        FROM car_image_text t
        JOIN cars c ON c.id = t.car_id
        WHERE t.version = 100
          AND t.summary IS NOT NULL
          AND c.vin IS NOT NULL AND c.vin <> ''
    """
    if not args.include_unverified:
        # Only what we can prove. See module docstring.
        sql += " AND COALESCE(t.summary->>'msrp_provenance','') <> 'unverified_pre_guard'"
        # And only from a document we believe is the factory's.
        #
        # `cmd_record` used to gate `sticker_msrp` on `sticker_is_original` but not
        # `priced_options`, so a dealer's marketing reprint had its total withheld while its
        # option prices were written -- 103 cars carry 532 such option rows, and 449
        # observations had already reached this shared registry, indistinguishable from
        # verified Monroney reads. (Since 2026-08-18 the total is stored too, labelled by
        # summary `msrp_document` -- which makes THIS filter the only thing keeping
        # non-factory documents out of the shared registry.)
        #
        # The inflow is stopped here rather than at record time because the raw row is
        # self-describing: it stores `sticker_is_original: false` beside the prices, so a
        # reader can see what it is. The registry cannot -- other subsystems read it stripped
        # of that context, which is the same reason fees are refused twice.
        #
        # The 449 already present are deliberately NOT purged. Only 4 options appear on both
        # an original and a reprint, and 3 of those 4 agree on price; that is nowhere near
        # enough to justify withdrawing 449 rows. Refusing to WRITE without evidence and
        # refusing to WITHDRAW without evidence are the same rule pointed in two directions,
        # and the last mass-withdrawal on suspicion here deleted 158 correct MSRPs.
        #
        # Test the SUMMARY flag, not the `has_sticker` column. Cars 162304 and 162608 are the
        # two reprints this was written for, and both carry `has_sticker = false` on the row
        # while their summaries record a document that was read and disclaimed -- a gate built
        # on the column would have missed precisely the cars it exists to catch.
        #
        # The flag is absent on 1,376 older rows, which are kept: absence is not a claim, and
        # those rows predate the reader being asked the question.
        sql += " AND COALESCE(t.summary->>'sticker_is_original','true') <> 'false'"
    sql += " ORDER BY t.car_id"
    if args.limit:
        sql += f" LIMIT {int(args.limit)}"
    corroborated = _corroborated_prices(cur, args.min_agree) if args.min_agree else None
    self_ok = _self_reconciling_cars(cur) if args.min_agree else set()
    if args.min_agree:
        _log.info("%d car(s) whose own arithmetic reconciles exactly -- their prices "
                  "need no cross-car corroboration", len(self_ok))
    if corroborated is not None:
        _log.info("%d (year/make/model/option, price) pair(s) corroborated by >= %d car(s)",
                  len(corroborated), args.min_agree)

    cur.execute(sql)
    rows = cur.fetchall()
    _log.info("candidate cars: %d", len(rows))

    cars_fed = observations = priced = named_only = skipped_no_id = 0
    rejections: list[tuple] = []
    uncorroborated = 0

    for car_id, summary, vin, year, make, model, trim in rows:
        s = summary if isinstance(summary, dict) else json.loads(summary)

        # A price book keyed on year/make/model is useless without them.
        if not (year and make and model):
            skipped_no_id += 1
            continue

        items: list[dict[str, Any]] = []
        for opt in s.get("priced_options") or []:
            name = str(opt.get("name") or "").strip()
            try:
                price = float(opt.get("price"))
            except (TypeError, ValueError):
                continue
            if not name:
                continue
            # Refuse fees here too, independently of the recording filter. A destination
            # charge is not purchasable equipment, and 14 of them reached the price book
            # before anything checked. Two gates because this one protects a shared,
            # self-improving registry that other subsystems read.
            from backend.scripts.image_batch import _is_mandatory_fee, _is_never_priced

            if _is_mandatory_fee(name):
                continue
            # And bundled lines, for the same reason and with a sharper edge.
            #
            # A SiriusXM trial is never charged for, so a figure printed beside one is the
            # neighbouring row's, picked up from a shifted column. This gate was applied at
            # record time but not here, and four such rows reached the shared price book:
            # "SiriusXM 3-Month Trial Extension" at $350, "1 Year Trial Subscription" at
            # $250 and "3-Year Trial Subscription" at $299 -- each carrying TWO observations,
            # because a shared sticker template produces the SAME wrong pairing on every car
            # printed from it. Corroboration cannot see that error; it confirms it.
            if _is_never_priced(name):
                continue
            # Same writer-side gate as the oem_sticker feed: a sticker's BASE/TOTAL
            # VEHICLE PRICE line (or the car's own model/trim echoed as a name) is
            # not an option and must not enter the shared price book. See
            # _is_vehicle_price_row's docstring for the live damage.
            from backend.enrichment.window_sticker_service import _is_vehicle_price_row

            if _is_vehicle_price_row(
                name,
                price,
                {"make": make, "model": model, "trim": trim},
                s.get("sticker_msrp"),
            ):
                continue
            low = name.lower()
            if corroborated is not None and car_id not in self_ok and (
                int(year), _canon_make(make), str(model).strip(), low, round(price)
            ) not in corroborated:
                uncorroborated += 1
                # Write the refusal down. An admitted price is visible and checkable; a
                # refused one used to vanish, which made an over-tight guard invisible by
                # construction. These rows are also the re-admission queue: corroboration
                # grows with the corpus, so a price refused today for lack of a second
                # sighting may earn one next week.
                rejections.append((
                    car_id, vin, year, make, model, trim, name, price,
                    "uncorroborated",
                    f"no other car reports {name!r} at ${price:,.0f} for "
                    f"{year} {make} {model}",
                ))
                continue
            items.append({
                "kind": "package" if ("package" in low or "pkg" in low) else "option",
                "name": name,
                "price": price,
            })
            priced += 1

        # Packages seen but not priced still carry information: they say this trim CAN
        # have that package, which is what the build sheet needs to list it at all.
        priced_names = {i["name"].lower() for i in items}
        for pkg in s.get("packages") or []:
            name = str(pkg).strip()
            if name and name.lower() not in priced_names:
                items.append({"kind": "package", "name": name})
                named_only += 1

        if not items:
            continue

        car = {"vin": vin, "year": year, "make": make, "model": model, "trim": trim}
        if args.dry_run:
            cars_fed += 1
            observations += len(items)
            continue
        try:
            # No conn= : the registry's SQL uses '?' placeholders that only the repo's
            # own connection wrapper rewrites for psycopg. Handing it a raw psycopg
            # connection makes every statement arrive with "0 placeholders but N
            # parameters". Letting it open its own is both correct and simpler.
            written = package_registry.record_package_observations(
                car, VISION_SOURCE, items
            )
        except Exception as exc:  # noqa: BLE001 - one bad car must not stop the feed
            _log.warning("car %s: %s", car_id, str(exc)[:120])
            continue
        cars_fed += 1
        observations += written

    _log.info(
        "%s %d car(s): %d observation(s) — %d priced, %d package-name-only",
        "would feed" if args.dry_run else "fed", cars_fed, observations, priced, named_only,
    )
    if skipped_no_id:
        _log.info("skipped %d car(s) lacking year/make/model", skipped_no_id)
    if uncorroborated:
        _log.info("refused %d option price(s) no other car corroborated", uncorroborated)
    if rejections and not args.dry_run:
        # A standing refusal is one fact, not one fact per run.
        #
        # This script is re-run after every vision wave, and the first version appended
        # unconditionally: 770 real refusals had become 1,505 rows, so "how much is this
        # gate costing us" -- the question the table exists to answer -- read almost exactly
        # double. Re-logging keeps the earliest `rejected_at`, which is the useful one:
        # corroboration grows with the corpus, so what matters is how long a price has been
        # waiting for a second sighting.
        # The append-unconditionally era left duplicates behind (770 refusals
        # had become 1,505 rows), and CREATE UNIQUE INDEX fails outright on a
        # table that already violates it. Dedupe first, keeping the lowest id
        # per key — the earliest rejected_at, which is the useful one.
        cur.execute(
            "DELETE FROM option_rejections a USING option_rejections b "
            "WHERE a.car_id IS NOT DISTINCT FROM b.car_id "
            "AND a.option_name = b.option_name "
            "AND a.price IS NOT DISTINCT FROM b.price "
            "AND a.reason = b.reason AND a.id > b.id"
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_option_rejections_standing "
            "ON option_rejections (car_id, option_name, price, reason)"
        )
        for r in rejections:
            cur.execute(
                "INSERT INTO option_rejections (car_id, vin, year, make, model, trim, "
                "option_name, price, reason, detail) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                r,
            )
        cur.execute("SELECT count(*) FROM option_rejections")
        _log.info(
            "%d refusal(s) this run; option_rejections holds %d standing refusal(s)",
            len(rejections), cur.fetchone()[0],
        )

    if not args.dry_run:
        cur.execute(
            "SELECT count(*) FROM package_observations WHERE source = %s", (VISION_SOURCE,)
        )
        _log.info("registry now holds %d observation(s) from %s", cur.fetchone()[0], VISION_SOURCE)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
