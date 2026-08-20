"""
Hand a batch of cars' gallery images to an agent, and take its findings back.

The local readers each hit a wall: Apple Vision is fast but reads only text, and
qwen3-vl reads meaning but at ~10s an image is 70+ days for the fleet. This path uses
model agents instead -- far more capable per image (they can tell a Monroney from a
promo graphic, pair an option to its price across columns, and name the dealership from
a building) and, on a Max plan, the constraint is credits rather than wall-clock.

Agents cannot fetch and stream thousands of images themselves, so this script is the
seam:

    claim   -> reserve N cars, download their images, write a manifest
    record  -> validate the agent's findings and merge them into car_image_text

Reservation matters because agents run concurrently. ``claim`` writes a row with
``version = _RESERVED`` so a sibling agent's claim skips that car; ``record`` overwrites
it with the real result. A reservation older than ``--stale-minutes`` is reclaimed, so an
agent that dies mid-batch costs a delay, not a permanent hole in the fleet.

Ordering is by expected value, not id: cars whose MSRP or option data is missing are the
ones a sticker can actually improve, and the dearer the car the more a wrong price costs
a shopper. Cheap cars with complete data come last.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

_log = logging.getLogger("image_batch")

# Provenance marker: findings produced by a vision agent, distinct from the local
# OCR passes (version 1/2) so the two are never confused when auditing.
AGENT_VISION_VERSION = 100

# Reservation sentinel. Negative so no real pass can collide with it.
_RESERVED = -1

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

# Read cap per image. Was 12MB; the full-resolution AEM renditions unlocked by
# _upsize_url run to ~6MB for a 6000x3375 PNG, and a busier scene can exceed 12MB.
_MAX_IMAGE_BYTES = 24 * 1024 * 1024


# ---------------------------------------------------------------------------
# Gallery URLs frequently point at a small RENDITION of a photo whose master is
# larger on the same CDN. Resolution is the binding constraint on reading window
# stickers -- a Monroney at 1024px is visible but illegible -- so before settling
# for the stored URL, try the same asset's larger variant.
#
# Every pattern below was verified empirically against production URLs on
# 2026-08-18 (paced GETs with a browser UA; dimensions read from the bytes):
#
#   assets.cai-media-management.com/resize/{W}x{H}/...
#       Serves min(requested, master), re-encoded. Dropping the /resize/WxH/
#       segment serves the MASTER at original quality: /resize/640x640/ asset
#       went 640x480 -> 1280x960; a /resize/1024x1024/ asset whose master is
#       1024x576 stayed 1024x576 but gained ~20% more JPEG bytes (no re-encode).
#       (/resize/0x0/ is byte-identical to the bare path.)
#
#   media.rti.toyota.com/adobe/assets/.../as/image.png?size=1200,663
#       Adobe AEM delivery; the size param binds. Stripping it served the master:
#       1179x663 -> 6000x3375 (5.4-6MB). Verified on two assets, including the
#       %2C-encoded-comma variant.
#
#   content.homenetiol.com/{a}/{b}/{W}x{H}/...
#       Path segment is a fit box; 0x0 means uncapped. /1600x1200/ -> /0x0/
#       served 1600x1200 -> 2000x1500. (Most stored HomeNet URLs are already 0x0.)
#
#   images.otf3.pixelmotiondemo.com/{W}x{H}/{name}.jpg
#       /364x273/ -> /2048x1536/ resolved 200 at 2048x1536. (Whether that is a
#       true master or an interpolation is unverified; it can only help.)
#
# Measured and deliberately NOT transformed:
#   pictures.dealer.com          bare URLs already serve the master (1600x1200);
#                                the rare ?w=640 form ignores the param (no-op).
#   vehicle-images.carscommerce.inc  masters served as stored (2880x2160 seen);
#                                the ?size= param does not bind.
#   delivery.via.assetscs.toyota.com  stored width=1920 already exceeds the
#                                1200x675 masters; stripping is a no-op.
#   photos.chapmanchoice.com     /640/ is the only rendition (others 404).
#   static.overfuel.com          _640_ is the only rendition (others 403).
#   cdn.gma.to                   thumbor with no_upscale; master already served.
# ---------------------------------------------------------------------------

_CAI_RESIZE = re.compile(
    r"^(https?://assets\.cai-media-management\.com)/resize/\d{1,5}x\d{1,5}/(.+)$", re.I
)
_AEM_SIZE = re.compile(
    r"^(https?://media\.rti\.toyota\.com/adobe/assets/[^?]+/as/image\.[a-z]{3,4})\?(.*)$",
    re.I,
)
_HOMENET_SIZE = re.compile(
    r"^(https?://content\.homenetiol\.com/[^/]+/[^/]+)/(\d{1,5})x(\d{1,5})/(.+)$", re.I
)
_PIXELMOTION_SIZE = re.compile(
    r"^(https?://[^/]*\.pixelmotiondemo\.com)/(\d{1,5})x(\d{1,5})/([^/?]+)$", re.I
)


def _upsize_url(url: str) -> list[str]:
    """
    Candidate URLs for a larger rendition of the same asset: best first, the
    stored URL always last. At most 2 candidates precede the original, so a
    CDN that rejects the transform costs a bounded number of extra requests.
    """
    candidates: list[str] = []
    u = url or ""

    m = _CAI_RESIZE.match(u)
    if m:
        candidates.append(f"{m.group(1)}/{m.group(2)}")

    m = _AEM_SIZE.match(u)
    if m and re.search(r"(?:^|&)size=", m.group(2), re.I):
        rest = "&".join(
            p for p in m.group(2).split("&") if not re.match(r"size=", p, re.I)
        )
        candidates.append(f"{m.group(1)}?{rest}" if rest else m.group(1))

    m = _HOMENET_SIZE.match(u)
    if m and (m.group(2), m.group(3)) != ("0", "0"):
        candidates.append(f"{m.group(1)}/0x0/{m.group(4)}")

    m = _PIXELMOTION_SIZE.match(u)
    if m:
        w, h = int(m.group(2)), int(m.group(3))
        if 0 < w < 2048 and h > 0:
            # Scale the fit box up to 2048 wide, preserving the stored aspect.
            new_h = round(h * 2048 / w)
            candidates.append(f"{m.group(1)}/2048x{new_h}/{m.group(4)}")

    out: list[str] = []
    for c in candidates[:2] + [u]:
        if c and c not in out:
            out.append(c)
    return out


# Magic-byte prefixes for the formats dealer CDNs actually serve. A transformed
# URL that 200s with an HTML error page must never replace a working original,
# so an upsize candidate is only accepted when its payload IS an image.
def _looks_like_image(blob: bytes) -> bool:
    if len(blob) < 12:
        return False
    if blob.startswith((b"\xff\xd8\xff", b"\x89PNG\r\n\x1a\n", b"GIF87a", b"GIF89a")):
        return True
    if blob[:4] == b"RIFF" and blob[8:12] == b"WEBP":
        return True
    if blob[4:8] == b"ftyp":  # AVIF / HEIC
        return True
    return False


def _dsn() -> str:
    dsn = (os.environ.get("INVENTORY_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip()
    if dsn:
        return dsn
    env = _REPO_ROOT / ".env"
    if env.exists():
        m = re.search(r"^INVENTORY_DATABASE_URL=(.+)$", env.read_text(), re.M)
        if m:
            return m.group(1).strip().strip("'\"")
    raise SystemExit("INVENTORY_DATABASE_URL is not set")


def _connect():
    import psycopg

    conn = psycopg.connect(_dsn(), connect_timeout=15)
    conn.autocommit = True
    return conn


def _select_images(urls: list[str], budget: int) -> list[str]:
    """
    Evenly spaced across the gallery, both ends included.

    Not the head and not the tail: dealers interleave document shots wherever they like.
    On the gallery that started this work the highlights slides sat at positions 6, 12
    and 18 of 40, so a head sample found nothing and a head+tail sample found 4 of 336.
    """
    n = len(urls)
    if n <= budget or budget <= 0:
        return urls
    if budget == 1:
        return urls[:1]
    step = (n - 1) / (budget - 1)
    return [urls[i] for i in sorted({int(round(i * step)) for i in range(budget)})]


def _fetch(url: str) -> bytes | None:
    try:
        # Galleries carry unescaped spaces; urllib raises http.client.InvalidURL for those,
        # which is an HTTPException and slips past OSError/ValueError.
        safe = urllib.parse.quote(url, safe=":/?#[]@!$&'()*+,;=%~")
        req = urllib.request.Request(safe, headers={"User-Agent": _UA, "Accept": "image/*"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            if resp.status != 200:
                return None
            return resp.read(_MAX_IMAGE_BYTES)
    except Exception:
        # One unreachable image must never end a batch.
        return None


def _download(url: str, dest: Path) -> bool:
    if not (url or "").lower().startswith(("http://", "https://")):
        return False
    candidates = _upsize_url(url)
    for i, cand in enumerate(candidates):
        blob = _fetch(cand)
        if blob:
            # An upsize candidate must prove it is an image; the stored URL keeps
            # the original lenient behavior (whatever the gallery served before).
            if cand == url or _looks_like_image(blob):
                try:
                    dest.write_bytes(blob)
                except OSError:
                    return False
                return True
        if i < len(candidates) - 1:
            # A beat between attempts at the same CDN, on top of the per-car pacing.
            time.sleep(0.2 + random.random() * 0.2)
    return False


def _options_gap(base: Any, priced: list[dict], total: float | None) -> float | None:
    """Total minus (base + itemized options). None when it cannot be computed."""
    if total is None:
        return None
    try:
        base_f = float(str(base).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return None
    if base_f <= 0:
        return None
    s = 0.0
    for o in priced or []:
        try:
            s += float(o.get("price") or 0)
        except (TypeError, ValueError):
            continue
    return round(float(total) - base_f - s, 2)


def _reconciles(base: Any, priced: list[dict], total: float) -> tuple[bool, str]:
    """
    Does base + itemized options actually add up to the claimed total?

    This is the only check proposed here that catches a price column shifted onto the
    TOTAL rather than onto a named option. The skew filter reads `priced_options`, so it
    is structurally blind to that case -- car 883394's $61,345 sat on the Destination
    Charge row with the Total row blank, and every range and plausibility check passed it
    because $61,345 is a perfectly ordinary BMW X1 total.

    Unlike the three named-option rules this project tested and rejected, it needs no
    threshold drawn from corpus statistics: a Monroney is an addition, so its parts either
    sum to its total or the reading is wrong somewhere.

    The tolerance exists because agents routinely capture the options they can read and
    miss one in a glare band, and because destination is sometimes itemized and sometimes
    folded in. So a shortfall is NOT evidence of a bad total -- it is the normal case. Only
    the impossible direction is refused: itemized parts that EXCEED the total, which means
    a price landed on the wrong row.
    """
    try:
        base_value = float(str(base).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return True, ""          # nothing to reconcile against; not evidence of a defect
    if base_value <= 0 or total <= 0:
        return True, ""
    options_sum = 0.0
    for opt in priced or []:
        try:
            options_sum += float(opt.get("price") or 0)
        except (TypeError, ValueError):
            continue
    # 1.5% of the total absorbs rounding and a single unlisted fee; anything past it means
    # the parts cost more than the whole, which a real sticker cannot do.
    if base_value + options_sum > total * 1.015:
        return False, (
            f"base ${base_value:,.0f} + options ${options_sum:,.0f} "
            f"= ${base_value + options_sum:,.0f}, which exceeds the claimed total "
            f"${total:,.0f} -- a price is on the wrong row"
        )
    return True, ""


def reconciles_exactly(s: dict) -> bool:
    """
    Do this car's own base + itemized options add up to its printed total?

    If they do, every price on the sticker is corroborated by the sticker itself, and
    no corpus statistic may overrule them. That matters because the corpus rules are
    about what is USUAL, and a Monroney is about one car: "M Sport Package" is standard
    equipment on a loaded BMW and a $2,550 line on a base one, so the
    usually-included rule strips it exactly when it is real.

    This was found by an auditor comparing an agent's submitted JSON against what
    survived in the database: on 2 of 5 sampled cars the scrub had deleted a
    verified-correct price -- "M Sport Package" $2,550 on car 881839 and "Rear Climate
    Control Console" $900 on car 888335 -- both plainly printed on the photographs.
    Cold start makes it worse: an option needs corroborating samples the corpus does
    not have yet at 5,000 cars, so early readings are the most likely to be discarded.

    Arithmetic beats frequency here. A sum that lands on the printed total to the
    dollar cannot be a shifted column.

    The second branch below is not decoration. This function shipped with only the
    exact-match test while its own comment claimed to handle destination, and an
    auditor caught the consequence: a Monroney that itemizes freight separately can
    NEVER close to zero, because destination is not equipment and the readers are told
    not to file it as an option. Nearly every real sticker does itemize it. So the
    exemption almost never fired, and the usually-included rule went on deleting
    verified lines -- "M Sport Package" $2,550 off car 884344 and "Convenience Package"
    $1,400 off car 881828, both arithmetic-checked to the dollar by the reader that
    submitted them, both gone from the database within the same command.
    """
    total = s.get("sticker_msrp")
    base = s.get("base_msrp")
    try:
        total_f, base_f = float(total), float(str(base).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return False
    if total_f <= 0 or base_f <= 0:
        return False
    opts_sum = 0.0
    for o in s.get("priced_options") or []:
        try:
            opts_sum += float(o.get("price") or 0)
        except (TypeError, ValueError):
            return False
    residual = total_f - base_f - opts_sum
    if abs(residual) < 1.0:
        return True          # destination folded into the base price, or itemized as an option
    # Destination itemized on its own row and correctly kept out of the options list.
    # Everything else on the sticker is then accounted for, which is the same proof.
    #
    # A car whose reader simply MISSED an option of freight-like size also lands here.
    # That is the intended trade: this branch only withholds permission to delete, it
    # never adds a price. Letting one skewed row survive costs a wrong option price;
    # refusing the exemption costs a correct one, and the corpus rules have now been
    # measured deleting correct ones on 2 of the 9 cars able to be checked.
    return _DESTINATION_BAND[0] <= residual <= _DESTINATION_BAND[1]


def cmd_claim(args: argparse.Namespace) -> int:
    # Wipe the output directory FIRST, before any path that can return early.
    #
    # Wave directories are reused (imgbatch2/w7 on every run), so a dead agent leaves its
    # results.json behind for the next occupant. Two agents died mid-wave and a later
    # record() replayed their predecessors' files, rewriting 20 cars that were never
    # claimed while the 19 real ones sat orphaned. The most dangerous case is an agent
    # that claims NOTHING and then finds a stale file waiting -- which is exactly the
    # branch an earlier placement of this wipe skipped.
    import shutil as _shutil

    _out = Path(args.out)
    if _out.exists():
        _shutil.rmtree(_out, ignore_errors=True)
    _out.mkdir(parents=True, exist_ok=True)

    conn = _connect()
    cur = conn.cursor()

    # Reclaim reservations from agents that died mid-batch.
    cur.execute(
        "DELETE FROM car_image_text WHERE version = %s "
        "AND extracted_at < NOW() - (%s || ' minutes')::interval",
        (_RESERVED, str(args.stale_minutes)),
    )
    if cur.rowcount:
        _log.info("released %d stale reservation(s)", cur.rowcount)

    # Reserve and select in ONE atomic statement.
    #
    # This was originally a SELECT followed by a loop of INSERTs, which is a textbook
    # race: twelve agents ran the SELECT within the same second, all saw the same top-N
    # cars, and all then "reserved" them. The wave reported ~118 cars processed and wrote
    # 27 -- most of the fleet re-analysed the same twenty cars, and every one of those
    # duplicate runs cost real tokens.
    #
    # INSERT ... SELECT ... ON CONFLICT DO NOTHING RETURNING makes the reservation the
    # same operation as the choice, so a car is handed to exactly one agent: whoever
    # loses the conflict gets no row back and moves on to the next candidate.
    # Each agent draws at random from a POOL of high-priority candidates rather than
    # taking the global top-N. Every agent sorting identically means every agent fights
    # for the same head rows, and retrying re-fights for the same ones -- in testing that
    # left 3 of 8 agents with nothing even after the conflict handling was correct.
    # Sampling from a pool keeps the priority ordering (the pool is the best N*20 cars)
    # while making collisions rare instead of certain.
    claim_sql = """
        INSERT INTO car_image_text (car_id, vin, version, extracted_at)
        SELECT cand.id, cand.vin, %s, NOW()
        FROM (
        SELECT c.id, COALESCE(c.vin,'') AS vin
        FROM cars c
        WHERE c.listing_removed_at IS NULL
          AND c.gallery IS NOT NULL AND c.gallery NOT IN ('', '[]')
          -- ANY existing row disqualifies a car, not just an agent row or a reservation.
          --
          -- `car_image_text.car_id` is the PRIMARY KEY, so a car that already has a
          -- LOCAL-OCR row (version 1-99) can never be inserted: the statement below hits
          -- ON CONFLICT DO NOTHING and silently returns nothing for it. The candidate
          -- filter used to allow those cars, and the priority ORDER BY then sorted them
          -- straight to the front, because "dealers that photograph stickers" is derived
          -- from exactly the local-OCR pass. The result was a pool holding **85 dead rows
          -- in every 100**: asking for 12 cars returned 2, measured with a single agent and no
          -- contention at all. Waves delivered 34-40 cars instead of 144, and it read as
          -- pool contention for weeks -- the retry loop re-drew from the same
          -- deterministic pool head every time, so it could never recover.
          --
          -- The 14,036 locally-analysed cars are therefore skipped rather than upgraded.
          -- Upgrading them would mean reserving their row in place, and the stale sweep
          -- DELETEs reservations, so an agent dying mid-batch would destroy the local OCR
          -- it was meant to improve on. Re-analysing them needs a reservation that
          -- remembers the prior version; that is a separate change.
          AND NOT EXISTS (
                SELECT 1 FROM car_image_text t WHERE t.car_id = c.id
          )
        ORDER BY
            -- Whether the dealer photographs stickers AT ALL dominates everything else.
            -- Ordering by "no MSRP, dearest first" sounded right and was actively wrong:
            -- it selected Lamborghinis, McLarens and Porsches, and exotic dealers shoot
            -- studio photography rather than paperwork, so the first 18 cars analysed
            -- produced zero stickers. Volume franchise dealers photograph the Monroney.
            -- The local OCR pass already told us which 46 dealers those are.
            (c.dealer_id IN (
                SELECT c2.dealer_id FROM car_image_text t2
                JOIN cars c2 ON c2.id = t2.car_id
                WHERE t2.version BETWEEN 1 AND 99 AND t2.has_sticker
            )) DESC,
            (c.msrp IS NULL) DESC,
            COALESCE(c.price, 0) DESC
        LIMIT %s
        ) cand
        ORDER BY random()
        LIMIT %s
        ON CONFLICT (car_id) DO NOTHING
        RETURNING car_id
        """

    def _claim(n: int) -> list[int]:
        # The pool has to be large relative to TOTAL concurrent demand, not to one agent's
        # ask. At n*20 the pool was 240 rows while 12 agents wanted 144 between them --
        # over half the pool claimed at once, so collisions dominated, retries burned
        # through their empty-round budget, and agents returned short. Deliveries decayed
        # 143 -> 60 cars per wave while the candidate supply was still ~22,000, which
        # looked like the pool running dry and was really just contention.
        pool = max(n * args.pool_factor, 400)
        cur.execute(
            claim_sql,
            (_RESERVED, pool, n),
        )
        return [r[0] for r in cur.fetchall()]

    # One call only. This used to be followed by a second
    #     claimed_ids = [r[0] for r in cur.fetchall()]
    # left over from when the claim was inlined here. _claim() has already drained the
    # cursor, so that line always evaluated to [] and overwrote a good result: every
    # agent reserved its first `count` cars, discarded their ids, and fell through to the
    # retry loop for a fresh set. The discarded rows stayed reserved -- invisible to the
    # next claim, unanalysed, and only released by the 90-minute stale sweep. It cost
    # roughly `count` cars per agent per wave and looked exactly like pool contention.
    claimed_ids = _claim(args.count)

    # Losing the conflict yields no row, and the statement does not fall through to the
    # next candidate -- so with twelve agents starting together, one wins the whole batch
    # and eleven come back empty. Retrying re-runs the SELECT, which by then excludes
    # everything just reserved, so each agent walks down the queue until it has its share.
    # An empty result means one of two very different things: everything is claimed
    # (stop), or a sibling agent won this instant's candidates (keep going). Treating
    # them the same is what left 4 of 6 agents idle in testing -- the first lost round
    # ended the loop. So only conclude exhaustion after several consecutive empties,
    # with a jittered pause so retries de-synchronise instead of colliding again.
    attempts = 0
    empty_rounds = 0
    while len(claimed_ids) < args.count and attempts < 12 and empty_rounds < 3:
        attempts += 1
        more = _claim(args.count - len(claimed_ids))
        if more:
            claimed_ids.extend(more)
            empty_rounds = 0
        else:
            empty_rounds += 1
            time.sleep(0.15 + random.random() * 0.4)

    if claimed_ids:
        cur.execute(
            """SELECT c.id, COALESCE(c.vin,''), c.year, c.make, c.model, c.trim,
                      c.price, c.msrp, c.gallery, c.exterior_color, c.interior_color
               FROM cars c WHERE c.id = ANY(%s)""",
            (claimed_ids,),
        )
        rows = cur.fetchall()
    else:
        rows = []
    if not rows:
        Path(args.out).mkdir(parents=True, exist_ok=True)
        (Path(args.out) / "manifest.json").write_text(json.dumps({"cars": []}))
        _log.info("nothing left to claim")
        return 0

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    cars: list[dict[str, Any]] = []
    for car_id, vin, year, make, model, trim, price, msrp, gallery, ext_color, int_color in rows:
        # Reserve immediately so a sibling agent cannot claim the same car.
        cur.execute(
            "UPDATE car_image_text SET vin = COALESCE(%s, vin) WHERE car_id = %s",
            (vin or None, car_id),
        )
        try:
            urls = json.loads(gallery) or []
        except (json.JSONDecodeError, TypeError):
            urls = []
        if not isinstance(urls, list) or not urls:
            continue

        car_dir = out_dir / str(car_id)
        car_dir.mkdir(exist_ok=True)
        picked = _select_images([u for u in urls if isinstance(u, str)], args.images)

        paths: list[str] = []
        with ThreadPoolExecutor(max_workers=6) as pool:
            jobs = {}
            for i, u in enumerate(picked):
                dest = car_dir / f"{i:02d}.jpg"
                jobs[pool.submit(_download, u, dest)] = (dest, u)
            for fut, (dest, _u) in jobs.items():
                if fut.result():
                    paths.append(str(dest))
        if not paths:
            continue

        cars.append({
            "car_id": car_id,
            "vin": vin,
            "listing": f"{year or ''} {make or ''} {model or ''} {trim or ''}".strip(),
            "listed_price": price,
            "feed_msrp": msrp,
            # What the dealer feed CLAIMS the colors are. The reader reports what the
            # photographs show; disagreement is a data-quality signal, not something
            # the reader should reconcile.
            "feed_exterior_color": ext_color,
            "feed_interior_color": int_color,
            "gallery_size": len(urls),
            "images": sorted(paths),
        })
        time.sleep(args.delay * (0.6 + random.random() * 0.8))

    manifest = {"cars": cars, "claimed_at": time.time()}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=1))
    _log.info("claimed %d car(s), %d image(s) downloaded to %s",
              len(cars), sum(len(c["images"]) for c in cars), out_dir)
    return 0


def _clean_number(value: Any, lo: float, hi: float) -> float | None:
    """A price the agent reported, or None if it is not a believable amount."""
    if value is None:
        return None
    if isinstance(value, str):
        value = re.sub(r"[^0-9.]", "", value) or None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if lo <= num <= hi else None


# Mandatory fees. These are printed on the sticker with a price, but they are not options:
# every car carries them, they cannot be declined, and recording them as purchasable
# equipment pollutes the observed-price book -- 14 "Destination Charge" observations and 8
# package_values rows had already leaked through before this filter existed.
#
# Match the WORD, not the phrase. The list began as full phrases and a reader that
# hedged its label walked straight through it: car 888263 recorded
# "Destination/handling charge (printed on sticker as 'Refrigerant')" at $1,350, which
# contains "destination" but not "destination charge", so the filter never fired and a
# freight charge was stored as a purchasable option. Readers describe what they see in
# their own words, so the terms have to survive being reworded.
#
# The term list itself lives in package_registry (its choke-point gate must
# agree with this record-time one); import, never copy.
from backend.enrichment.package_registry import FEE_TERMS as _FEE_TERMS


# Line items that are BUNDLED and therefore never carry a price on a Monroney. A figure
# next to one of these is always the neighbouring row's, picked up from a shifted column.
#
# This class defeats every statistical guard we have. The corroboration gate assumes that
# when several cars disagree about an option's price, one of them is misread -- but a
# systematic column shift on a SHARED STICKER TEMPLATE produces the SAME wrong pairing on
# every car that uses it. Eight cars in this corpus all recorded a priced "SiriusXM Trial
# Subscription" ($900 on four of them, from a template where $900 is the Harman Kardon
# system on the line above). They corroborated each other into looking perfectly normal,
# and $900 also sits under the $950 unpriced-option floor, so nothing tripped.
#
# Agreement is not evidence when the error is in the template. Only a domain fact catches
# it: a free trial is free.
from backend.enrichment.package_registry import NEVER_PRICED_TERMS as _NEVER_PRICED


def _is_never_priced(name: str) -> bool:
    low = " ".join((name or "").lower().split())
    return any(term in low for term in _NEVER_PRICED)


def _is_mandatory_fee(name: str) -> bool:
    low = (name or "").strip().lower()
    return any(term in low for term in _FEE_TERMS)


# Small dealer-installed accessories. On a Monroney these are the cheapest lines on the
# sheet, and they sit immediately above the Destination Charge.
_ACCESSORY_TERMS = (
    "floor mat", "first aid", "center cap", "wheel lock", "splash guard", "mud flap",
    "cargo mat", "cargo net", "key fob", "touch-up", "touch up", "license plate",
    "valve cap", "lug nut", "trunk mat", "all weather", "all-weather",
    # "wheel lock" does not substring-match "Wheel Stud Locks", which is how one
    # skewed row slipped past this list and into the DB.
    "wheel stud", "stud lock",
)

# A destination/freight charge is four figures and lands just below those accessory rows.
#
# The ceiling must cover HEAVY-DUTY trucks, not just cars. At 2,500 the 2026 Ram 3500's
# itemized $2,595 destination charge fell $95 outside the band, so reconciles_exactly()
# refused the exemption for car 162315 even though its options summed to the printed
# total to the dollar -- and the usually-included rule then deleted its arithmetic-proven
# "8-Speed Automatic $1,000" line. (GM's HD vans print $2,395, inside the old band; Ram
# HD was the first to cross it.) In _is_skewed_option the band is subsumed by the
# `or price > 600.0` clause, so widening it changes nothing there.
_DESTINATION_BAND = (900.0, 2_700.0)


def _is_skewed_option(name: str, price: float) -> bool:
    """
    True when an option's price plainly belongs to the row beneath it.

    A sticker photographed at an angle makes the price column drift against the item-name
    column, so a cheap accessory inherits the Destination Charge below it. Audits caught
    this on 17 rows -- verified at pixel level on car 887269, where the sticker reads
    "Floor Mats $225 / Floating Center Caps $175 / Destination Charge $1,450" and the
    reader recorded "Floating Center Caps: $1,450".

    It is a nasty class because the MSRP TOTAL stays correct (it is a bold, well-separated
    line), so the headline number looks right while the itemisation is fiction. Clean reads
    in the same wave put Center Caps at $175 and a First Aid Kit at $50, which is the scale
    these parts actually cost -- nothing near four figures.

    Dropping the price keeps the option name, which is still true; only the number is lost.
    """
    low = name.lower()
    # A bundle is not an accessory, whatever words appear inside its name. "TRD
    # Performance Package (Dual Air Intake Boxes, TRD Cat-Back Exhaust, TRD Performance
    # Badge...)" at $981 is a real, correctly-priced package -- it tripped this guard only
    # because "badge" appears in the parts list. Packages legitimately cost four figures,
    # so exempting them removes the false positives without weakening the real check.
    if any(w in low for w in ("package", " pkg", "bundle", "group")):
        return False
    if not any(term in low for term in _ACCESSORY_TERMS):
        return False
    return _DESTINATION_BAND[0] <= price <= _DESTINATION_BAND[1] or price > 600.0


def cmd_record(args: argparse.Namespace) -> int:
    results = json.loads(Path(args.results).read_text())
    if isinstance(results, dict):
        results = results.get("cars") or results.get("results") or []
    if not isinstance(results, list):
        _log.error("results must be a JSON list of per-car objects")
        return 2

    conn = _connect()
    cur = conn.cursor()
    written = 0

    # Only write cars that are currently RESERVED. `record` used to trust whatever
    # car_ids a results file named, which is how a stale file from a previous wave
    # silently overwrote 20 unrelated cars. A reservation is proof this caller claimed
    # the car; without one, the file is not describing work this run did.
    submitted = []
    for item in results:
        if isinstance(item, dict):
            try:
                submitted.append(int(item.get("car_id")))
            except (TypeError, ValueError):
                pass
    reserved: set[int] = set()
    if submitted:
        cur.execute(
            "SELECT car_id FROM car_image_text WHERE car_id = ANY(%s) AND version = %s",
            (submitted, _RESERVED),
        )
        reserved = {r[0] for r in cur.fetchall()}
    rejected = [c for c in submitted if c not in reserved]
    if rejected and not args.allow_unreserved:
        _log.error(
            "refusing %d car(s) with no live reservation -- stale results file? %s",
            len(rejected), rejected[:8],
        )

    for item in results:
        if not isinstance(item, dict):
            continue
        try:
            car_id = int(item.get("car_id"))
        except (TypeError, ValueError):
            continue
        if car_id not in reserved and not args.allow_unreserved:
            continue

        vin = (item.get("vin") or "").strip() or None
        equipment = [str(x).strip() for x in (item.get("equipment") or []) if str(x).strip()]
        packages = [str(x).strip() for x in (item.get("packages") or []) if str(x).strip()]

        priced: list[dict[str, Any]] = []
        for opt in item.get("options") or []:
            if not isinstance(opt, dict):
                continue
            name = str(opt.get("name") or "").strip()
            price = _clean_number(opt.get("price"), 50, 100_000)
            if not name or price is None:
                continue
            if _is_never_priced(name):
                # A bundled trial cannot cost money; the figure belongs to another row.
                _log.warning("car %s: dropping priced %r -- bundled items are never priced",
                             car_id, name[:60])
                continue
            if _is_mandatory_fee(name):
                # A fee, not an option. Every car has one; it is not purchasable.
                continue
            if _is_skewed_option(name, price):
                # Column drift, not a real price -- see _is_skewed_option.
                continue
            priced.append({"name": name, "price": price})

        # An MSRP is only taken when the agent confirmed the sticker belongs to THIS car.
        # A sticker photographed from a neighbouring vehicle is the failure that produces
        # a fabricated "below MSRP" badge on a shopper-facing page.
        # Three conditions, not two. The third exists because of car 888561: its sticker
        # was shot at an angle with the "Total Suggested Retail Price" hidden behind a wiper
        # antenna, so the agent summed base + options by hand -- and silently dropped the
        # Destination Charge. The result understated MSRP by ~$1,450 while passing every
        # check: VIN exact, confident notes, inside the deviation band. A reconstructed
        # total is precisely where a missed line disappears without trace, so only a total
        # actually read off the document is accepted.
        # Currency is the fourth condition. Car 592273 carried a genuine Canadian Ford
        # Monroney -- EnerGuide L/100km panel, French text, "NOT INTENDED FOR SALE OR
        # REGISTRATION IN US" -- whose "TOTAL MSRP $123,174.00" is correct in CAD. The car
        # lists at $85,072 USD in Phoenix, so storing the figure unconverted invents a
        # 44.8% "below MSRP" discount. The reading agent spotted it and said so in notes,
        # which nothing downstream reads. Anything not clearly USD is refused rather than
        # converted: an exchange rate at an unknown date is a guess, and a missing MSRP is
        # better than a wrong one.
        currency = str(item.get("sticker_currency") or "USD").strip().upper()
        msrp = None
        if (
            item.get("vin_confirmed")
            and item.get("has_window_sticker")
            and item.get("msrp_read_directly")
            and currency == "USD"
            # sticker_is_original is deliberately NOT a gate any more. It was, from
            # the day the flag was first checked at all: three Landers McLarty RAM
            # build sheets, each disclaiming "this is not the actual Monroney label"
            # in its own footer, had sat in sticker_msrp as if they were factory
            # documents -- the agents set the flag correctly every time; the gate
            # simply ignored it -- so the fix then was to refuse non-original
            # documents outright. Policy change 2026-08-18 (explicit user decision):
            # the photographed sticker is the single source of truth, so a dealer
            # build sheet's total is STORED too -- but its provenance travels with
            # it. The flag is still persisted below, and summary['msrp_document']
            # now records which document the total came from ('monroney' vs
            # 'dealer_build_sheet') so every reader can label it honestly instead
            # of the value being silently nulled.
        ):
            msrp = _clean_number(item.get("msrp"), 5_000, 500_000)

        # Arithmetic reconciliation. See _reconciles(): the last gate that can catch a
        # shifted column landing on the total itself.
        reconcile_note = ""
        priced_refused = None
        if msrp is not None:
            ok, why = _reconciles(item.get("base_msrp"), priced, msrp)
            if not ok:
                reconcile_note = why
                _log.warning("car %s: MSRP refused -- %s", car_id, why)
                msrp = None
                # The itemization is refused along with the total. The same arithmetic
                # that disproves one disproves the other: when base + options exceeds
                # the claimed total, at least one recorded price sits on the wrong row,
                # and nothing here can tell which. This gate used to null only the MSRP
                # while summary['priced_options'] still persisted the disproven prices
                # into car_image_text -- from where the registry feed pushed them into
                # the shared price book. A 10-car sticker re-read found 9 of 69 stored
                # option prices wrong, every one a single-row column drift, every one
                # provable by exactly this sum-check.
                #
                # The names survive as unpriced package entries: that the option is ON
                # the car is still a true observation; only its number is disproven. A
                # confidently wrong price is worth far less than a blank -- but the
                # evidence is never destroyed: the refusal reason and the raw note both
                # persist in the summary below.
                already = {p.strip().lower() for p in packages}
                packages += [
                    o["name"] for o in priced
                    if o["name"].strip().lower() not in already
                ]
                priced_refused = why
                priced = []

        summary = {
            "version": AGENT_VISION_VERSION,
            "equipment": equipment,
            "packages": packages,
            "priced_options": priced,
            # Why an itemization was refused (its parts arithmetically exceed the
            # claimed total -- see the reconciliation block above). None when the sums
            # held or nothing applied. The option NAMES live on in `packages`; only
            # the disproven numbers are gone.
            "priced_options_refused": priced_refused,
            "sticker_msrp": msrp,
            "sticker_image_urls": item.get("sticker_images") or [],
            "dealer_name_seen": (item.get("dealer_name") or None),
            # Factory allocation, kept apart from current location. Conflating the two
            # made a correct plate-frame reading get replaced by the invoice rooftop.
            "sticker_sold_to": (item.get("sticker_sold_to") or None),
            "dealer_domains": item.get("dealer_domains") or [],
            # Colors observed on the PHYSICAL car in its photographs. Candidates, not
            # facts: readers are instructed to report null when a gallery is stock/CGI
            # renders, because a configurator image depicts a spec, not this vehicle.
            # Kept beside the feed's claim so a later adoption pass can weigh
            # agreement/contradiction without re-opening the images.
            "exterior_color_seen": (str(item.get("exterior_color") or "").strip() or None),
            "interior_color_seen": (str(item.get("interior_color") or "").strip() or None),
            "color_source": (str(item.get("color_source") or "").strip() or None),
            "vin_confirmed": bool(item.get("vin_confirmed")),
            # Persist the provenance, not just gate on it. Gating alone discards the
            # evidence: a later audit cannot tell a total that was READ from one that
            # was reconstructed, and every row looks equally unverified.
            "msrp_read_directly": bool(item.get("msrp_read_directly")),
            # Whether the document was a real factory Monroney or a dealer-generated
            # build sheet. Car 874954 recorded a perfectly transcribed $57,515 off a
            # sheet stamped "NOT ORIGINAL COPY", covering the base Ram ProMaster
            # chassis while the listing sells a Winnebago camper conversion -- 28%
            # below list, for a different vehicle. The agent DID caveat it, in notes,
            # where no consumer of sticker_msrp will ever look.
            "sticker_is_original": bool(item.get("sticker_is_original", True)),
            # Which document the total came from, derived from the flag above at the
            # moment of recording (2026-08-18 policy: build-sheet totals are stored,
            # not refused, so the document type must be first-class -- readers label
            # a Monroney and a dealer build sheet differently).
            "msrp_document": (
                "monroney"
                if item.get("sticker_is_original", True)
                else "dealer_build_sheet"
            ),
            "sticker_currency": currency,
            # How much of the total the itemized options FAIL to account for.
            #
            # Every guard here catches wrong values; this one measures a MISSING value.
            # On car 881850 an agent reported two option prices as "obscured by glare" --
            # one was plainly legible at $350 -- and silently dropped a $600 wheel option
            # entirely. Nothing flagged it, because an omission leaves no bad data behind.
            # It only surfaced when an auditor re-summed the sticker and found the lines
            # reconcile to the printed $60,215 ONLY with that $600 included.
            #
            # A gap is not proof of omission (destination is often folded into the total,
            # and agents legitimately withhold ambiguous prices), so this never blocks a
            # write. But an unexplained four-figure gap is a strong signal that the
            # equipment list undercounts the car, and until it is recorded nobody can even
            # ask the question.
            "options_gap": _options_gap(item.get("base_msrp"), priced, msrp),
            # Why a total was refused, kept so an audit can tell "never read" apart from
            # "read and rejected". Empty when the arithmetic held or did not apply.
            "msrp_reconcile_refused": reconcile_note or None,
            "base_msrp": item.get("base_msrp"),
            # NOT truncated. This was [:500], and 554 of 4,220 rows (13%) hit that
            # cutoff. On car 883394 the agent was mid-sentence explaining why the
            # total was ambiguous -- "...a Destination Charge line item is present
            # just above it but it" -- and the caveat was severed exactly there. The
            # surrounding code already warns that caveats in notes are invisible to
            # consumers of sticker_msrp; silently cutting them in half is worse.
            # A few KB of text is cheaper than one unexplained wrong price.
            "notes": str(item.get("notes") or ""),
            "images_read": int(item.get("images_read") or 0),
            "images_rejected": [],
            # Did this agent actually look? An agent whose Read calls all failed --
            # oversized photos, dead CDN URLs, a reset connection -- reports
            # has_window_sticker=false, which is byte-for-byte identical to "I looked at
            # this car and it has no Monroney". The first is a statement about our
            # tooling, the second about the vehicle, and collapsing them is precisely
            # what let a recheck pass delete 158 verified prices with a well-argued
            # reason attached to every one.
            #
            # So say which it was. Consumers that count "cars without a sticker" must
            # exclude these rather than treat them as evidence of absence.
            "assessed": int(item.get("images_read") or 0) > 0,
        }

        if not summary["assessed"]:
            _log.warning(
                "car %s: agent read 0 images -- recorded as UNASSESSED, not as "
                "'no sticker'", car_id,
            )

        cur.execute(
            """
            INSERT INTO car_image_text (
                car_id, vin, version, extracted_at, summary,
                images_seen, images_read, images_rejected_json,
                has_sticker, sticker_msrp, equipment_count
            ) VALUES (%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (car_id) DO UPDATE SET
                vin = COALESCE(EXCLUDED.vin, car_image_text.vin),
                version = EXCLUDED.version,
                extracted_at = NOW(),
                summary = EXCLUDED.summary,
                images_seen = EXCLUDED.images_seen,
                images_read = EXCLUDED.images_read,
                -- Merge, never clobber: an earlier local pass may hold a sticker MSRP
                -- recovered by column geometry that this reader did not see.
                has_sticker = EXCLUDED.has_sticker OR car_image_text.has_sticker,
                sticker_msrp = COALESCE(EXCLUDED.sticker_msrp, car_image_text.sticker_msrp),
                equipment_count = GREATEST(EXCLUDED.equipment_count,
                                           car_image_text.equipment_count)
            """,
            (
                car_id, vin, AGENT_VISION_VERSION, json.dumps(summary),
                int(item.get("gallery_size") or 0), int(item.get("images_read") or 0),
                json.dumps([]), bool(item.get("has_window_sticker")), msrp, len(equipment),
            ),
        )
        written += 1

    _log.info("recorded %d car(s)", written)

    # Scrub immediately rather than leaving it to a separate manual pass. Wave 8 wrote
    # a skewed $1,450 "Decoding additional functions" that sat in the table until the
    # audit found it, purely because the last scrub had run before that wave landed.
    # The check is corpus-wide and cheap, so there is no reason to defer it.
    class _ScrubArgs:
        dry_run = False
        min_corroboration = 3
        outlier_factor = 3.0
        min_unpriced = 8
        unpriced_min_price = 950.0

    cmd_scrub_skew(_ScrubArgs())
    return 0


def _norm_option_name(name: str) -> str:
    """
    Option name reduced to a form that matches across stickers.

    Corroboration only works if the same option is recognised as the same option, and
    exact-string matching quietly fails at that. "SiriusXM 1yr Trial Subscription" and
    "SiriusXM 1-year trial subscription" are the same line, but as raw strings they never
    meet -- so a $1,450 SiriusXM charge sat unchallenged against 95 cars that list it as
    standard, because the guard never saw them as related.

    Normalising folds case, punctuation, spacing and the year/yr/1-year variants together.
    """
    s = (name or "").lower()
    s = re.sub(r"\b(\d+)\s*-?\s*(?:yr|year)s?\b", r"\1 year", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def cmd_scrub_skew(args: argparse.Namespace) -> int:
    """
    Remove option prices that the corpus itself contradicts.

    The record-time guard matches a list of accessory words, which only catches skew when
    it lands on a small part. It missed "BMW Wheel Stud Locks" (the list had "wheel lock")
    and, more importantly, an entire class it was never going to catch: normally-free
    service lines. "Decoding additional functions" reads Included, or $125-$275, on 14
    cars in this corpus and $1,450-$1,550 on seven others -- destination-charge-sized, on
    a line that is usually free.

    So instead of guessing which names are cheap, ask the data. A given option name
    appears on many stickers; where one car's figure is a wild outlier against the median
    for that same name, the price column drifted. This needs no vocabulary and improves on
    its own as the corpus grows.

    Only the price is dropped; the option name is still a true observation.
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT car_id, summary FROM car_image_text "
        "WHERE version = %s AND summary IS NOT NULL",
        (AGENT_VISION_VERSION,),
    )
    rows = cur.fetchall()

    # Gather every price ever seen for each normalised option name, and separately how
    # often that same name appears as UNPRICED equipment.
    #
    # The unpriced tally catches a class the median cannot. Lines like "8-Speed Sport
    # Automatic Transmission", "4-zone automatic climate control" or "Soft-close automatic
    # doors" print as "Included" on almost every sticker, so they rarely appear in
    # priced_options at all -- there is no median to be an outlier against. When one car
    # shows $1,300 for a transmission that reads Included on 88 others, that is the same
    # column drift, and only the unpriced corpus reveals it.
    seen: dict[str, list[float]] = {}
    unpriced: dict[str, int] = {}
    parsed: list[tuple[int, dict[str, Any]]] = []
    for car_id, summary in rows:
        s = summary if isinstance(summary, dict) else json.loads(summary)
        parsed.append((car_id, s))
        for opt in s.get("priced_options") or []:
            name = _norm_option_name(str(opt.get("name") or ""))
            try:
                price = float(opt.get("price"))
            except (TypeError, ValueError):
                continue
            if name:
                seen.setdefault(name, []).append(price)
        for item in (s.get("equipment") or []) + (s.get("packages") or []):
            key = _norm_option_name(str(item))
            if key:
                unpriced[key] = unpriced.get(key, 0) + 1

    def _median(xs: list[float]) -> float:
        xs = sorted(xs)
        mid = len(xs) // 2
        return xs[mid] if len(xs) % 2 else (xs[mid - 1] + xs[mid]) / 2.0

    cleaned = dropped = exempt = 0
    for car_id, s in parsed:
        opts = s.get("priced_options") or []
        if reconciles_exactly(s):
            # The sticker corroborates itself. Leave every price alone.
            exempt += 1
            continue
        keep = []
        for opt in opts:
            name = _norm_option_name(str(opt.get("name") or ""))
            try:
                price = float(opt.get("price"))
            except (TypeError, ValueError):
                continue
            others = [p for p in seen.get(name, []) if p != price]
            # Need corroboration before overruling a reading.
            if len(others) >= args.min_corroboration:
                med = _median(others)
                if med > 0 and price > med * args.outlier_factor:
                    dropped += 1
                    continue
            # Priced here, but standard equipment nearly everywhere else.
            #
            # Bounded deliberately, and the bound has been raised twice.
            #
            # The rule fires on "usually included", which is true of many genuinely
            # optional items on SOME trims -- heated seats are standard on a loaded car and
            # a paid option on a base one. Unbounded it stripped every priced option from
            # car 888394. At a $600 floor it was still deleting real Monroney lines: car
            # 884372's "4-Zone Automatic Climate Control $650" and "Bowers & Wilkins
            # Surround Sound $950" were both genuinely printed, and both were discarded as
            # corpus outliers.
            #
            # What this rule actually exists to catch is a DESTINATION CHARGE bleeding onto
            # a standard-equipment row, and destination is a four-figure number. So the
            # floor now sits at the bottom of that band. Below it, a price on a
            # usually-included item is far more likely to be a real trim difference than a
            # misread, and deleting a true price has a real cost.
            elif (
                unpriced.get(name, 0) >= args.min_unpriced
                and price >= args.unpriced_min_price
            ):
                dropped += 1
                continue
            keep.append(opt)
        if len(keep) != len(opts):
            cleaned += 1
            s["priced_options"] = keep
            if not args.dry_run:
                cur.execute(
                    "UPDATE car_image_text SET summary = %s WHERE car_id = %s",
                    (json.dumps(s), car_id),
                )

    _log.info(
        "%s: %d car(s) affected, %d contradicted option price(s) removed "
        "(corroboration>=%d, outlier>%.1fx median)",
        "would clean" if args.dry_run else "cleaned",
        cleaned, dropped, args.min_corroboration, args.outlier_factor,
    )
    return 0


def cmd_requeue_unassessed(args: argparse.Namespace) -> int:
    """
    Return agent rows that assessed nothing to the claim pool.

    An agent whose image reads all failed (dead CDN URLs, oversized files, a
    delivery glitch) records `assessed: false` -- a statement about our tooling, not
    about the car. But `car_image_text.car_id` is the primary key and claim skips any
    existing row, so that car is permanently excluded from re-analysis unless its row
    is removed. The row holds no observation (no sticker verdict, no equipment, no
    colors), so deleting it loses nothing; the car simply becomes claimable again.
    """
    conn = _connect()
    cur = conn.cursor()
    sql = (
        "DELETE FROM car_image_text WHERE version = %s "
        "AND (summary->>'assessed') = 'false' RETURNING car_id"
    )
    if args.dry_run:
        cur.execute(
            "SELECT car_id FROM car_image_text WHERE version = %s "
            "AND (summary->>'assessed') = 'false'",
            (AGENT_VISION_VERSION,),
        )
        ids = [r[0] for r in cur.fetchall()]
        _log.info("would requeue %d unassessed car(s): %s", len(ids), ids[:20])
        return 0
    cur.execute(sql, (AGENT_VISION_VERSION,))
    ids = [r[0] for r in cur.fetchall()]
    _log.info("requeued %d unassessed car(s): %s", len(ids), ids[:20])
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT count(*) FROM cars WHERE listing_removed_at IS NULL "
        "AND gallery IS NOT NULL AND gallery NOT IN ('', '[]')"
    )
    total = cur.fetchone()[0]
    cur.execute(
        "SELECT count(*) FILTER (WHERE version = %s), "
        "       count(*) FILTER (WHERE version = %s), "
        "       count(*) FILTER (WHERE version > 0 AND version < %s), "
        "       count(sticker_msrp) FILTER (WHERE version = %s) "
        "FROM car_image_text",
        (AGENT_VISION_VERSION, _RESERVED, AGENT_VISION_VERSION, AGENT_VISION_VERSION),
    )
    agent_done, reserved, local_done, agent_msrps = cur.fetchone()
    _log.info("cars with a gallery ........ %d", total)
    _log.info("  analysed by agents ...... %d (%.1f%%)", agent_done, 100.0 * agent_done / max(1, total))
    _log.info("  analysed locally only ... %d", local_done)
    _log.info("  currently reserved ...... %d", reserved)
    _log.info("  MSRPs from agent vision . %d", agent_msrps)
    _log.info("  remaining ............... %d", total - agent_done - local_done)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    c = sub.add_parser("claim", help="reserve cars and download their images")
    c.add_argument("--count", type=int, default=8)
    c.add_argument("--images", type=int, default=8, help="images per car")
    c.add_argument("--out", required=True, help="scratch directory for this batch")
    c.add_argument("--delay", type=float, default=0.1)
    c.add_argument("--stale-minutes", type=int, default=90)
    c.add_argument("--pool-factor", type=int, default=80,
                   help="candidate pool = count * this. Must comfortably exceed "
                        "the demand of ALL concurrent agents, or they collide.")
    c.set_defaults(func=cmd_claim)

    r = sub.add_parser("record", help="merge an agent's findings")
    r.add_argument("--results", required=True)
    r.add_argument("--allow-unreserved", action="store_true",
                   help="write rows with no live reservation (re-runs only)")
    r.set_defaults(func=cmd_record)

    k = sub.add_parser("scrub-skew", help="drop option prices the corpus contradicts")
    k.add_argument("--dry-run", action="store_true")
    k.add_argument("--min-corroboration", type=int, default=3,
                   help="other cars that must name the same option before overruling")
    k.add_argument("--outlier-factor", type=float, default=3.0)
    k.add_argument("--unpriced-min-price", type=float, default=950.0,
                   help="only overrule a usually-included item above this price")
    k.add_argument("--min-unpriced", type=int, default=8,
                   help="cars that must list the item as standard equipment "
                        "before a price on it is treated as column drift")
    k.set_defaults(func=cmd_scrub_skew)

    s = sub.add_parser("status", help="how far the backfill has got")
    s.set_defaults(func=cmd_status)

    q = sub.add_parser("requeue-unassessed",
                       help="release cars whose reads all failed back into the pool")
    q.add_argument("--dry-run", action="store_true")
    q.set_defaults(func=cmd_requeue_unassessed)

    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
