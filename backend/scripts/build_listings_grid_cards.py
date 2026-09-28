"""
Bring the persisted listings grid cards (``listings_grid_cards``) up to date.

    .venv/bin/python -m backend.scripts.build_listings_grid_cards

The radius-scoped ``GET /api/listings/cars`` serves stored card JSON so a metro of
~50k cars answers in well under a second; serializing those cards per request would
take ~40 s. The web process refreshes changed cards itself (inline when few, in the
background when many), but a store that is empty -- a fresh database, or a deploy
that changed the card serializer, which changes every card's revision -- would make
the first shopper in each metro pay the whole build. Run this in its own process
after such a deploy and after a full scan. It only re-serializes cards whose row,
attribution verdict, public-incomplete flag or serializer revision changed, or that
are older than a day, so a re-run on a current store is a read-only pass.
"""

from __future__ import annotations

import argparse
import sys
import time


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--batch", type=int, default=2000, help="cars per batch (default 2000)")
    args = parser.parse_args(argv)

    from dotenv import load_dotenv

    load_dotenv()
    from backend.db.repositories import grid_cards_repo as gc

    t0 = time.time()

    def progress(done: int, total: int, stats: dict) -> None:
        print(
            f"{done}/{total} cars  fresh={stats['fresh']} rebuilt={stats['rebuilt']}  "
            f"{time.time() - t0:.0f}s",
            flush=True,
        )

    stats = gc.build_all_cards(batch=max(100, args.batch), progress=progress)
    print(f"done in {time.time() - t0:.1f}s: {stats}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
