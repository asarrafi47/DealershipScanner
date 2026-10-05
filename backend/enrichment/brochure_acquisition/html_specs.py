"""
HTML specification pages (--html-specs, --rebuild-html-overlays, --reverify-html-specs).

Moved verbatim out of ``backend/scripts/fetch_oem_brochures.py`` (audit
datascripts.md F7); that script is still the CLI and re-exports these names.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from backend.enrichment.brochure_acquisition.paths import (
    _REPO,
)
from backend.enrichment.brochure_sources import (
    PacedFetcher,
    RobotsPolicy,
    sha256_bytes,
)
from backend.enrichment.dictionary_catalog import canonical_make
from backend.enrichment.dictionary_paths import (
    DERIVED_DIR,
    TRIM_ADDS_BY_YEAR_DIR,
)
from backend.enrichment.html_spec_sources import (
    HONDA_NEWSROOM_HOME,
    HTML_SPEC_MAKES,
    HTML_SPEC_PAGES_DIR,
    HTML_SPEC_TEXT_DIR,
    StoredPage,
    build_overlay,
    build_transcript,
    honda_channel_for_model,
    honda_model_channels,
    honda_spec_releases,
    honda_specs_tab_url,
    official_spec_page,
    pick_spec_release,
    read_grids,
    store_page,
    utc_now,
    verify_citation,
    verify_page_identity,
    write_transcript,
)
from backend.enrichment.html_spec_sources import (
    OVERLAY_SOURCE as HTML_SPEC_OVERLAY_SOURCE,
)
from backend.enrichment.html_spec_sources import (
    transcript_path as html_spec_transcript_path,
)

logger = logging.getLogger(__name__)


#
# Same lane, different locator. A make that publishes its per-trim equipment
# grid as an HTML table is not a make that "publishes nothing we can use" -- it
# is a make whose documents have no page numbers. Everything else is unchanged:
# the host allowlist, robots.txt, the pacing, the identity gate, the text
# quality gate, "store the bytes and verify against the stored bytes".
#
# See ``backend/enrichment/html_spec_sources`` for the citation contract and for
# what the extractor refuses to read.

#: One line per stored HTML spec page, alongside the PDF ledger.
HTML_SPEC_LOG_PATH = DERIVED_DIR / "html_spec_fetch_log.jsonl"

#: Where ``--html-specs`` writes the ladder overlays it builds.
#:
#: Deliberately NOT ``derived/trim_adds_by_year``. Two independent reasons, and
#: either alone would be enough:
#:
#:   * ``source="html_spec_quoted"`` is not in
#:     ``brochure_extract.ADMISSIBLE_OVERLAY_SOURCES``, so an overlay dropped
#:     into the live directory would be inert anyway -- and would sit there
#:     looking like coverage to anything that counts files;
#:   * a file in the live directory is loaded by the render path. A new
#:     producer writing straight into it is how an unreviewed store reaches a
#:     car page. Promoting these is a reviewed step: see the note in
#:     ``html_spec_sources`` and the hand-off in this lane's report.
HTML_SPEC_OVERLAY_DIR = DERIVED_DIR / "html_spec_overlays"

#: The live overlay directory, read ONLY to refuse to shadow a file already in
#: it. Nothing here writes to it.
LIVE_OVERLAY_DIR = TRIM_ADDS_BY_YEAR_DIR


def _html_spec_targets(
    gaps: list[dict[str, Any]], makes: set[str]
) -> list[dict[str, Any]]:
    """Gap entries whose make has a measured HTML discovery chain."""
    return [g for g in gaps if canonical_make(g["make"]).lower() in makes]


def _honda_release_for(
    fetcher: PacedFetcher,
    robots: RobotsPolicy,
    year: int,
    make: str,
    model: str,
    channel_cache: dict[str, str],
) -> tuple[str | None, str, list[str]]:
    """
    Walk Honda's three anchor hops to the spec release for this vehicle.

    Returns ``(release_url, refusal_reason, urls_visited)``. Every hop reads an
    href off the page before it; no slug is constructed. Robots is consulted for
    every URL, including the index hops.
    """
    visited: list[str] = []

    def get(url: str) -> tuple[int, str] | None:
        if not robots.allows(url):
            return None
        visited.append(url)
        try:
            return fetcher.get_text(url)
        except Exception as exc:  # noqa: BLE001 - transport failures are data
            logger.info("      fetch failed %s: %s", url, str(exc)[:120])
            return None

    home = channel_cache.get("__home__")
    if home is None:
        result = get(HONDA_NEWSROOM_HOME)
        if result is None or result[0] != 200:
            return None, "hondanews.com home page not served", visited
        home = result[1]
        channel_cache["__home__"] = home

    channels = honda_model_channels(home, HONDA_NEWSROOM_HOME)
    channel_url = honda_channel_for_model(channels, make, model)
    if not channel_url:
        return (
            None,
            f"no automobile channel anchored for {make} {model} "
            f"({len(channels)} channels on the home page)",
            visited,
        )

    specs_url = channel_cache.get(channel_url)
    if specs_url is None:
        result = get(channel_url)
        if result is None or result[0] != 200:
            return None, f"channel page not served: {channel_url}", visited
        found = honda_specs_tab_url(result[1], channel_url)
        if not found:
            return None, f"channel page anchors no Specs tab: {channel_url}", visited
        specs_url = found
        channel_cache[channel_url] = specs_url

    listing = channel_cache.get(f"listing::{specs_url}")
    if listing is None:
        result = get(specs_url)
        if result is None or result[0] != 200:
            return None, f"specs tab not served: {specs_url}", visited
        listing = result[1]
        channel_cache[f"listing::{specs_url}"] = listing

    releases = honda_spec_releases(listing, specs_url)
    release, reason = pick_spec_release(releases, year, make, model)
    if release is None:
        return None, f"{reason} ({len(releases)} releases listed)", visited
    return release.url, "", visited


def _held_html_spec(year: int, make: str, model: str) -> str | None:
    """The URL an already-stored page for this vehicle was fetched from."""
    path = html_spec_transcript_path(year, make, model)
    if not path.is_file():
        return None
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return str(blob.get("source_url") or "") or None


def _append_html_spec_log(record: dict[str, Any]) -> None:
    HTML_SPEC_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with HTML_SPEC_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def fetch_html_specs(
    gaps: list[dict[str, Any]],
    *,
    limit: int,
    delay: float,
    user_agent: str,
    download: bool,
) -> dict[str, Any]:
    """
    Discover, fetch, store, parse and cite official HTML spec pages.

    Counters returned are what the run measured, not what it attempted:
    ``fetched`` is pages whose bytes are on disk, ``parsed`` is pages that
    yielded at least one readable trim grid, ``refused`` is everything the gates
    turned away with a reason, ``verified`` is citations re-derived from the
    STORED bytes by :func:`html_spec_sources.verify_citation`.
    """
    fetcher = PacedFetcher(delay=delay, user_agent=user_agent)
    robots = RobotsPolicy(fetcher.get_text, agent=fetcher.robots_agent)
    channel_cache: dict[str, str] = {}

    stats: dict[str, Any] = {
        "considered": 0,
        "discovered": 0,
        "already_held": 0,
        "fetched": 0,  # HTTP 200 with a body
        "parsed": 0,  # at least one readable trim grid
        "stored": 0,  # bytes written to disk
        "refused": 0,
        "overlays": 0,
        "citations": 0,
        "verified": 0,
        "verify_failures": {},
        "refusals": [],
    }

    targets = _html_spec_targets(gaps, set(HTML_SPEC_MAKES))
    print(
        f"HTML spec pages: {len(targets)} gap groups on makes with a measured "
        f"discovery chain ({', '.join(sorted(HTML_SPEC_MAKES))}); "
        f"taking {min(limit, len(targets))}"
    )

    def refuse(gap: dict[str, Any], stage: str, reason: str) -> None:
        stats["refused"] += 1
        stats["refusals"].append(
            {"group_key": gap["group_key"], "stage": stage, "reason": reason}
        )
        print(f"    refused ({stage}): {reason}")

    for gap in targets[:limit]:
        stats["considered"] += 1
        year, make, model = gap["year"], gap["make"], gap["primary"]["model"]
        print(f"\n  {year} {make} {model}  ({gap['cars']} active cars)")

        release_url, reason, _visited = _honda_release_for(
            fetcher, robots, year, make, model, channel_cache
        )
        if not release_url:
            refuse(gap, "discovery", reason)
            continue
        if not official_spec_page(release_url, make):
            refuse(gap, "allowlist", f"{release_url} is not on an official host")
            continue
        if not robots.allows(release_url):
            verdict = robots.verdict_for(release_url)
            refuse(gap, "robots", f"{release_url}: {verdict}")
            continue
        stats["discovered"] += 1
        print(f"    release: {release_url}")

        # Resume: a page already stored FROM THIS URL is not re-requested. The
        # stored bytes are the citable substrate, so re-fetching would only risk
        # replacing them with a newer page that our citations do not address.
        held = _held_html_spec(year, make, model)
        if held == release_url:
            stats["already_held"] += 1
            print("    already held from this URL - not re-fetched")
            continue

        if not download:
            print("    dry run - not fetched")
            continue

        try:
            status, body = fetcher.get_text(release_url)
        except Exception as exc:  # noqa: BLE001
            refuse(gap, "fetch", f"{release_url}: {str(exc)[:160]}")
            continue
        if status != 200 or not body:
            refuse(gap, "fetch", f"{release_url}: HTTP {status}, {len(body or '')} bytes")
            continue
        stats["fetched"] += 1
        payload = body.encode("utf-8")
        retrieved_at = utc_now()

        grids, grid_refusals = read_grids(body)
        if not grids:
            refuse(
                gap,
                "parse",
                f"no readable trim grid ({len(payload)} bytes); "
                + "; ".join(grid_refusals[:3]),
            )
            continue
        stats["parsed"] += 1

        identified, why = verify_page_identity(body, grids, year, make, model)
        if not identified:
            refuse(gap, "identity", why)
            continue

        page = store_page(
            payload,
            year=year,
            make=make,
            model=model,
            url=release_url,
            retrieved_at=retrieved_at,
        )
        stats["stored"] += 1
        print(
            f"    stored {page.bytes:,} bytes sha256={page.sha256[:12]}… "
            f"-> {page.path.name}; {len(grids)} grid(s), "
            f"{sum(len(g.features) for g in grids)} feature rows"
        )

        transcript = build_transcript(page, grids)
        transcript_file = write_transcript(transcript)
        transcript_rel = str(transcript_file.resolve().relative_to(_REPO))

        overlay = build_overlay(page, grids, transcript_rel)
        if overlay is None:
            refuse(gap, "extract", "no trim column stated an add against its neighbour")
            _append_html_spec_log(
                {
                    "catalog_key": transcript["catalog_key"],
                    "year": year,
                    "make": make,
                    "model": model,
                    "source_url": release_url,
                    "fetched_at": retrieved_at,
                    "sha256": page.sha256,
                    "bytes": page.bytes,
                    "html_path": str(page.path.resolve().relative_to(_REPO)),
                    "transcript_path": transcript_rel,
                    "overlay_path": None,
                    "citations": 0,
                    "verified": 0,
                    "user_agent": user_agent,
                    "fetcher": "backend.scripts.fetch_oem_brochures --html-specs",
                }
            )
            continue

        # Verify EVERY citation against the bytes we just stored, before the
        # overlay is written. This re-parses the document from scratch; it does
        # not ask the extractor whether the extractor was right.
        stored_bytes = page.path.read_bytes()
        verdicts: dict[str, int] = {}
        for entries in overlay["adds_provenance"].values():
            for entry in entries:
                verdict = verify_citation(entry, stored_bytes)
                entry["verified"] = verdict == "verified"
                entry["citation_verification"] = {
                    "verdict": verdict,
                    "checked_at": utc_now(),
                    "checker": "backend.enrichment.html_spec_sources.verify_citation",
                }
                verdicts[verdict] = verdicts.get(verdict, 0) + 1
        total = sum(verdicts.values())
        good = verdicts.get("verified", 0)
        stats["citations"] += total
        stats["verified"] += good
        for verdict, count in verdicts.items():
            if verdict != "verified":
                stats["verify_failures"][verdict] = (
                    stats["verify_failures"].get(verdict, 0) + count
                )

        HTML_SPEC_OVERLAY_DIR.mkdir(parents=True, exist_ok=True)
        stem = transcript["catalog_key"].replace("|", "__")
        overlay_path = HTML_SPEC_OVERLAY_DIR / f"{stem}.json"
        live_path = LIVE_OVERLAY_DIR / f"{stem}.json"
        overlay["shadows_live_overlay"] = (
            _overlay_source_of(live_path) if live_path.is_file() else None
        )
        overlay_path.write_text(
            json.dumps(overlay, indent=1, ensure_ascii=False), encoding="utf-8"
        )
        overlay_written = str(overlay_path.resolve().relative_to(_REPO))
        stats["overlays"] += 1
        if overlay["shadows_live_overlay"]:
            stats.setdefault("shadowing", []).append(
                {"key": transcript["catalog_key"], "live_source": overlay["shadows_live_overlay"]}
            )
        print(
            f"    {total} citations, {good} verified against the stored bytes"
            + (f"; overlay -> {overlay_path.name}" if overlay_written else "")
        )

        _append_html_spec_log(
            {
                "catalog_key": transcript["catalog_key"],
                "year": year,
                "make": make,
                "model": model,
                "source_url": release_url,
                "fetched_at": retrieved_at,
                "sha256": page.sha256,
                "bytes": page.bytes,
                "html_path": str(page.path.resolve().relative_to(_REPO)),
                "transcript_path": transcript_rel,
                "overlay_path": overlay_written,
                "overlay_source": overlay["source"],
                "trims": overlay["trims_available"],
                "citations": total,
                "verified": good,
                "verify_failures": {k: v for k, v in verdicts.items() if k != "verified"},
                "user_agent": user_agent,
                "fetcher": "backend.scripts.fetch_oem_brochures --html-specs",
            }
        )

    return stats


def rebuild_html_spec_overlays() -> int:
    """
    Re-derive transcripts and overlays from the STORED bytes. No network.

    An extraction rule change has to be re-appliable to the documents we already
    hold, offline, or the corpus quietly ends up holding output from several
    generations of rule. Re-fetching would be the wrong fix twice over: it costs
    the host requests it does not owe us, and the page may have changed, in
    which case the new overlay would no longer be about the bytes our earlier
    citations addressed.
    """
    transcripts = sorted(HTML_SPEC_TEXT_DIR.glob("*.json"))
    if not transcripts:
        print(f"No transcripts under {HTML_SPEC_TEXT_DIR}")
        return 0

    rebuilt = 0
    citations = 0
    verified = 0
    dropped = 0
    for path in transcripts:
        blob = json.loads(path.read_text(encoding="utf-8"))
        html_path = _REPO / str(blob.get("source_html") or "")
        if not html_path.is_file():
            print(f"{path.name:<40} stored HTML missing at {blob.get('source_html')}")
            continue
        payload = html_path.read_bytes()
        if sha256_bytes(payload) != str(blob.get("source_html_sha256") or ""):
            print(f"{path.name:<40} stored HTML no longer hashes to the recorded sha256")
            continue
        body = payload.decode("utf-8", errors="replace")
        year, make, model = blob["year"], blob["make"], blob["model"]
        grids, _ = read_grids(body)
        identified, why = verify_page_identity(body, grids, year, make, model)
        if not identified:
            print(f"{path.name:<40} refused on re-read: {why}")
            continue

        page = StoredPage(
            year=year,
            make=make,
            model=model,
            url=str(blob.get("source_url") or ""),
            retrieved_at=str(blob.get("retrieved_at") or ""),
            sha256=sha256_bytes(payload),
            bytes=len(payload),
            path=html_path,
            title=str(blob.get("source_title") or ""),
        )
        write_transcript(build_transcript(page, grids))
        overlay_path = HTML_SPEC_OVERLAY_DIR / f"{path.stem}.json"
        overlay = build_overlay(page, grids, str(path.resolve().relative_to(_REPO)))
        if overlay is None:
            if overlay_path.is_file():
                overlay_path.unlink()
                dropped += 1
            print(f"{path.name:<40} no citable add under the current rule")
            continue

        counts: dict[str, int] = {}
        for entries in overlay["adds_provenance"].values():
            for entry in entries:
                verdict = verify_citation(entry, payload)
                entry["verified"] = verdict == "verified"
                entry["citation_verification"] = {
                    "verdict": verdict,
                    "checked_at": utc_now(),
                    "checker": "backend.enrichment.html_spec_sources.verify_citation",
                }
                counts[verdict] = counts.get(verdict, 0) + 1
        HTML_SPEC_OVERLAY_DIR.mkdir(parents=True, exist_ok=True)
        overlay_path.write_text(
            json.dumps(overlay, indent=1, ensure_ascii=False), encoding="utf-8"
        )
        rebuilt += 1
        total = sum(counts.values())
        good = counts.get("verified", 0)
        citations += total
        verified += good
        print(f"{path.name:<40} {total:>4} citations, {good:>4} verified")

    print(
        f"\nrebuilt {rebuilt} overlays from stored bytes ({dropped} dropped as "
        f"no longer citable); {citations} citations, {verified} verified"
    )
    return 0


def reverify_html_specs() -> int:
    """
    Re-derive every stored HTML citation from the stored bytes, and report.

    Reads the overlays back off disk rather than trusting the numbers the fetch
    printed, which is the same reason ``verify_trim_citations.py`` exists for the
    PDF lane: a run that certifies its own output has proved nothing.
    """
    overlays = [
        p
        for p in sorted(HTML_SPEC_OVERLAY_DIR.glob("*.json"))
        if _overlay_source_of(p) == HTML_SPEC_OVERLAY_SOURCE
    ]
    if not overlays:
        print(f"No {HTML_SPEC_OVERLAY_SOURCE} overlays under {HTML_SPEC_OVERLAY_DIR}")
        return 0

    totals: dict[str, int] = {}
    bytes_cache: dict[str, bytes] = {}
    for path in overlays:
        overlay = json.loads(path.read_text(encoding="utf-8"))
        stem = str(overlay.get("catalog_key") or path.stem).replace("|", "__")
        html_path = HTML_SPEC_PAGES_DIR / f"{stem}.html"
        payload = bytes_cache.get(stem)
        if payload is None:
            payload = html_path.read_bytes() if html_path.is_file() else b""
            bytes_cache[stem] = payload
        counts: dict[str, int] = {}
        for entries in (overlay.get("adds_provenance") or {}).values():
            for entry in entries if isinstance(entries, list) else []:
                verdict = verify_citation(entry, payload)
                counts[verdict] = counts.get(verdict, 0) + 1
                totals[verdict] = totals.get(verdict, 0) + 1
        summary = ", ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        print(f"{path.name:<44} {summary}")
    print("\nTotals:")
    for verdict, count in sorted(totals.items(), key=lambda kv: -kv[1]):
        print(f"  {verdict:<38} {count}")
    return 0


def _overlay_source_of(path: Path) -> str:
    try:
        blob = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    return str(blob.get("source") or "").strip().lower() if isinstance(blob, dict) else ""
