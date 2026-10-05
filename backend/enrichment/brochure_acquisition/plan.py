"""
The default mode of ``backend/scripts/fetch_oem_brochures.py``: plan, then download.

Gap list -> filter by --brand/--year/--min-cars/--supported-only -> gap artifact
-> (``--html-specs``: the HTML specification lane, then stop) -> resolve each
selected gap OEM tier first (archive only with ``--allow-archive``) -> dry-run
report, or with ``--download`` fetch + extract + ledger.

Moved verbatim out of that script's ``main()`` (audit datascripts.md F7); only
the indentation changed. The script keeps argparse and the mode dispatch.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from backend.enrichment.brochure_acquisition.corpus import open_content_index
from backend.enrichment.brochure_acquisition.download import download_and_extract
from backend.enrichment.brochure_acquisition.gaps import (
    load_gap_list,
    write_gap_artifact,
)
from backend.enrichment.brochure_acquisition.html_specs import fetch_html_specs
from backend.enrichment.brochure_acquisition.paths import (
    _REPO,
    CONTENT_INDEX_PATH,
    FETCH_LOG_PATH,
)
from backend.enrichment.brochure_acquisition.reachability import _UA_CHOICES
from backend.enrichment.brochure_acquisition.sources import (
    ArchiveResolver,
    resolve_source,
)
from backend.enrichment.brochure_sources import (
    ARCHIVE_SOURCE_NOTE,
    DISCOVERY_UNRESOLVED,
    HOST_ACCESS_NOTES,
    TIER_OEM,
    VERIFIED_MAKES,
    PacedFetcher,
    RobotsPolicy,
)
from backend.enrichment.dictionary_paths import BROCHURE_TEXT_DIR
from backend.enrichment.html_spec_sources import (
    REFUSED_BY_DESIGN as HTML_SPEC_REFUSED_BY_DESIGN,
)


def paced_delay(args: argparse.Namespace) -> float:
    """``--delay`` floored at 3s, for every mode that makes requests."""
    # Pacing is not negotiable: bursts get this project soft-blocked.
    return max(3.0, args.delay)


def plan_and_download(args: argparse.Namespace) -> int:
    """Everything ``main()`` does when no single-purpose mode flag was given."""
    delay = paced_delay(args)

    gaps, stats = load_gap_list()
    print(
        f"corpus files={stats['corpus_files']}  active cars={stats['active_cars']}\n"
        f"distinct vehicles={stats['distinct_vehicles']} "
        f"(raw catalog_key combos={stats['raw_catalog_key_combos']})\n"
        f"covered vehicles={stats['covered_vehicles']}  "
        f"uncovered vehicles={stats['uncovered_vehicles']}\n"
        f"covered cars={stats['covered_cars']} "
        f"({stats['covered_cars'] / max(1, stats['active_cars']):.1%})  "
        f"gap cars={stats['gap_cars']}\n"
        f"alias-only catalog keys (document held, lookup key missing)="
        f"{stats['alias_only_keys']} covering {stats['alias_only_cars']} cars"
    )

    wanted_brands = {b.lower() for b in (args.brand or [])}
    wanted_years = set(args.year or [])
    filtered = [
        gap
        for gap in gaps
        if gap["cars"] >= args.min_cars
        and (not wanted_brands or gap["make"].lower() in wanted_brands)
        and (not wanted_years or gap["year"] in wanted_years)
        and (not args.supported_only or not gap["unsupported_reason"])
    ]

    if not args.no_artifact:
        path = write_gap_artifact(
            filtered,
            stats,
            args.artifact_dir / (
                f"gap_list_{'_'.join(sorted(wanted_brands))}.json"
                if wanted_brands
                else "gap_list.json"
            ),
            ",".join(sorted(wanted_brands)) or None,
        )
        print(f"\ngap artifact ({len(filtered)} entries) -> {path}")

    if args.show_gaps:
        print(f"\nTop {args.show_gaps} gaps by active cars:")
        for gap in filtered[: args.show_gaps]:
            flag = "-" if gap["unsupported_reason"] else "OK"
            spellings = (
                f"  [{len(gap['variants'])} spellings]" if len(gap["variants"]) > 1 else ""
            )
            print(
                f"{gap['cars']:6d}  {flag:<3} {gap['year']} {gap['make']} "
                f"{gap['model']}{spellings}"
            )

    if args.html_specs:
        return html_specs_mode(filtered, args, delay)


    selected = filtered[: args.limit]
    index, duplicates = open_content_index()
    if duplicates:
        print(
            f"\ncontent index: {len(index.by_sha)} distinct documents, "
            f"{len(duplicates)} stored more than once (see --audit-corpus)"
        )

    print(
        f"\n{'DOWNLOAD' if args.download else 'DRY RUN (default) - nothing will be written'}"
        f": considering {len(selected)} gaps"
        f" covering {sum(g['cars'] for g in selected)} active cars\n"
    )

    user_agent = _UA_CHOICES["browser" if args.user_agent == "both" else args.user_agent]
    fetcher = PacedFetcher.for_tier(TIER_OEM, delay=delay, user_agent=user_agent)
    robots = RobotsPolicy(fetcher.get_text, agent=fetcher.robots_agent)

    archive: ArchiveResolver | None = None
    if args.allow_archive:
        archive = ArchiveResolver(delay=delay, user_agent=user_agent)
        print(
            f"archive tier ENABLED as a fallback (paced {archive.fetcher.delay:g}s vs "
            f"{fetcher.delay:g}s for OEM)\n  {ARCHIVE_SOURCE_NOTE}\n"
        )

    counts: dict[str, int] = {}
    by_tier: dict[str, int] = {}
    fetched = 0
    would_fetch = 0
    page_cache: dict[str, tuple[int, str]] = {}

    for gap in selected:
        resolved = resolve_source(gap, fetcher, robots, page_cache, archive=archive)
        counts[resolved["status"]] = counts.get(resolved["status"], 0) + 1

        label = f"{gap['cars']:6d}  {gap['year']} {gap['make']} {gap['model']}"
        if resolved["status"] != "found":
            extra = resolved.get("archive_detail")
            print(
                f"{label}\n         SKIP [{resolved['status']}] {resolved['detail']}"
                + (f"\n           archive: {extra}" if extra else "")
            )
            continue
        by_tier[resolved["tier"]] = by_tier.get(resolved["tier"], 0) + 1

        target = BROCHURE_TEXT_DIR / f"{gap['primary_catalog_key']}.json"

        tier_tag = f"[{resolved['tier']}]"
        if not args.download:
            would_fetch += 1
            print(
                f"{label}\n"
                f"         WOULD FETCH {tier_tag} {resolved['doc_url']}\n"
                f"           via {resolved['discovery_url']}  ({resolved['detail']})\n"
                + (
                    f"           OEM tier first said: {resolved['oem_refusal']}\n"
                    if resolved.get("oem_refusal")
                    else ""
                )
                + f"           -> {target.relative_to(_REPO)}"
            )
            continue

        outcome = download_and_extract(
            resolved, fetcher, robots, index, archive=archive
        )
        counts[outcome["status"]] = counts.get(outcome["status"], 0) + 1
        if outcome["status"] == "fetched":
            fetched += 1
            authenticity = outcome.get("authenticity") or {}
            comparison = authenticity.get("oem_comparison") or {}
            print(
                f"{label}\n"
                f"         FETCHED {tier_tag} {outcome['doc_url']}\n"
                f"           {outcome['bytes']} bytes, {outcome['page_count']} pages, "
                f"{outcome['pages_saved']} saved, {outcome['text_chars']} chars, "
                f"sha256 {outcome['sha256'][:12]}\n"
                + (
                    f"           producer={authenticity.get('pdf_metadata', {}).get('producer')!r} "
                    f"text_layer={authenticity.get('pages_with_text')}/"
                    f"{authenticity.get('page_count')}p"
                    + (f" vs OEM: {comparison.get('outcome')}" if comparison else "")
                    + "\n"
                    if authenticity
                    else ""
                )
                + f"           -> {target.name}"
            )
        elif outcome["status"] in ("duplicate_content", "already_downloaded"):
            print(f"{label}\n         DEDUPED [{outcome['status']}] {outcome['detail']}")
        elif outcome["status"] == "archive_quarantined":
            print(
                f"{label}\n         ARCHIVE QUARANTINED {outcome['detail']}\n"
                f"           kept at {outcome['pdf_path']}"
            )
        elif outcome["status"] == "stored_text_rejected":
            print(
                f"{label}\n         PDF STORED, TEXT REJECTED {outcome['detail']}\n"
                f"           kept at {Path(outcome['pdf_path']).name}"
            )
        else:
            print(f"{label}\n         FAIL [{outcome['status']}] {outcome['detail']}")

    print("\nOutcome counts:")
    for status, count in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {status:<28} {count}")
    if by_tier:
        print("\nResolved by tier (OEM is always tried first):")
        for tier, count in sorted(by_tier.items()):
            print(f"  {tier:<28} {count}")
    if archive is not None:
        print(
            f"\narchive requests this run: {archive.fetcher.request_count} "
            f"at a {archive.fetcher.delay:g}s floor"
        )
    if args.download:
        print(f"\nfetched {fetched} new document(s); provenance -> {FETCH_LOG_PATH}")
        print(f"content index -> {CONTENT_INDEX_PATH}")
    else:
        print(f"\nwould fetch {would_fetch} document(s); re-run with --download")

    print(f"\nmakes verified end to end: {', '.join(sorted(VERIFIED_MAKES))}")
    print("makes with an official host but an unresolved discovery step:")
    for make, reason in sorted(DISCOVERY_UNRESOLVED.items()):
        print(f"  {make:<12} {reason}")
    if HOST_ACCESS_NOTES:
        print("\nhosts measured as refusing automated access (robots.txt unreadable):")
        for host, note in sorted(HOST_ACCESS_NOTES.items()):
            print(f"  {host:<22} {note}")
    return 0


def html_specs_mode(
    filtered: list[dict], args: argparse.Namespace, delay: float
) -> int:
    """``--html-specs``: run the HTML specification lane over the filtered gaps."""
    html_stats = fetch_html_specs(
        filtered,
        limit=args.limit,
        delay=delay,
        user_agent=_UA_CHOICES[
            "browser" if args.user_agent == "both" else args.user_agent
        ],
        download=args.download,
    )
    print(
        "\nHTML spec pages: "
        f"considered={html_stats['considered']} "
        f"discovered={html_stats['discovered']} "
        f"already held={html_stats['already_held']} "
        f"fetched={html_stats['fetched']} parsed={html_stats['parsed']} "
        f"stored={html_stats['stored']} "
        f"refused={html_stats['refused']} overlays={html_stats['overlays']}\n"
        f"citations={html_stats['citations']} "
        f"verified against stored bytes={html_stats['verified']}"
    )
    if html_stats["verify_failures"]:
        print("verification failures:")
        for verdict, count in sorted(
            html_stats["verify_failures"].items(), key=lambda kv: -kv[1]
        ):
            print(f"  {verdict:<38} {count}")
    if html_stats["refusals"]:
        print("\nrefusals:")
        for row in html_stats["refusals"]:
            print(f"  {row['group_key']:<28} {row['stage']:<11} {row['reason'][:110]}")
    print("\nnot read out of these pages, by design:")
    for name, why in HTML_SPEC_REFUSED_BY_DESIGN.items():
        print(f"  {name:<16} {why}")
    if not args.no_artifact and args.download:
        path = args.artifact_dir / "html_spec_run.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {"generated_at": datetime.now(timezone.utc).isoformat(), **html_stats},
                indent=2,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        print(f"\nreport -> {path}")
    return 0
