#!/usr/bin/env python3
"""
Fill missing trim overlays using web research + Claude.

Claude's API does not browse the internet by itself. This script uses the
existing ``WebResearcher`` (Brave Search + Playwright) to fetch trim comparison
pages, then asks Claude to extract ``adds_by_trim`` JSON — same shape as
``analyze_brochure_with_llm``.

Usage::

    python -m backend.scripts.analyze_trim_adds_with_web --limit 20
    python -m backend.scripts.analyze_trim_adds_with_web --year 2015 --make bmw
    python -m backend.scripts.analyze_trim_adds_with_web --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from backend.enrichment.dictionary_paths import (  # noqa: E402
    BROCHURE_TEXT_SLIM_DIR,
    TRIM_ADDS_BY_YEAR_DIR,
)
from backend.scripts.analyze_brochure_with_llm import (  # noqa: E402
    AnthropicAuthError,
    _CURATED_SOURCES,
    _MAX_BULLETS,
    _MIN_TEXT_CHARS,
    _MIN_TRIMS,
    _call_claude,
    _get_client,
    _parse_response,
    _write_overlay,
    brochure_ladder_id_for_ck,
    build_llm_user_prompt,
    discovered_trim_hierarchy_for_ck,
    prepare_slim_payload,
)
from backend.utils.project_env import load_project_dotenv  # noqa: E402
from backend.utils.web_researcher import WebResearcher  # noqa: E402

logger = logging.getLogger(__name__)

_SOURCE = "brochure_llm_web"
_MIN_WEB_CHARS = 80
_KNOWLEDGE_PROMPT = """\
List the factory trim lineup for the {year} {make} {model} sold in the United States.

Return JSON only:
{{
  "trims": ["MOST_LUXURIOUS", ..., "LEAST_LUXURIOUS"],
  "adds": {{
    "TRIM_NAME": ["specific feature or package delta", ...]
  }}
}}

Rules:
- Use real US-market trim names for that model year.
- For each trim above the base, list concrete features/packages it adds vs the trim below.
- For the base trim, list key standard equipment.
- Each bullet 5–25 words, specific (engine, screen size, safety package, wheels, leather, etc.).
- Max {max_bullets} bullets per trim.
- If uncertain about a trim, omit it rather than inventing.
"""


def _overlay_catalog_keys() -> set[str]:
    keys: set[str] = set()
    for path in TRIM_ADDS_BY_YEAR_DIR.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        ck = str(data.get("catalog_key") or "").strip()
        if ck:
            keys.add(ck)
    return keys


def _existing_blocks(ck: str, *, overwrite: bool) -> str | None:
    path = TRIM_ADDS_BY_YEAR_DIR / f"{ck.replace('|', '__')}.json"
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    src = str(data.get("source") or "").lower()
    if any(curated in src for curated in _CURATED_SOURCES):
        return "skipped:curated"
    if src == _SOURCE and not overwrite:
        return "skipped:already_done"
    if src in {"brochure_llm", "promoted_brochure_auto"} and not overwrite:
        return "skipped:has_overlay"
    return None


def _web_text_for_ymm(year: int, make: str, model: str, *, researcher: WebResearcher) -> tuple[str, str] | None:
    """Return (text, source_url) from direct guides or search."""
    query = f"{year} {make} {model} trim comparison features standard equipment"
    result = researcher.search_and_summarize(query, year=year, make=make, model=model)
    if result and len(result.text.strip()) >= _MIN_WEB_CHARS:
        header = f"Source: {result.url}\nTitle: {result.title}\n\n"
        return header + result.text.strip(), result.url
    return None


def _brochure_text_for_ymm(meta: dict[str, Any]) -> str:
    text = str(meta.get("combined_trim_pages_text") or "").strip()
    if len(text) >= _MIN_TEXT_CHARS:
        return prepare_slim_payload(text)
    return ""


def _claude_extract(
    client: Any,
    *,
    catalog_key: str,
    year: int,
    make: str,
    model: str,
    text: str,
    source_note: str,
) -> dict[str, Any] | None:
    trim_hierarchy = discovered_trim_hierarchy_for_ck(catalog_key)
    if not trim_hierarchy:
        return None
    prompt = build_llm_user_prompt(
        year=year,
        make=make,
        model=model,
        slim_text=text,
        trim_hierarchy=trim_hierarchy,
        max_bullets=_MAX_BULLETS,
    )
    prompt += f"\n\n{source_note}"
    raw = _call_claude(client, prompt)
    return _parse_response(raw, expected_hierarchy=trim_hierarchy)


def _claude_knowledge_extract(
    client: Any,
    *,
    year: int,
    make: str,
    model: str,
) -> dict[str, Any] | None:
    prompt = _KNOWLEDGE_PROMPT.format(
        year=year,
        make=make,
        model=model,
        max_bullets=_MAX_BULLETS,
    )
    raw = _call_claude(client, prompt)
    return _parse_response(raw)


def analyze_one_web(
    meta: dict[str, Any],
    client: Any,
    *,
    researcher: WebResearcher,
    dry_run: bool = False,
    overwrite: bool = False,
    knowledge_fallback: bool = True,
) -> str:
    ck = str(meta.get("catalog_key") or "")
    year = int(meta.get("year") or 0)
    make = str(meta.get("make") or "").strip()
    model = str(meta.get("model") or "").strip()
    if not ck or not year or not make or not model:
        return "empty:missing_meta"

    skip = _existing_blocks(ck, overwrite=overwrite)
    if skip:
        return skip

    if dry_run:
        return "dry_run"

    parsed: dict[str, Any] | None = None
    source_url = ""
    source_tag = _SOURCE

    fetched = _web_text_for_ymm(year, make, model, researcher=researcher)
    if fetched:
        text, source_url = fetched
        try:
            parsed = _claude_extract(
                client,
                catalog_key=ck,
                year=year,
                make=make,
                model=model,
                text=text,
                source_note=(
                    "Note: Text scraped from the public web. Prefer OEM facts when conflicts appear. "
                    f"Research URL: {source_url}"
                ),
            )
        except AnthropicAuthError:
            raise
        except Exception as e:
            return f"error:api:{e}"

    if not parsed:
        brochure = _brochure_text_for_ymm(meta)
        if brochure:
            try:
                parsed = _claude_extract(
                    client,
                    catalog_key=ck,
                    year=year,
                    make=make,
                    model=model,
                    text=brochure,
                    source_note="Note: Text from stored OEM brochure extract (no live web page).",
                )
                source_tag = "brochure_llm"
            except AnthropicAuthError:
                raise
            except Exception as e:
                return f"error:api:{e}"

    if not parsed and knowledge_fallback:
        try:
            parsed = _claude_knowledge_extract(client, year=year, make=make, model=model)
            source_tag = "brochure_llm_knowledge"
        except AnthropicAuthError:
            raise
        except Exception as e:
            return f"error:api:{e}"

    if not parsed or len(parsed.get("trims") or []) < _MIN_TRIMS:
        return "empty:parse_failed"

    ladder_id = brochure_ladder_id_for_ck(ck) or f"brochure_web_{ck.replace('|', '__')}"
    out = _write_overlay(
        ck,
        year,
        make,
        model,
        parsed,
        ladder_id=ladder_id,
        source=source_tag,
    )
    if source_url:
        try:
            payload = json.loads(out.read_text(encoding="utf-8"))
            payload["web_source_url"] = source_url
            out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        except (OSError, json.JSONDecodeError):
            pass
    return f"written:{out.name}"


def _load_targets(
    *,
    year: int,
    make: str,
    missing_only: bool,
) -> list[dict[str, Any]]:
    have = _overlay_catalog_keys() if missing_only else set()
    out: list[dict[str, Any]] = []
    for path in sorted(BROCHURE_TEXT_SLIM_DIR.glob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            meta = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        ck = str(meta.get("catalog_key") or "")
        y = int(meta.get("year") or 0)
        mk = str(meta.get("make") or "").strip()
        if year and y != year:
            continue
        if make and make.lower().replace(" ", "") not in mk.lower().replace(" ", ""):
            continue
        if missing_only and ck in have:
            continue
        out.append(meta)
    return out


def main() -> int:
    load_project_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Web research + Claude trim overlay extraction.")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--year", type=int, default=0)
    parser.add_argument("--make", default="")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Process every slim YMM, not only those missing overlays",
    )
    parser.add_argument(
        "--no-knowledge-fallback",
        action="store_true",
        help="Do not ask Claude from model knowledge when web/brochure fail",
    )
    args = parser.parse_args()

    targets = _load_targets(
        year=args.year,
        make=args.make,
        missing_only=not args.all,
    )
    if args.limit:
        targets = targets[: args.limit]

    if not targets:
        print("No targets to process.")
        return 0

    client = None
    if not args.dry_run:
        try:
            client = _get_client()
        except RuntimeError as e:
            print(f"Error: {e}")
            return 1

    researcher = WebResearcher(max_text_chars=8000, timeout_ms=30_000)
    stats: dict[str, int] = {}

    for i, meta in enumerate(targets):
        ck = str(meta.get("catalog_key") or "")
        try:
            status = analyze_one_web(
                meta,
                client,
                researcher=researcher,
                dry_run=args.dry_run,
                overwrite=args.overwrite,
                knowledge_fallback=not args.no_knowledge_fallback,
            )
        except AnthropicAuthError as e:
            logger.error("Anthropic auth failed at %d/%d — stopping.", i + 1, len(targets))
            print(f"Error: {e}")
            stats["error:api_auth"] = stats.get("error:api_auth", 0) + 1
            break

        key = status.split(":")[0]
        stats[key] = stats.get(key, 0) + 1
        label = ck.replace("|", "__")
        if not status.startswith("skipped"):
            logger.info("[%d/%d] %s → %s", i + 1, len(targets), label, status)
        elif (i + 1) % 50 == 0:
            logger.info("[%d/%d] …", i + 1, len(targets))

    print("\nDone.")
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
