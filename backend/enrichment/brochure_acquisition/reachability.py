"""
Per-brand reachability probe (--probe-reachability).

Moved verbatim out of ``backend/scripts/fetch_oem_brochures.py`` (audit
datascripts.md F7); that script is still the CLI and re-exports these names.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from backend.enrichment.brochure_sources import (
    BROWSER_USER_AGENT,
    IDENTIFIED_USER_AGENT,
    PROBE_TARGETS,
    PacedFetcher,
    ReachabilityResult,
    RobotsPolicy,
    probe_target,
)

_UA_CHOICES = {"browser": BROWSER_USER_AGENT, "identified": IDENTIFIED_USER_AGENT}


def probe_reachability(
    ua_labels: list[str], delay: float, brand: str | None
) -> list[ReachabilityResult]:
    targets = [t for t in PROBE_TARGETS if not brand or t[0] == brand.lower()]
    results: list[ReachabilityResult] = []
    for label in ua_labels:
        agent = _UA_CHOICES[label]
        fetcher = PacedFetcher(delay=delay, user_agent=agent)
        robots = RobotsPolicy(fetcher.get_text, agent=fetcher.robots_agent)
        for make, host, page_url in targets:
            result = probe_target(make, host, page_url, fetcher, robots, label)
            results.append(result)
            print(
                f"{label:<11} {make:<14} {result.host:<26} {result.verdict:<26} "
                f"blocked_by={result.blocked_by or '-':<19} "
                f"robots={result.robots_status:<8} http={result.page_status or '-':<5} "
                f"{result.detail[:90]}"
            )
    return results


def write_reachability_artifact(
    results: list[ReachabilityResult], path: Path
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "probes": [r.to_json() for r in results],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def report_reachability(args: argparse.Namespace, delay: float) -> int:
    """``--probe-reachability``: probe, print the verdict tables, write the artifact.

    The body of that branch of ``fetch_oem_brochures.main``, moved verbatim.
    """
    labels = ["browser", "identified"] if args.user_agent == "both" else [
        args.user_agent
    ]
    brand = (args.brand or [None])[0]
    print(f"Per-brand reachability (paced {delay:g}s, sequential)\n")
    results = probe_reachability(labels, delay, brand)
    by_verdict: dict[str, int] = {}
    for result in results:
        by_verdict[result.verdict] = by_verdict.get(result.verdict, 0) + 1
    print("\nVerdict counts:")
    for verdict, count in sorted(by_verdict.items(), key=lambda kv: -kv[1]):
        print(f"  {verdict:<28} {count}")

    # "We do not allow that host" and "that host will not answer us" are
    # different findings. Reporting them together is how an unfinished
    # allowlist gets written up as an OEM refusing us.
    print("\nWho declined:")
    for label, tag in (
        ("we did (host not allowlisted)", "our_allowlist"),
        ("they did (published robots rules)", "their_robots_rules"),
        ("they did (edge refuses us)", "their_edge"),
        ("nobody (probe completed)", ""),
    ):
        hosts = sorted({r.host for r in results if r.blocked_by == tag})
        if hosts:
            print(f"  {label:<36} {len(hosts):>2}  {', '.join(hosts)}")
    if not args.no_artifact:
        path = write_reachability_artifact(
            results, args.artifact_dir / "reachability.json"
        )
        print(f"\nreport -> {path}")
    return 0
