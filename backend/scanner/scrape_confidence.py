"""Parse scanner stdout for scrape confidence markers."""

from __future__ import annotations

import json
import re
from typing import Any

_SCAN_CONFIDENCE_RE = re.compile(r"^SCAN_CONFIDENCE:(\{.*\})\s*$", re.MULTILINE)
_SCAN_VEHICLE_COUNT_RE = re.compile(r"^SCAN_VEHICLE_COUNT:(\d+)\s*$", re.MULTILINE)


def parse_scanner_stdout(stdout: str) -> dict[str, Any]:
    """Extract ``scrape_confidence`` and vehicle count from scanner.js output."""
    out: dict[str, Any] = {}
    text = stdout or ""
    for match in _SCAN_CONFIDENCE_RE.finditer(text):
        try:
            payload = json.loads(match.group(1))
            if isinstance(payload, dict):
                out["scrape_confidence"] = payload
        except json.JSONDecodeError:
            continue
    counts = _SCAN_VEHICLE_COUNT_RE.findall(text)
    if counts:
        out["vehicle_count"] = int(counts[-1])
    return out


def merge_result_with_scanner_stdout(result: dict[str, Any] | None, stdout: str) -> dict[str, Any]:
    merged = dict(result or {})
    parsed = parse_scanner_stdout(stdout)
    if parsed.get("scrape_confidence"):
        merged["scrape_confidence"] = parsed["scrape_confidence"]
    if parsed.get("vehicle_count") is not None and "vehicle_count" not in merged:
        merged["vehicle_count"] = parsed["vehicle_count"]
    return merged


def _vehicle_count_from_result(result: dict[str, Any]) -> int | None:
    sc = result.get("scrape_confidence")
    if not isinstance(sc, dict):
        sc = {}
    vc = result.get("vehicle_count")
    if vc is None:
        vc = sc.get("vehicle_count")
    try:
        return int(vc) if vc is not None else None
    except (TypeError, ValueError):
        return None


def soft_scrape_failure(job_type: str, result: dict[str, Any] | None) -> bool:
    """True when the process exited cleanly but inventory scrape did not succeed."""
    res = result or {}
    sc = res.get("scrape_confidence")
    if not isinstance(sc, dict):
        sc = {}
    level = (sc.get("level") or "").lower()
    vc = _vehicle_count_from_result(res)
    jt = (job_type or "").strip().lower()
    if jt == "onboard" and (vc == 0 or level == "low"):
        return True
    return vc == 0 and level == "low"


def job_display_status(status: str, job_type: str, result: dict[str, Any] | None) -> str:
    st = (status or "").strip().lower()
    if st == "done" and soft_scrape_failure(job_type, result):
        return "incomplete"
    return st or status or ""
