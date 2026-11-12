"""Scan-loop configuration: defaults, ceilings, fixed limits and the loader of ``config/loop.json``.

Pure and standard library only: no environment read, no clock, no file read. The caller passes the
raw text of the control file (or ``None`` when it is missing). The Railway tick enforces the budgets
and the cloud routine reads ``cloud_run``, so no cap lives only on one side. A control file that
cannot be read or trusted means the loop idles (``enabled=False``), never "unlimited".
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field

KINDS = ("probe", "validate", "rescan", "apply")

# Constants the data repository cannot change.
MAX_REQUEST_BYTES = 4096
MAX_PROPOSAL_BYTES = 33792
MAX_STATUS_BYTES = 16384
MAX_CONFIG_BYTES = 1024
MAX_DEALERS_PER_REQUEST = 8
MAX_READ_FILES = 50
REASON_MAX_CHARS = 160
DIGEST_MAX_BYTES = 6144
DEALER_ID_PATTERN = r"^[a-z0-9][a-z0-9_-]{0,79}$"
DEALER_ID_RE = re.compile(DEALER_ID_PATTERN)

# Environment variable names (values are read by the wrappers, never here).
ENV_SCAN_MODE = "SCAN_MODE"
ENV_DATA_REPO = "LOOP_DATA_REPO"
ENV_DATA_TOKEN = "LOOP_DATA_TOKEN"
ENV_DATA_DIR = "LOOP_DATA_DIR"
ENV_TARGET = "LOOP_TARGET"
ENV_PUBLISH_SHARD_LOGS = "LOOP_PUBLISH_SHARD_LOGS"
ENV_PUBLISH_RECIPES_FULL = "LOOP_PUBLISH_RECIPES_FULL"
ENV_FLEET_WINDOW_UTC = "SCAN_FLEET_WINDOW_UTC"


@dataclass(frozen=True)
class CloudRun:
    """Limits read by cloud sessions and routines; Railway does not enforce them."""

    dealers: int = 8
    agents: int = 20
    max_fixes: int = 4
    workflows_at_once: int = 1
    routines: int = 3
    weekly_share_stop: float = 0.6


@dataclass(frozen=True)
class LoopConfig:
    enabled: bool = True
    kinds: tuple = KINDS
    dealers_per_cycle: int = 6
    cycles_per_day: int = 4
    attempts_per_dealer: int = 3
    applies_per_dealer_per_day: int = 1
    cloud_run: CloudRun = field(default_factory=CloudRun)
    stale_after_hours: int = 36

    @property
    def idle(self) -> bool:
        return not self.enabled


DEFAULTS = LoopConfig()

# Twice the default of the four top-level numbers the Railway tick enforces.
CEILINGS = {
    "dealers_per_cycle": 12,
    "cycles_per_day": 8,
    "attempts_per_dealer": 6,
    "applies_per_dealer_per_day": 2,
}

_CLOUD_INT_KEYS = ("dealers", "agents", "max_fixes", "workflows_at_once", "routines")


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, float) and math.isfinite(value)


def _idle_config() -> LoopConfig:
    return LoopConfig(enabled=False)


def _reject_constant(name):
    raise ValueError("non-finite number")


def load_loop_config(raw) -> LoopConfig:
    """Parse the text (or bytes) of ``config/loop.json``; anything unusable gives ``enabled=False``."""
    if raw is None:
        return _idle_config()
    try:
        if isinstance(raw, str):
            data_bytes = raw.encode("utf-8")
        elif isinstance(raw, (bytes, bytearray)):
            data_bytes = bytes(raw)
        else:
            return _idle_config()
        if len(data_bytes) > MAX_CONFIG_BYTES:
            return _idle_config()
        obj = json.loads(data_bytes.decode("utf-8"), parse_constant=_reject_constant)
    except (ValueError, UnicodeError, RecursionError):
        return _idle_config()
    if not isinstance(obj, dict):
        return _idle_config()

    values = {}
    if "enabled" in obj:
        if not isinstance(obj["enabled"], bool):
            return _idle_config()
        values["enabled"] = obj["enabled"]

    if "kinds" in obj:
        kinds = obj["kinds"]
        if not isinstance(kinds, list) or any(not isinstance(k, str) or k not in KINDS for k in kinds):
            return _idle_config()
        values["kinds"] = tuple(k for k in KINDS if k in kinds)

    for key, ceiling in CEILINGS.items():
        if key in obj:
            if not _is_int(obj[key]):
                return _idle_config()
            values[key] = max(0, min(obj[key], ceiling))

    if "stale_after_hours" in obj:
        if not _is_int(obj["stale_after_hours"]):
            return _idle_config()
        values["stale_after_hours"] = max(0, obj["stale_after_hours"])

    if "cloud_run" in obj:
        cloud = obj["cloud_run"]
        if not isinstance(cloud, dict):
            return _idle_config()
        cloud_values = {}
        for key in _CLOUD_INT_KEYS:
            if key in cloud:
                if not _is_int(cloud[key]):
                    return _idle_config()
                cloud_values[key] = max(0, cloud[key])
        if "weekly_share_stop" in cloud:
            if not _is_number(cloud["weekly_share_stop"]):
                return _idle_config()
            cloud_values["weekly_share_stop"] = min(1.0, max(0.0, float(cloud["weekly_share_stop"])))
        values["cloud_run"] = CloudRun(**cloud_values)

    return LoopConfig(**values)
