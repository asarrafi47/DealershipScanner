#!/usr/bin/env python3
"""Read JSON array of vehicle dicts from stdin; merge analytics ep blobs; write JSON to stdout."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.utils.analytics_ep import merge_ep_batch  # noqa: E402


def main() -> None:
    data = json.load(sys.stdin)
    if not isinstance(data, list):
        raise SystemExit("stdin must be a JSON array")
    out = merge_ep_batch(data)
    json.dump(out, sys.stdout)


if __name__ == "__main__":
    main()
