#!/usr/bin/env python3
"""Quick sanity checks for decode_trim_logic (run from project root: python scripts/verify_decode_trim.py)."""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from backend.knowledge_engine import decode_trim_logic  # noqa: E402


def check(name: str, make, model, trim, title, expect: dict) -> None:
    got = decode_trim_logic(make, model, trim, title)
    for k, v in expect.items():
        assert got.get(k) == v, f"{name}: {k} want {v!r} got {got.get(k)!r} (full={got})"
    print(f"OK: {name}")


def main() -> None:
    # 2026 BMW 330i xDrive — 4 cyl, AWD
    check(
        "2026 330i xDrive",
        "BMW",
        "3 Series",
        "330i xDrive",
        "2026 BMW 330i xDrive",
        {"cylinders": 4, "drivetrain": "AWD"},
    )
    # X5 xDrive40i — 6 + mild hybrid
    check(
        "X5 xDrive40i",
        "BMW",
        "X5",
        "xDrive40i",
        "2026 BMW X5 xDrive40i",
        {"cylinders": 6, "drivetrain": "AWD", "fuel_type_hint": "Gas / Mild Hybrid"},
    )
    # 5 Series + 40i
    check(
        "540i",
        "BMW",
        "5 Series",
        "540i",
        "2026 BMW 540i",
        {"cylinders": 6},
    )
    # M60i (xDrive often in full title; decoder needs xDrive/4MATIC text for AWD)
    check(
        "X7 M60i",
        "BMW",
        "X7",
        "M60i xDrive",
        "2026 BMW X7 M60i",
        {"cylinders": 8, "drivetrain": "AWD"},
    )
    # Mercedes E450 4MATIC
    check(
        "E450 4MATIC",
        "Mercedes-Benz",
        "E-Class",
        "E 450 4MATIC",
        "2026 Mercedes-Benz E 450 4MATIC",
        {"cylinders": 6, "drivetrain": "AWD"},
    )
    print("All decode_trim_logic checks passed.")


if __name__ == "__main__":
    main()
