"""Cooperative stop flag for SIGINT / long crawls."""
from __future__ import annotations

import logging
import signal
from typing import Any

_stop_requested = False


def stop_requested() -> bool:
    return _stop_requested


def _set_stop() -> None:
    global _stop_requested
    _stop_requested = True


def install_sigint_handler(logger: logging.Logger) -> None:
    def _handler(signum: Any, frame: Any) -> None:
        _set_stop()
        logger.warning(
            "Interrupt received — will stop after current site (press again to force quit)"
        )

    signal.signal(signal.SIGINT, _handler)
