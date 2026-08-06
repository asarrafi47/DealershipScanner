"""Paced HTTP fetching."""
from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

from .agents import (
    DEFAULT_DELAY_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    USER_AGENT,
    robots_agent_for,
)
from .tiers import (
    ARCHIVE_DELAY_SECONDS,
    TIER_ARCHIVE,
)

# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------


class PacedFetcher:
    """
    Sequential HTTP client with a fixed minimum gap between requests.

    Deliberately has no concurrency: bursts get this project soft-blocked.
    """

    def __init__(
        self,
        *,
        delay: float = DEFAULT_DELAY_SECONDS,
        user_agent: str = USER_AGENT,
        session=None,
        sleep=time.sleep,
        clock=time.monotonic,
    ) -> None:
        self.delay = delay
        self.user_agent = user_agent
        self.robots_agent = robots_agent_for(user_agent)
        self._sleep = sleep
        self._clock = clock
        self._last = 0.0
        self.request_count = 0
        if session is None:
            import requests

            session = requests.Session()
        self._session = session

    @classmethod
    def for_tier(cls, tier: str, *, delay: float = DEFAULT_DELAY_SECONDS, **kwargs):
        """
        A fetcher paced for ``tier``.

        The archive floor is :data:`ARCHIVE_DELAY_SECONDS` and it is a floor, not
        a default: ``--delay 3`` cannot make an archive crawl faster than the OEM
        one. ``max`` also means a caller asking for something slower still gets
        it.
        """
        if tier == TIER_ARCHIVE:
            delay = max(delay, ARCHIVE_DELAY_SECONDS)
        return cls(delay=delay, **kwargs)

    def _pace(self) -> None:
        if self.request_count:
            elapsed = self._clock() - self._last
            if elapsed < self.delay:
                self._sleep(self.delay - elapsed)
        self._last = self._clock()
        self.request_count += 1

    def get_text(self, url: str) -> tuple[int, str]:
        self._pace()
        resp = self._session.get(
            url,
            headers={"User-Agent": self.user_agent, "Accept": "text/html,*/*"},
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=True,
        )
        return resp.status_code, resp.text

    def get_bytes(self, url: str) -> tuple[int, bytes, str]:
        self._pace()
        resp = self._session.get(
            url,
            headers={"User-Agent": self.user_agent, "Accept": "application/pdf,*/*"},
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=True,
        )
        return (
            resp.status_code,
            resp.content,
            resp.headers.get("Content-Type", ""),
        )


def looks_like_pdf(payload: bytes) -> bool:
    """True if the bytes actually begin with a PDF header."""
    return bool(payload) and payload[:5] == b"%PDF-"
