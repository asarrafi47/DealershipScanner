"""robots.txt fetch, cache and verdicts."""
from __future__ import annotations

import logging
import urllib.robotparser
from dataclasses import dataclass
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

from .agents import (
    ROBOTS_AGENT,
)
from .tiers import (
    ARCHIVE_HOSTS,
)

# --------------------------------------------------------------------------
# robots.txt
# --------------------------------------------------------------------------


#: Hosts where an unreadable robots.txt does NOT block the host.
#:
#: This is an exemption list, not a relaxed rule: the "unreadable means no"
#: default in :class:`RobotsPolicy` is unchanged and still binds every OEM host.
#:
#: ``auto-brochures.com`` is on it because it publishes no robots.txt at all:
#: ``/robots.txt`` 302-redirects to ``/404.html``, which is served with HTTP 200
#: and an HTML body. Measured 2026-08-01 -- ``GET
#: https://www.auto-brochures.com/robots.txt`` -> final URL
#: ``https://www.auto-brochures.com/404.html``, HTTP 200, 10,558 bytes of HTML.
#: The user reviewed this and decided to admit the host anyway
#: (:data:`ARCHIVE_SOURCE_NOTE`); the compensating control is
#: :data:`ARCHIVE_DELAY_SECONDS`.
#:
#: Adding a host here is a policy decision and belongs in a reviewed edit, in
#: the open, exactly like :data:`OFFICIAL_HOSTS`. It is not a knob.
ROBOTS_UNREADABLE_EXEMPT_HOSTS: frozenset[str] = frozenset(ARCHIVE_HOSTS)


def _looks_like_html(body: str) -> bool:
    """True if a robots.txt response body is really an HTML page."""
    head = (body or "").lstrip()[:400].lower()
    return head.startswith(("<!doctype", "<html")) or "<head" in head or "<body" in head


@dataclass
class RobotsVerdict:
    host: str
    #: "ok" | "missing" | "blocked" | "error" | "html" | "exempt"
    status: str
    detail: str = ""

    @property
    def readable(self) -> bool:
        return self.status in ("ok", "missing")


class RobotsPolicy:
    """
    Per-host robots.txt cache that fails closed.

    Outcomes:

    * HTTP 200 with a real robots body -> parse and honour the rules for
      :data:`ROBOTS_AGENT`.
    * HTTP 404/410 -> no policy published, everything allowed.
    * HTTP 200 with an **HTML** body -> ``"html"``, and the host is blocked. A
      site that answers robots.txt with its 404 page has published no rules, and
      handing that HTML to :mod:`urllib.robotparser` yields "no ``User-agent``
      line, therefore everything is allowed" -- fail-open by accident. Treating
      it as unreadable is the same rule as the one below, applied to a case that
      previously slipped past it.
    * HTTP 401/403 or a transport error -> **disallow everything on that host**.
      A host that refuses to serve its own robots.txt to us is refusing
      automated access; we record that and skip the host.

    One exemption, and only one: a host in
    :data:`ROBOTS_UNREADABLE_EXEMPT_HOSTS` whose robots.txt is unreadable gets
    verdict ``"exempt"`` and is allowed. That is a reviewed policy decision per
    host, not a fallback, and it is recorded in the verdict so a run's report can
    never present an exempted host as one that published permissive rules.
    """

    def __init__(
        self,
        fetch,
        *,
        agent: str = ROBOTS_AGENT,
        exempt_hosts: frozenset[str] = ROBOTS_UNREADABLE_EXEMPT_HOSTS,
    ) -> None:
        self._fetch = fetch
        self._agent = agent
        self._exempt = frozenset(h.lower() for h in exempt_hosts)
        self._parsers: dict[str, urllib.robotparser.RobotFileParser | None] = {}
        self.verdicts: dict[str, RobotsVerdict] = {}

    def _unreadable(
        self, host: str, status: str, detail: str
    ) -> urllib.robotparser.RobotFileParser | None:
        """Record an unreadable robots.txt: blocked, unless this host is exempt."""
        if host in self._exempt:
            parser = urllib.robotparser.RobotFileParser()
            parser.parse([])
            self.verdicts[host] = RobotsVerdict(
                host,
                "exempt",
                f"{detail}; host is on ROBOTS_UNREADABLE_EXEMPT_HOSTS by reviewed policy",
            )
            self._parsers[host] = parser
            return parser
        self.verdicts[host] = RobotsVerdict(host, status, detail)
        self._parsers[host] = None
        return None

    def _load(self, host: str) -> urllib.robotparser.RobotFileParser | None:
        if host in self._parsers:
            return self._parsers[host]
        url = f"https://{host}/robots.txt"
        try:
            status, body = self._fetch(url)
        except Exception as exc:  # noqa: BLE001 - transport failures are data
            return self._unreadable(host, "error", str(exc)[:200])

        if status in (404, 410):
            parser = urllib.robotparser.RobotFileParser()
            parser.parse([])
            self.verdicts[host] = RobotsVerdict(host, "missing", f"HTTP {status}")
            self._parsers[host] = parser
            return parser

        if status != 200:
            return self._unreadable(host, "blocked", f"HTTP {status}")

        if _looks_like_html(body or ""):
            return self._unreadable(
                host,
                "html",
                f"HTTP 200 but the body is HTML ({len(body or '')} bytes), not robots rules",
            )

        parser = urllib.robotparser.RobotFileParser()
        parser.parse((body or "").splitlines())
        self.verdicts[host] = RobotsVerdict(host, "ok", "HTTP 200")
        self._parsers[host] = parser
        return parser

    def allows(self, url: str) -> bool:
        host = (urlparse(url).hostname or "").lower()
        if not host:
            return False
        parser = self._load(host)
        if parser is None:
            return False
        try:
            return bool(parser.can_fetch(self._agent, url))
        except Exception:  # noqa: BLE001
            return False

    def verdict_for(self, url_or_host: str) -> RobotsVerdict | None:
        host = (urlparse(url_or_host).hostname or url_or_host).lower()
        return self.verdicts.get(host)
