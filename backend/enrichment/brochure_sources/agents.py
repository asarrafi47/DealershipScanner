"""User agents, robots agent names, and request pacing defaults."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


#: Ordinary desktop Chrome. Default: OEM edges routinely 403 anything else, and
#: a paced sequential client honouring robots.txt is a well-behaved reader
#: whatever string it sends.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

#: Names this client. Kept selectable so the reachability report can measure
#: what an OEM serves an *identified* automated reader versus a browser string.
IDENTIFIED_USER_AGENT = (
    "Mozilla/5.0 (compatible; Claude-User/1.0; +https://claude.com/claude-code) "
    "DealershipScanner-brochure-fetch/1.0"
)

USER_AGENT = BROWSER_USER_AGENT

#: robots.txt product token each user agent is matched against. A browser string
#: claims no product token, so it is held to the ``*`` group.
ROBOTS_AGENT_BY_UA = {
    BROWSER_USER_AGENT: "*",
    IDENTIFIED_USER_AGENT: "Claude-User",
}

#: Default robots product token (matches :data:`USER_AGENT`).
ROBOTS_AGENT = ROBOTS_AGENT_BY_UA[USER_AGENT]


def robots_agent_for(user_agent: str) -> str:
    """robots.txt product token to match ``user_agent`` against."""
    return ROBOTS_AGENT_BY_UA.get(user_agent, "*")

#: Seconds between consecutive network requests. This project has repeatedly
#: been soft-blocked by bursts; paced sequential access gets through.
DEFAULT_DELAY_SECONDS = 4.0

REQUEST_TIMEOUT_SECONDS = 45
