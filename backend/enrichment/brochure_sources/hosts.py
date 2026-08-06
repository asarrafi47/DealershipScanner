"""Official host allowlist, per-host access notes, and unsupported makes."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# Official host allowlist
# --------------------------------------------------------------------------

# Keys are normalised make tokens (see ``_make_token``). Values are the only
# hosts a document may be downloaded from for that make.
OFFICIAL_HOSTS: dict[str, frozenset[str]] = {
    "mazda": frozenset(
        {"news.mazdausa.com", "www.mazdausa.com", "mazdausa.com"}
    ),
    "kia": frozenset(
        {"www.kiamedia.com", "kiamedia.com", "www.kia.com", "kia.com"}
    ),
    "toyota": frozenset({"www.toyota.com", "toyota.com"}),
    "nissan": frozenset({"www.nissanusa.com", "nissanusa.com"}),
    # Added 2026-07-31 as a REVIEWED entry. An acquisition agent wanted this host and
    # monkeypatched is_official_url() at runtime to get past the check, then persisted
    # 12 documents into the corpus. The documents themselves are fine -- they are
    # Infiniti's own /content/dam/Infiniti/US/vehicle-brochures/ CDN paths -- but the
    # bypass defeated the one control that makes provenance trustworthy. Recording the
    # host here is what should have happened: the allowlist is meant to be edited in the
    # open and reviewed, never routed around. See docs at the top of this module.
    "infiniti": frozenset({"www.infinitiusa.com", "infinitiusa.com"}),
    "subaru": frozenset({"media.subaru.com", "www.subaru.com", "subaru.com"}),
    "hyundai": frozenset(
        {"www.hyundainews.com", "hyundainews.com", "www.hyundaiusa.com"}
    ),
    "volkswagen": frozenset({"media.vw.com", "www.vw.com", "vw.com"}),
    # Added 2026-07-31 as a REVIEWED entry, for the largest single acquisition
    # hole in the fleet (411 gap groups / 7,814 active cars). These are Ford
    # Motor Company's own consumer and newsroom domains; no CDN or aggregator is
    # listed. Registering them does NOT make Ford fetchable -- see
    # DISCOVERY_UNRESOLVED and HOST_ACCESS_NOTES, where every one of them is
    # recorded as refusing automated access. The entry exists so that the probe
    # can report "documents on a host we do not allow" separately from "Ford is
    # unreachable", and so the allowlist is edited in the open rather than
    # routed around later.
    "ford": frozenset({"www.ford.com", "ford.com", "media.ford.com"}),
    "lincoln": frozenset(
        {"www.lincoln.com", "lincoln.com", "media.lincoln.com"}
    ),
    # ------------------------------------------------------------------
    # Added 2026-08-01 as REVIEWED entries, for the current-model-year gap.
    #
    # All four were probed host by host on 2026-08-01 under BOTH user agents
    # before being written down; the responses are in HOST_ACCESS_NOTES and the
    # remaining blocker per make is in DISCOVERY_UNRESOLVED. Only manufacturer-
    # operated domains are listed -- no CDN, no aggregator, no dealer.
    #
    # Registering a host does NOT make a make fetchable, and for three of these
    # four it does not: see DISCOVERY_UNRESOLVED. The entry exists so
    # ``--probe-reachability`` can separate "we never allowlisted this host"
    # (BLOCKED_BY_US, our problem) from "this host will not serve us"
    # (BLOCKED_BY_THEIR_EDGE, theirs), and so the allowlist keeps being edited in
    # the open rather than routed around.
    # ------------------------------------------------------------------
    # BMW AG's own press club. The one make here whose documents are proven
    # reachable end to end: see BMW_PRESS_ATTACHMENT_EVIDENCE.
    "bmw": frozenset(
        {"www.press.bmwgroup.com", "press.bmwgroup.com", "www.bmwusa.com"}
    ),
    # GM's own consumer site and its Socrates newsroom.
    "chevrolet": frozenset(
        {"www.chevrolet.com", "chevrolet.com", "media.chevrolet.com", "news.chevrolet.com"}
    ),
    # Mercedes-Benz USA's own consumer site and newsroom.
    "mercedesbenz": frozenset({"www.mbusa.com", "mbusa.com", "media.mbusa.com"}),
    # American Honda's own newsroom. automobiles.honda.com is deliberately NOT
    # here: it 403s its own robots.txt to both user agents, so no document on it
    # may be fetched and listing it would imply otherwise.
    "honda": frozenset({"hondanews.com", "www.hondanews.com"}),
}


#: The one 2026-08-01 measurement that proves a *document* -- not just a
#: robots.txt -- is reachable on a newly registered host.
#:
#: ``GET https://www.press.bmwgroup.com/usa/article/attachment/T0458814EN_US/651500``
#: (the attachment anchored on the "The new BMW X5 and iX5" article page, read
#: out of that page's own ``href``, not constructed) returned HTTP 200,
#: ``Content-Type: application/pdf``, 283,871 bytes, ``%PDF-1.7``. Opened with
#: pypdf it is 24 pages and 59,109 characters of extractable text, prints
#: "X5" 58 times and prints "2026", i.e. it would clear both
#: :func:`verify_document_identity` and :func:`assess_text_quality`.
#:
#: It is recorded here because the honest summary of BMW is "the host serves us
#: real documents and we cannot yet find them per vehicle", and those are two
#: different facts that must not collapse into one.
BMW_PRESS_ATTACHMENT_EVIDENCE = (
    "https://www.press.bmwgroup.com/usa/article/attachment/T0458814EN_US/651500"
    " -> HTTP 200 application/pdf, 283,871 bytes, 24 pages, 59,109 chars"
    " (measured 2026-08-01)"
)

#: What ``derived/brochure_fetch_log.jsonl`` actually records about GM, read
#: 2026-08-02 because :data:`DISCOVERY_UNRESOLVED` asserted the opposite and an
#: assertion that contradicts our own ledger is the failure mode this project
#: keeps having. Every number below was counted from the ledger and checked
#: against the files on disk; none of it came from asking the citation register.
#:
#:   * 19 documents were fetched from GM consumer hosts between
#:     2026-07-31T22:46:51Z and 22:55:50Z: **16 from www.gmc.com, 2 from
#:     www.chevrolet.com, 1 from www.buick.com**. All 19 PDFs are on disk.
#:   * 13 of the 19 have live text in ``derived/brochure_text``. The other 6
#:     were fetched and their text refused by the gates that already existed:
#:     five "document text never mentions the model" (four Sierra HD books and
#:     the 2019 Sierra 1500), one U+FFFD decode failure (2023 Silverado 2500HD).
#:     The PDFs were kept, as the rules require.
#:   * Coverage, measured against active inventory on 2026-08-02: the 19
#:     documents span 19 gap groups holding **540 active cars**; the 13 with
#:     live text hold **476**. Chevrolet's own share of that is small -- 2
#:     documents, 47 cars, of which the one usable document (2023 Colorado)
#:     covers 39. The volume is GMC's.
#:
#: TWO THINGS THAT ARE NOT COVERAGE THIS LANE CAN CLAIM, and they are why the
#: makes stay in :data:`DISCOVERY_UNRESOLVED` / :data:`UNSUPPORTED_MAKES`:
#:
#:   1. All 19 rows record ``discovery_url="web-search:official-host-only"``.
#:      No code in this repository writes that string, so these documents did
#:      not come from a discovery path that exists here and re-running the
#:      fetcher will not find them again.
#:   2. ``www.gmc.com`` and ``www.buick.com`` are **not in**
#:      :data:`OFFICIAL_HOSTS` for their make, so 17 of the 19 could not have
#:      passed :func:`is_official_url`. That is the same bypass recorded against
#:      the Infiniti entry above. It is reported here rather than repaired by
#:      quietly adding the hosts: registering a host requires a measured
#:      robots.txt response in :data:`HOST_ACCESS_NOTES`, and neither host has
#:      been probed by this lane.
GM_LEDGER_EVIDENCE = (
    "brochure_fetch_log.jsonl holds 19 GM-host documents fetched 2026-07-31 "
    "(16 www.gmc.com, 2 www.chevrolet.com, 1 www.buick.com); 13 have live "
    "derived text and cover 19 gap groups / 540 active cars (476 for the 13). "
    "All 19 were discovered out of band and 17 came from hosts not in "
    "OFFICIAL_HOSTS."
)

#: Makes whose discovery path has produced a usable brochure end to end in a
#: real run of this script. Anything in :data:`OFFICIAL_HOSTS` but not here has
#: a correct host allowlist and an unfinished discovery step; see
#: :data:`DISCOVERY_UNRESOLVED`.
VERIFIED_MAKES: frozenset[str] = frozenset({"mazda", "toyota", "nissan"})

#: Makes with a correct host allowlist from which no document can currently be
#: obtained, and why. Measured 2026-07-31; the JS-shell byte counts were
#: identical under both user agents. Listed separately from
#: :data:`UNSUPPORTED_MAKES` because the host work is already done -- only the
#: last step is blocked.
DISCOVERY_UNRESOLVED: dict[str, str] = {
    "kia": (
        "kiamedia.com serves the spec sheet, but its robots.txt disallows "
        "/*/*/download/* for User-agent: * -- we resolve the document and "
        "decline to fetch it"
    ),
    "subaru": "media.subaru.com press-kit URL returns HTTP 200 with a 0-byte body",
    "volkswagen": "media.vw.com model page is a 2,560-byte JS shell, no PDF links",
    "hyundai": "hyundainews.com release page is a 3,253-byte JS shell, no PDF links",
    "ford": (
        "ford.com answers, with a 301 to www.ford.com; www.ford.com, "
        "media.ford.com, shop.ford.com, corporate.ford.com and www.fordpro.com "
        "all refuse to serve robots.txt, under both user agents, so no document "
        "on any of them may be fetched (re-verified per host 2026-07-31, see "
        "HOST_ACCESS_NOTES)"
    ),
    "lincoln": (
        "same as Ford: lincoln.com answers with a 301 to www.lincoln.com, and "
        "www.lincoln.com and media.lincoln.com both refuse to serve robots.txt "
        "under both user agents (re-verified per host 2026-07-31, see "
        "HOST_ACCESS_NOTES)"
    ),
    # ------------------------------------------------------------------
    # Added 2026-08-01. Each of these three makes now has a REACHABLE host --
    # robots.txt readable, page served -- and no per-vehicle document to fetch
    # from it. That is a different and better-understood failure than
    # "unsupported_make", which is what they returned before, and it is recorded
    # as a measurement so nobody re-does the probing.
    # ------------------------------------------------------------------
    "bmw": (
        "www.press.bmwgroup.com is reachable and its robots.txt permits "
        "/usa/article/* under both user agents, and its attachment route serves "
        "real spec-bearing PDFs (see BMW_PRESS_ATTACHMENT_EVIDENCE). What is "
        "missing is a per-vehicle index: /usa/article/search/query:X5 and "
        "/usa/article/search?query=X5 both return the SAME 46 article links as "
        "the unfiltered list (7 Series, R 1300 GS, ...), so the query is applied "
        "client-side, and article URLs are opaque ids (T0458814EN_US) whose "
        "slugs carry no model year. www.bmwusa.com is readable too -- but only "
        "to the identified user agent, and its X5 page (1,003,983 bytes) links "
        "exactly one PDF, a third-party Consumer Reports reprint on "
        "reprints.theygsgroup.com; the sitemap has no brochure page at all. "
        "NOTE for whoever picks this up: --probe-reachability reports the BMW "
        "article page as 'no_document_links' because _DOCUMENT_ROUTE does not "
        "recognise /usa/article/attachment/<id>/<id>, which carries no .pdf "
        "extension. That is the matcher, not the host -- the route was fetched "
        "by hand and returned a real PDF. Adding it would need a year-bearing "
        "page path for page_scoped_document to attribute it safely, and the "
        "article slug has none"
    ),
    # CORRECTED 2026-08-02 against derived/brochure_fetch_log.jsonl. The previous
    # text asserted that GM "publishes no PDF". The ledger says otherwise and had
    # said so since the day before that sentence was written, so the claim was
    # false when made. See GM_LEDGER_EVIDENCE for the documents and the counts;
    # what is corrected here is only the conclusion, not the index-page
    # measurements, which are left as recorded on 2026-08-01 and were NOT re-run.
    "chevrolet": (
        "the INDEX is missing, not the documents. Recorded 2026-08-01: "
        "www.chevrolet.com serves robots.txt (HTTP 200, 3,266 bytes) to a "
        "browser user agent, /download-catalog 302s to the site root, "
        "/browse-brochures 404s, the Silverado 1500 model page is 1,023,434 "
        "bytes containing zero '.pdf' strings, sitemap.xml lists no brochure or "
        "catalog page, and media.chevrolet.com model URLs 302 to "
        "news.chevrolet.com/newsroom.html (31,995 bytes, no document anchor). "
        "But the host DOES serve brochure PDFs: the fetch ledger holds two, "
        "fetched from https://www.chevrolet.com/content/dam/chevrolet/na/us/"
        "english/index/shopping-tools/download-catalog/11-pdf/ on 2026-07-31 "
        "(2023 Colorado, 31 pages; 2023 Silverado 2500HD, 32 pages). Neither "
        "was found by a route in this repo -- both record discovery_url "
        "'web-search:official-host-only', a string no code here writes -- so "
        "the discovery step is still unresolved and this make stays here. What "
        "is refuted is only the claim that GM publishes nothing usable"
    ),
    "mercedesbenz": (
        "media.mbusa.com publishes no robots.txt (HTTP 404 under both user "
        "agents) and its home page is 486,647 bytes with zero '.pdf' hrefs. Its "
        "only document route is /releases/<slug>/download, which answers HTTP "
        "403 -- the newsroom gates attachments behind registration. www.mbusa.com "
        "serves readable robots.txt (1,025 bytes) and no brochure PDFs"
    ),
    # PARTLY RESOLVED 2026-08-02, and only for the PDF lane does it stay here.
    # The 2026-08-01 measurement below is confirmed -- Honda publishes no PDF we
    # can reach -- but "no PDF" was being read as "nothing we can use", and that
    # part is now refuted by a working end-to-end fetch. See
    # :data:`HTML_SPEC_LANE_NOTE` and ``backend/enrichment/html_spec_sources``.
    "honda": (
        "hondanews.com is readable (robots.txt HTTP 404, 0 bytes -> no rules "
        "published) but publishes specifications as HTML, not as documents: the "
        "2026 HR-V 'Specifications & Features' release is 89,840 bytes and the "
        "HR-V specs channel 107,344 bytes, both with zero '.pdf' hrefs, and the "
        "/download route redirects to the site root (registration). "
        "automobiles.honda.com 403s its own robots.txt to both user agents. "
        "NO PDF LANE EXISTS FOR THIS MAKE and none is expected; the HTML lane "
        "(html_spec_sources) fetches those same releases and cites them"
    ),
}


#: HTML specification pages, added 2026-08-02 as a second document kind rather
#: than a second door: same host allowlist, same robots policy, same pacing,
#: same identity and text-quality gates. Only the LOCATOR differs -- a PDF
#: bullet cites ``{file, page}``, an HTML bullet cites
#: ``{url, retrieved_at, sha256, table/row/column}`` into bytes we stored.
#:
#: Why this note lives here: three makes in :data:`DISCOVERY_UNRESOLVED` were
#: measured by looking for ``.pdf`` hrefs, and a reader of those entries could
#: reasonably conclude the makes publish no specifications at all. They publish
#: them as HTML. What was measured on 2026-08-02, per make:
#:
#:   honda        RESOLVED for HTML. hondanews.com anchors 15 automobile
#:                channel pages off its home page; each channel page anchors a
#:                "Specs" tab; each specs tab lists one "<YEAR> Honda <MODEL>
#:                Specifications & Features" release per model year (Accord:
#:                24 releases, model years 2011-2026). The release body is a
#:                single HTML table with one trim per column and a per-trim
#:                standard-equipment mark in every cell. Every hop is an href
#:                read off the page before it -- the Toyota lesson.
#:   chevrolet    NOT UNLOCKED. Measured 2026-08-02, robots honoured, one paced
#:                request each: www.chevrolet.com/trucks/silverado/1500 is
#:                1,024,461 bytes containing **zero ``<table>`` elements**. Its
#:                only spec-ish anchors go to
#:                /shopping/configurator/truck/2026/silverado-1500/.../compare,
#:                which is 36,016 bytes -- a client-side app shell with no
#:                table and no further anchor. There is no server-rendered trim
#:                grid to quote.
#:   mercedesbenz NOT UNLOCKED. Measured the same way and the same day:
#:                www.mbusa.com/en/vehicles/class/gle/suv is 1,294,569 bytes
#:                with zero ``<table>`` elements; the per-trim compare page it
#:                anchors (/en/compare-vehicles/2026-gle-gle350w4) is 779,016
#:                bytes, also with zero. media.mbusa.com's home page (486,647
#:                bytes) likewise has none.
#:
#: So the honest scope of the HTML lane is: it unlocks Honda. It does not
#: unlock Chevrolet or Mercedes-Benz, whose specifications are assembled in the
#: browser rather than printed into the page. A later lane wanting those two
#: would be reading a JSON API, which is a different source kind with a
#: different citation contract and is not what this module implements.
HTML_SPEC_LANE_NOTE = (
    "Honda's per-trim equipment grids are HTML, not PDF, and are fetched and "
    "cited by backend/enrichment/html_spec_sources (added 2026-08-02). "
    "Chevrolet and Mercedes-Benz were measured the same day and serve zero "
    "<table> elements on their model, compare and newsroom pages, so the HTML "
    "lane does not unlock them."
)

#: Per-host evidence for a host that cannot be read at all, so "unreachable" is
#: never an assertion without a measurement behind it. Every string below is a
#: response actually received from ``https://<host>/robots.txt``, one paced
#: sequential request per host per protocol.
#:
#: RE-VERIFIED 2026-07-31, host by host, and the earlier summary needed one
#: correction: it is NOT true that every Ford/Lincoln host refuses us. The two
#: apex hosts answer normally and hand us to a host that refuses:
#:
#:   ford.com    -> HTTP 301, 239 bytes, to https://www.ford.com/robots.txt
#:   lincoln.com -> HTTP 301, 242 bytes, to https://www.lincoln.com/robots.txt
#:
#: Everything the redirects lead to, and every other registered host, refuses:
#:
#:   * :data:`BROWSER_USER_AGENT` and :data:`IDENTIFIED_USER_AGENT` behave
#:     identically -- the refusal is not user-agent selection. Both were run
#:     through ``--probe-reachability --user-agent both`` on 2026-07-31 and all
#:     four www/media hosts came back ``robots_blocked`` under both.
#:   * over HTTP/2 the TLS handshake completes and the edge then resets the
#:     stream (``INTERNAL_ERROR``) before any response header;
#:   * forced to HTTP/1.1 the connection is accepted and then black-holed --
#:     0 bytes, curl error 28 at the 40s ceiling;
#:   * an independent client (``curl``) reproduces both, and the same client
#:     reached the control host ``news.mazdausa.com`` in 0.11-0.26s (HTTP 404,
#:     62 bytes -- Mazda publishes no robots.txt), so the refusal is Ford's and
#:     not this machine's network path.
#:
#: robots.txt being unreadable is a refusal of automated access, and this module
#: treats that as "no" (see :class:`RobotsPolicy`). No further transport was
#: tried: rotating protocols or fingerprints until one gets through is evading a
#: block, not honouring one. The deliverable here is the documented refusal.
HOST_ACCESS_NOTES: dict[str, str] = {
    # Answers -- but only to redirect to a host that does not.
    "ford.com": (
        "robots.txt: HTTP 301, 239 bytes, in 0.33-0.50s -> "
        "https://www.ford.com/robots.txt, which refuses (2026-07-31)"
    ),
    "lincoln.com": (
        "robots.txt: HTTP 301, 242 bytes, in 0.39-0.43s -> "
        "https://www.lincoln.com/robots.txt, which refuses (2026-07-31)"
    ),
    # Refuse.
    "www.ford.com": (
        "robots.txt: HTTP/2 stream reset (INTERNAL_ERROR) in 0.43s; "
        "HTTP/1.1 0 bytes, timed out at 40.008s (2026-07-31)"
    ),
    "media.ford.com": (
        "robots.txt: HTTP/2 stream reset (INTERNAL_ERROR) in 0.33s; "
        "HTTP/1.1 0 bytes, timed out at 40.002s (2026-07-31)"
    ),
    "shop.ford.com": (
        "robots.txt: HTTP/2 stream reset (INTERNAL_ERROR) in 0.12s; "
        "HTTP/1.1 0 bytes, timed out at 40.011s (2026-07-31)"
    ),
    "corporate.ford.com": (
        "robots.txt: 0 bytes on both protocols, timed out at 40.01s "
        "(no reset, just no answer) (2026-07-31)"
    ),
    "www.fordpro.com": (
        "robots.txt: HTTP/2 stream reset (INTERNAL_ERROR) in 0.72s; "
        "HTTP/1.1 0 bytes, timed out at 40.011s (2026-07-31)"
    ),
    "www.fleet.ford.com": (
        "robots.txt: HTTP 301, 0 bytes, in 0.06-0.14s -> "
        "https://www.fordpro.com/en-us/?redirectFromFleet=1 (the site root, not "
        "a robots.txt), and www.fordpro.com refuses (2026-07-31)"
    ),
    "www.lincoln.com": (
        "robots.txt: HTTP/2 stream reset (INTERNAL_ERROR) in 0.22s; "
        "HTTP/1.1 0 bytes, timed out at 40.012s (2026-07-31)"
    ),
    "media.lincoln.com": (
        "robots.txt: HTTP/2 stream reset (INTERNAL_ERROR) in 0.15s; "
        "HTTP/1.1 0 bytes, timed out at 40.006s (2026-07-31)"
    ),
    # ------------------------------------------------------------------
    # RE-TESTED 2026-08-01, because a recorded refusal that is never re-measured
    # becomes folklore. Every Ford host above reproduced its 2026-07-31 result
    # exactly, one paced request per host per protocol per user agent:
    # www.ford.com / media.ford.com / shop.ford.com / www.fordpro.com all reset
    # the HTTP/2 stream in 0.11-0.29s and black-hole HTTP/1.1 for the full 30s
    # ceiling; corporate.ford.com answers on neither; ford.com still 301s to
    # www.ford.com with 239 bytes. The identified user agent got byte-identical
    # treatment. Ford's refusal stands, and it is Ford's, not this machine's --
    # www.toyota.com answered in 0.10s from the same shell in the same minute.
    #
    # Four Ford hosts NOT previously recorded were tried the same day, so the
    # search is on the record as having been widened rather than abandoned:
    "owner.ford.com": (
        "robots.txt: 0 bytes, connection accepted then black-holed, timed out "
        "at 25.01s (2026-08-01)"
    ),
    "build.ford.com": (
        "robots.txt: HTTP/2 stream reset (INTERNAL_ERROR) in 0.13s (2026-08-01)"
    ),
    "www.fordvehicles.com": (
        "robots.txt: HTTP 302 to https://www.ford.com/ (the site root, not a "
        "robots.txt), and www.ford.com refuses (2026-08-01)"
    ),
    "www.fordparts.com": (
        "robots.txt: HTTP 301 -> parts.ford.com/robots.txt, which resets the "
        "HTTP/2 stream (2026-08-01)"
    ),
    # The one Ford host that answers -- and it changes nothing, which is why it
    # is recorded rather than pursued. A CDN origin publishes no index page, and
    # every Ford host that could carry one refuses, so there is no anchor to read
    # a document URL from. Constructing one from a template is the inference this
    # module does not do.
    "cdn.ford.com": (
        "robots.txt: HTTP 404, 426 bytes, in 0.57s -- readable (no rules "
        "published), but it is a CDN origin with no index page to discover a "
        "document from (2026-08-01)"
    ),
    # ------------------------------------------------------------------
    # Hosts registered 2026-08-01, measured under BOTH user agents.
    # ------------------------------------------------------------------
    "www.press.bmwgroup.com": (
        "robots.txt: HTTP 200, 10,705 bytes, in 0.12-0.16s under BOTH user "
        "agents; its rules disallow only */oneclick and /my/, so /usa/article/* "
        "is permitted. Its attachment route served a 283,871-byte PDF "
        "(2026-08-01)"
    ),
    "press.bmwgroup.com": (
        "robots.txt: HTTP 200, 10,705 bytes, 301 to www.press.bmwgroup.com, "
        "under both user agents (2026-08-01)"
    ),
    "www.bmwusa.com": (
        "robots.txt: the user agents get OPPOSITE answers -- HTTP/2 stream reset "
        "in 0.12s to the browser string, HTTP 200 with 186 bytes of permissive "
        "rules to the identified string. Readable, and it publishes no brochure "
        "PDF (2026-08-01)"
    ),
    "www.chevrolet.com": (
        "robots.txt: HTTP 200, 3,266 bytes, to the browser user agent; HTTP 403, "
        "385 bytes, to the identified one (2026-08-01)"
    ),
    "media.chevrolet.com": (
        "robots.txt: HTTP 200, 66,410 bytes, to the browser user agent; HTTP "
        "403 to the identified one (2026-08-01)"
    ),
    "news.chevrolet.com": (
        "robots.txt: HTTP 404 (13,396-byte HTML 404 page) to the browser user "
        "agent; HTTP 403 to the identified one (2026-08-01)"
    ),
    "media.mbusa.com": (
        "robots.txt: HTTP 404 with a 417,694-byte HTML soft-404 body, under both "
        "user agents -- no rules published. Its /releases/<slug>/download route "
        "answers HTTP 403 (2026-08-01)"
    ),
    "www.mbusa.com": (
        "robots.txt: HTTP 200, 1,025 bytes, under both user agents (2026-08-01)"
    ),
    "hondanews.com": (
        "robots.txt: HTTP 404, 0 bytes, after a redirect to /en-US/robots.txt, "
        "under both user agents -- no rules published (2026-08-01)"
    ),
    "automobiles.honda.com": (
        "robots.txt: HTTP 403, 389 bytes, under both user agents -- an Akamai "
        "'Access Denied' page. Unreadable means no (2026-08-01)"
    ),
    # The apex hosts. Measured because the first draft of this edit registered
    # them without probing them, and the allowlist test caught it: a host in
    # OFFICIAL_HOSTS with no recorded response is exactly the assertion this
    # table exists to prevent.
    "chevrolet.com": (
        "robots.txt: HTTP 301 -> www.chevrolet.com/robots.txt, then HTTP 200 "
        "with 3,266 bytes to the browser user agent and HTTP 403 to the "
        "identified one (2026-08-01)"
    ),
    "mbusa.com": (
        "robots.txt: HTTP 301 -> www.mbusa.com/robots.txt, then HTTP 200 with "
        "1,025 bytes, under both user agents (2026-08-01)"
    ),
    "www.hondanews.com": (
        "robots.txt: two redirects to hondanews.com/en-US/robots.txt, HTTP 404, "
        "0 bytes -- no rules published, under both user agents (2026-08-01)"
    ),
}

#: Makes with no machine-fetchable official brochure index found yet. Every
#: string is what ``--probe-reachability`` measured on 2026-07-31; the user agent
#: is named because it changes the answer for several brands (Toyota and Nissan
#: 403 an identified agent at robots.txt and serve their brochure index to a
#: browser string). Kept explicit so the fetcher reports *why* a high-volume gap
#: is skipped instead of silently dropping it.
#: ``bmw``, ``chevrolet``, ``mercedesbenz`` and ``honda`` were moved OUT of this
#: table on 2026-08-01 and into :data:`DISCOVERY_UNRESOLVED`, because each now
#: has a registered, measured, reachable host and the remaining blocker is the
#: discovery step. :func:`unsupported_reason` consults ``DISCOVERY_UNRESOLVED``
#: first, so leaving a duplicate entry here would be dead text that a later
#: reader would take for a current measurement.
UNSUPPORTED_MAKES: dict[str, str] = {
    "lexus": "www.lexus.com/brochures 404s; no other index located",
    "acura": "no index probed",
    "infiniti": "no index probed",
    "jeep": "jeep.com/bmo.html is 113KB of build-and-price app, zero PDF anchors",
    "ram": "ramtrucks.com/bmo.html is 112KB of build-and-price app, zero PDF anchors",
    "dodge": "no Stellantis brochure index located (see jeep/ram)",
    "chrysler": "no Stellantis brochure index located (see jeep/ram)",
    # CORRECTED 2026-08-02 against the fetch ledger, same as the chevrolet entry
    # in DISCOVERY_UNRESOLVED: "no index" was recorded as if it meant "no
    # documents", and www.gmc.com had already served 16 of them. See
    # GM_LEDGER_EVIDENCE.
    "gmc": (
        "gmc.com/browse-brochures 404s (read-timed-out on an earlier probe), so "
        "no index is known -- but www.gmc.com does serve brochure PDFs from "
        "/content/dam/gmc/na/us/english/index/about/download-brochures/: 16 are "
        "in the fetch ledger from 2026-07-31, 11 with live text, covering 436 "
        "active cars -- the bulk of the GM documents we hold. They were "
        "discovered out of band and www.gmc.com is not in OFFICIAL_HOSTS, so "
        "this make is not fetchable by anything in this repo"
    ),
    "buick": (
        "no GM brochure index located (see chevrolet/gmc) -- but www.buick.com "
        "served one brochure PDF from /content/dam/buick/na/us/en/index/"
        "shopping-tools/download-catalog/ on 2026-07-31 (2020 Encore GX, live "
        "text). Discovered out of band; the host is not in OFFICIAL_HOSTS"
    ),
    "cadillac": "no GM brochure index located (see chevrolet/gmc)",
    "audi": "media.audiusa.com model index is a 2,497-byte JS shell, no PDF links",
    "volvo": "media.volvocars.com robots.txt returns HTTP 503 to both user agents",
}
