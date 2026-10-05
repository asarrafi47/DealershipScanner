"""
Car detail page (VDP) hot-path guards.

Two regressions cost real page time and both are cheap to pin down:

* ``_public_gallery_photo_count`` is re-run for every active listing on every
  listings-grid rebuild (which fires on a 60s cache token).  It is memoized and
  counts without materialising the filtered list, so both shortcuts must agree
  with ``filter_public_gallery_urls`` exactly — a wrong count would show a wrong
  photo badge and reorder the grid via ``listing_sort_key_by_price``.
* ``/api/cars/<id>/packages/ensure`` used to hold the request open for the whole
  on-demand OEM sticker fetch, and the budget that was supposed to stop that
  only covered the inner call: the route then did its DB + panel work *while the
  worker kept running*, so a 6s budget still produced 13-17s responses, every
  repeat view paid it again, and the status could flip between calls because the
  row was read mid-write.  The budget now belongs to the request, the payload is
  built before any worker starts, and a car that just ran the pipeline is in
  cooldown.
* The budget then silently *dropped* the sticker: the response said "fetching"
  and nothing else, and ``car_packages.js`` calls the endpoint once per page
  load.  Measured on a real car (91583, fresh process, premium session) the XHR
  answered at 6.01s and the worker stored a real Monroney PDF 18.9s later that
  no shopper ever saw.  The response now carries a retry contract
  (``window_sticker_fetch_in_flight`` / ``window_sticker_should_retry`` /
  ``window_sticker_retry_after_seconds``) a client can loop on.
* ``window_sticker_status`` also lied: car 125964 answered
  ``window_sticker_ensure_skipped="cooldown"`` and ``window_sticker_status=
  "fetching"`` in the same response with no thread alive.  "fetching" now means
  exactly "a worker for this car is alive as this response is built"; the
  source-exists-but-nothing-running case is ``pending``.
"""

import json
import threading

import pytest

from backend.routes import cars_pages
from backend.routes.car_detail import packages_ensure
from backend.routes.car_detail.packages_ensure import (
    ENSURE_COMPLETED,
    ENSURE_COOLDOWN,
    ENSURE_INFLIGHT,
    ENSURE_TIMEOUT,
    STICKER_STATUS_FETCHING,
    STICKER_STATUS_HIDDEN,
    STICKER_STATUS_PENDING,
    STICKER_STATUS_READY,
    STICKER_STATUS_UNAVAILABLE,
    _packages_ensure_budget_seconds,
    _packages_ensure_cooldown_seconds,
    _run_packages_ensure_with_budget,
    _window_sticker_status,
)
from backend.utils.car_serialize import serialize as ser
from backend.vision.url_heuristics import (
    filter_public_gallery_urls,
    heuristic_listing_gallery_fluff_url,
)

# Realistic mix: dealer lot photos, a Carfax badge, an iPacket tile, duplicates.
_GALLERY = [
    "https://cdn.dealerinspire.com/inv/2023/abc_001.jpg",
    "https://cdn.dealerinspire.com/inv/2023/abc_002.jpg",
    "https://cdn.dealerinspire.com/inv/2023/abc_003.jpg",
    "https://www.carfax.com/img/badge/1-owner.png",
    "https://ipacket.com/window-sticker/tile.png",
    "https://cdn.dealerinspire.com/inv/2023/abc_001.jpg",
]


def _uncached_photo_count(raw, image_url=None) -> int:
    """The pre-memo computation, kept here as the oracle."""
    urls = filter_public_gallery_urls(ser._parse_gallery_url_list(raw))
    if not urls and image_url:
        iu = str(image_url).strip()
        if iu and not heuristic_listing_gallery_fluff_url(iu):
            urls = [iu]
    return len(urls)


@pytest.mark.parametrize(
    "raw, image_url",
    [
        (_GALLERY, None),
        (_GALLERY, "https://cdn.dealerinspire.com/inv/2023/hero.jpg"),
        ([], "https://cdn.dealerinspire.com/inv/2023/hero.jpg"),
        ([], "https://www.carfax.com/img/badge/1-owner.png"),
        ([], None),
        (None, None),
        ("not json at all", None),
        ('["https://cdn.dealerinspire.com/inv/x/1.jpg"]', None),
    ],
)
def test_photo_count_memo_matches_uncached(raw, image_url):
    want = _uncached_photo_count(raw, image_url)
    # First call fills the memo, second must come back off it — both must agree.
    assert ser._public_gallery_photo_count(raw, image_url=image_url) == want
    assert ser._public_gallery_photo_count(raw, image_url=image_url) == want


def test_photo_count_memo_is_actually_used():
    ser._PHOTO_COUNT_MEMO.clear()
    ser._public_gallery_photo_count(_GALLERY, image_url=None)
    assert len(ser._PHOTO_COUNT_MEMO) == 1
    ser._public_gallery_photo_count(_GALLERY, image_url=None)
    assert len(ser._PHOTO_COUNT_MEMO) == 1


def test_photo_count_memo_separates_image_url_fallback():
    """Same empty gallery, different fallback image → must not collide in the memo."""
    ser._PHOTO_COUNT_MEMO.clear()
    kept = ser._public_gallery_photo_count([], image_url="https://cdn.dealerinspire.com/inv/a/1.jpg")
    dropped = ser._public_gallery_photo_count([], image_url="https://www.carfax.com/img/badge/1-owner.png")
    assert kept == 1
    assert dropped == 0


def test_photo_count_memo_evicts_at_cap(monkeypatch):
    monkeypatch.setattr(ser, "_PHOTO_COUNT_MEMO_MAX", 3)
    ser._PHOTO_COUNT_MEMO.clear()
    for i in range(10):
        ser._public_gallery_photo_count([f"https://cdn.dealerinspire.com/inv/x/{i}.jpg"])
    assert len(ser._PHOTO_COUNT_MEMO) <= 3


def test_photo_count_unhashable_input_still_counts():
    """A gallery blob we can't key on must fall through, not raise or cache wrongly."""
    ser._PHOTO_COUNT_MEMO.clear()
    assert ser._public_gallery_photo_count({"unexpected": "shape"}) == 0
    assert ser._PHOTO_COUNT_MEMO == {}


# The count-only path skips ``kept.sort(...)`` and the two list builds inside
# ``filter_public_gallery_urls``. That is only sound because the sort decides
# *which* of two URLs that upgrade to the same full-size asset is returned, not
# how many distinct assets there are. These are the shapes where that matters.
_THUMB = "https://vehicle-images.carscommerce.inc/stock/thumbnails/large/abc.jpg"
_FULL = "https://vehicle-images.carscommerce.inc/stock/abc.jpg"


@pytest.mark.parametrize(
    "urls",
    [
        [],
        ["   ", ""],
        _GALLERY,
        # thumbnail + its full-size twin, both orders: one photo, not two
        [_THUMB, _FULL],
        [_FULL, _THUMB],
        # twin pair interleaved with lot photos and fluff
        [
            "https://cdn.dealerinspire.com/inv/2023/abc_001.jpg",
            _THUMB,
            "https://www.carfax.com/img/badge/1-owner.png",
            _FULL,
            "https://cdn.dealerinspire.com/inv/2023/abc_002.jpg",
        ],
        # every URL is fluff
        [
            "https://www.carfax.com/img/badge/1-owner.png",
            "https://ipacket.com/window-sticker/tile.png",
        ],
        # non-strings and Nones mixed in
        ["https://cdn.dealerinspire.com/inv/2023/abc_001.jpg", None, 7, {"a": 1}],
    ],
)
def test_count_only_path_matches_filter_public_gallery_urls(urls):
    assert ser._count_public_gallery_urls(urls) == len(filter_public_gallery_urls(urls))


@pytest.mark.parametrize(
    "show, ready, visual, has_source, in_flight, expected",
    [
        (False, False, False, False, False, STICKER_STATUS_HIDDEN),
        (False, True, True, True, True, STICKER_STATUS_HIDDEN),
        (True, True, False, False, False, STICKER_STATUS_READY),
        (True, False, True, False, False, STICKER_STATUS_READY),
        # Stored beats everything below it: a worker that is still running does
        # not turn a sticker we already have back into a spinner.
        (True, True, False, True, True, STICKER_STATUS_READY),
        # A worker really is running — the one and only "fetching" case.
        (True, False, False, True, True, STICKER_STATUS_FETCHING),
        (True, False, False, False, True, STICKER_STATUS_FETCHING),
        # The lie this exists to kill: a source exists, nothing is stored and
        # nothing is running. That is "pending", not "fetching" — car 125964
        # answered "fetching" here while the ensure call was skipped by cooldown.
        (True, False, False, True, False, STICKER_STATUS_PENDING),
        # Panel shown, nothing stored, no source to fetch from at all.
        (True, False, False, False, False, STICKER_STATUS_UNAVAILABLE),
    ],
)
def test_window_sticker_status_states(show, ready, visual, has_source, in_flight, expected):
    assert (
        _window_sticker_status(
            show_sticker_ui=show,
            sticker_ready=ready,
            sticker_visual=visual,
            has_source=has_source,
            fetch_in_flight=in_flight,
        )
        == expected
    )


def test_window_sticker_status_never_says_fetching_without_a_live_fetch():
    """The invariant, over the whole input space: fetching ⇒ fetch_in_flight."""
    for show in (True, False):
        for ready in (True, False):
            for visual in (True, False):
                for has_source in (True, False):
                    st = _window_sticker_status(
                        show_sticker_ui=show,
                        sticker_ready=ready,
                        sticker_visual=visual,
                        has_source=has_source,
                        fetch_in_flight=False,
                    )
                    assert st != STICKER_STATUS_FETCHING, (show, ready, visual, has_source)


# Fake slow OEM fetches block on this event instead of sleeping. A worker is
# "still running" for exactly as long as the test wants and no longer, so the
# assertions below are about ORDER ("the request came back while the fetch was
# still blocked") rather than seconds on whatever machine runs the suite. The
# 30s cap only matters if a regression makes the code wait for the worker: the
# test then fails instead of hanging.
_RELEASE_WORKERS = threading.Event()
_WORKER_CAP_S = 30.0


def _wait_for_release() -> None:
    _RELEASE_WORKERS.wait(_WORKER_CAP_S)


@pytest.fixture
def drain_ensure_workers():
    """Don't leak the fake slow-fetch workers (or their bookkeeping) into the next test."""
    _RELEASE_WORKERS.clear()
    packages_ensure._packages_ensure_inflight.clear()
    packages_ensure._packages_ensure_snapshot.clear()
    packages_ensure._packages_ensure_attempted_at.clear()
    packages_ensure._packages_ensure_panel_cache.clear()
    yield
    _RELEASE_WORKERS.set()
    for t in list(packages_ensure._packages_ensure_inflight.values()):
        t.join(10.0)
    packages_ensure._packages_ensure_inflight.clear()
    packages_ensure._packages_ensure_snapshot.clear()
    packages_ensure._packages_ensure_attempted_at.clear()
    packages_ensure._packages_ensure_panel_cache.clear()
    _RELEASE_WORKERS.clear()


def _spy(monkeypatch, name: str) -> list:
    """Count calls to ``cars_pages.<name>`` (the route looks these up as module globals)."""
    calls: list = []
    real = getattr(cars_pages, name)

    def wrapper(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(cars_pages, name, wrapper)
    return calls


def _release_and_join_workers() -> None:
    """Let every blocked fake fetch finish, and wait until its bookkeeping has run."""
    threads = list(packages_ensure._packages_ensure_inflight.values())
    _RELEASE_WORKERS.set()
    for t in threads:
        t.join(10.0)
        assert not t.is_alive()


def test_packages_ensure_budget_env(monkeypatch):
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "2.5")
    assert _packages_ensure_budget_seconds() == 2.5
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "junk")
    assert _packages_ensure_budget_seconds() > 0
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "")
    assert _packages_ensure_budget_seconds() > 0


def test_packages_ensure_cooldown_env(monkeypatch):
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS", "30")
    assert _packages_ensure_cooldown_seconds() == 30
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS", "junk")
    assert _packages_ensure_cooldown_seconds() > 0
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS", "0")
    assert _packages_ensure_cooldown_seconds() == 0


def test_packages_ensure_returns_within_budget(monkeypatch, drain_ensure_workers):
    """A slow OEM fetch must not hold the request open past the budget."""
    import backend.enrichment.listing_packages_service as lps

    started = threading.Event()

    def slow(car_id, **kw):
        started.set()
        _wait_for_release()
        return {"ok": True, "car_id": car_id}

    monkeypatch.setattr(lps, "ensure_listing_packages_for_car", slow)

    status, outcome = _run_packages_ensure_with_budget(-4242, allow_vision=False, budget=0.1)

    assert started.wait(5.0)
    assert outcome == ENSURE_TIMEOUT
    assert status is None
    # The call came back while the fetch was still blocked: the budget ended the
    # wait, not the fetch. Waiting for the worker would have returned COMPLETED.
    assert not _RELEASE_WORKERS.is_set()
    assert packages_ensure._packages_ensure_worker_alive(-4242)


def test_packages_ensure_single_flight(monkeypatch, drain_ensure_workers):
    """A second view of the same VIN must not start a second fetch, nor wait on the first."""
    import backend.enrichment.listing_packages_service as lps

    calls = []

    def slow(car_id, **kw):
        calls.append(car_id)
        _wait_for_release()
        return {"ok": True, "car_id": car_id}

    monkeypatch.setattr(lps, "ensure_listing_packages_for_car", slow)

    _run_packages_ensure_with_budget(-4243, allow_vision=False, budget=0.1)
    worker = packages_ensure._packages_ensure_inflight[-4243]
    joins = []
    real_join = worker.join

    def counting_join(*a, **kw):
        joins.append(a)
        return real_join(*a, **kw)

    worker.join = counting_join

    status, outcome = _run_packages_ensure_with_budget(-4243, allow_vision=False, budget=0.1)

    assert outcome == ENSURE_INFLIGHT
    assert status is None
    assert joins == [], "second view waited on the running fetch instead of short-circuiting"
    assert len(calls) == 1


def test_packages_ensure_returns_result_when_fast(monkeypatch, drain_ensure_workers):
    import backend.enrichment.listing_packages_service as lps

    monkeypatch.setattr(
        lps, "ensure_listing_packages_for_car", lambda car_id, **kw: {"ok": True, "car_id": car_id}
    )
    status, outcome = _run_packages_ensure_with_budget(-4244, allow_vision=False, budget=5)
    assert outcome == ENSURE_COMPLETED
    assert status == {"ok": True, "car_id": -4244}


# --- endpoint-level contract -------------------------------------------------
#
# The budget is only worth anything if the *request* honours it. These drive the
# real Flask route with a premium session, the way a browser does.

_ENSURE_CAR_ID = 4242


@pytest.fixture
def ensure_client(monkeypatch, drain_ensure_workers):
    """Premium test client for ``/api/cars/<id>/packages/ensure`` with the row stubbed out."""
    import secrets

    from backend import main as main_mod

    row = {
        "id": _ENSURE_CAR_ID,
        "vin": "1N4BL4DV1RN359022",
        "year": 2024,
        "make": "Nissan",
        "model": "Altima",
        "gallery": [],
        "packages": None,
        "source_url": "https://dealer.example.com/used/altima/1N4BL4DV1RN359022",
    }
    state = {"row": row}

    monkeypatch.setenv("BILLING_STRIPE_ENABLED", "0")
    monkeypatch.setattr(cars_pages, "get_car_by_id", lambda car_id, **kw: dict(state["row"]))
    monkeypatch.setattr(
        cars_pages,
        "prepare_car_detail_context",
        lambda car: {"packages_panel_has_content": False, "listing_sticker_options": []},
    )

    import backend.enrichment.window_sticker_service as wss
    import backend.scanner.window_sticker as ws

    monkeypatch.setattr(wss, "sticker_panel_payload", lambda ctx, car=None: {})
    # ``_stored`` stands in for "a sticker file exists on disk for this VIN" —
    # the fake pipeline sets it the way the real one stores a PDF.
    monkeypatch.setattr(wss, "window_sticker_available", lambda car: bool(state["row"].get("_stored")))
    monkeypatch.setattr(wss, "window_sticker_has_visual", lambda car: bool(state["row"].get("_stored")))
    monkeypatch.setattr(ws, "show_window_sticker_panel", lambda car, ctx=None: True)
    monkeypatch.setattr(ws, "cdjr_oem_window_sticker_eligible", lambda car: False)
    monkeypatch.setattr(ws, "car_listing_sticker_urls", lambda car: list(state["row"].get("_urls") or []))
    monkeypatch.setattr(ws, "get_window_sticker_url", lambda vin: None)
    monkeypatch.setattr(ws, "sticker_embed_preview_url", lambda car, **kw: None)

    token = secrets.token_urlsafe(32)
    with main_mod.app.test_client() as c:
        with c.session_transaction() as sess:
            sess["user_id"] = 1
            sess["user_is_premium"] = True
            sess["_csrf_token"] = token

        def post():
            return c.post(
                f"/api/cars/{_ENSURE_CAR_ID}/packages/ensure", headers={"X-CSRF-Token": token}
            )

        yield post, state


def test_endpoint_honours_wall_clock_budget(monkeypatch, ensure_client):
    """
    The regression this exists for: the budget bounded the inner ``ensure`` call
    only, and the route then did its DB/panel work alongside the still-running
    worker — 6s budget, 13-17s responses.

    Pinned structurally: the inner call gets at most the request's budget, the
    panel is built once (before the worker starts, never alongside it), and the
    response is back while the fetch is still blocked.
    """
    import backend.enrichment.listing_packages_service as lps

    post, _state = ensure_client
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "0.2")

    def slow(car_id, **kw):
        _wait_for_release()
        return {"ok": True, "car_id": car_id}

    monkeypatch.setattr(lps, "ensure_listing_packages_for_car", slow)
    runs = _spy(monkeypatch, "_run_packages_ensure_with_budget")
    panel_builds = _spy(monkeypatch, "_packages_ensure_payload")

    r = post()
    assert r.status_code == 200
    assert len(runs) == 1
    inner_budget = runs[0][1]["budget"]
    assert 0 < inner_budget <= 0.2, f"inner call got {inner_budget}s of a 0.2s request budget"
    assert len(panel_builds) == 1, "panel was rebuilt alongside the still-running worker"
    assert not _RELEASE_WORKERS.is_set()
    assert packages_ensure._packages_ensure_worker_alive(_ENSURE_CAR_ID)

    body = r.get_json()
    # The worker is still going, so the client must be told to come back.
    assert body["window_sticker_status"] == STICKER_STATUS_FETCHING
    assert body["window_sticker_fetch_in_flight"] is True
    assert body["window_sticker_should_retry"] is True
    assert body["window_sticker_retry_after_seconds"] > 0


def test_endpoint_repeat_views_do_not_re_pay_the_budget(monkeypatch, ensure_client):
    """Second and later views must not restart the pipeline (the cooldown)."""
    import backend.enrichment.listing_packages_service as lps

    post, _state = ensure_client
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "1.0")
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS", "300")

    calls = []

    def fetch(car_id, **kw):
        calls.append(car_id)
        return {"ok": True, "car_id": car_id}

    monkeypatch.setattr(lps, "ensure_listing_packages_for_car", fetch)

    first_r = post()
    assert first_r.status_code == 200
    # Paying the budget again means entering the budgeted runner again.
    runs = _spy(monkeypatch, "_run_packages_ensure_with_budget")
    for _ in range(4):
        r = post()
        assert r.status_code == 200
    assert len(calls) == 1, f"pipeline ran {len(calls)} times for one car"
    assert runs == [], f"repeat views entered the budgeted runner {len(runs)} times"
    body = r.get_json()
    assert body.get("window_sticker_ensure_skipped") == ENSURE_COOLDOWN


def test_cooldown_skipped_view_never_claims_a_fetch_is_running(monkeypatch, ensure_client):
    """
    Car 125964, exactly: ``window_sticker_ensure_skipped="cooldown"`` and
    ``window_sticker_status="fetching"`` in the same response, with no thread
    alive. A cooldown-skipped view starts nothing, so it must not say "fetching".
    """
    import backend.enrichment.listing_packages_service as lps

    post, state = ensure_client
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "1.0")
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS", "300")
    # A sticker source exists (this is what used to force "fetching") but the
    # pipeline stores nothing — the shape 6,274 active listings are in.
    state["row"]["_urls"] = ["https://dealer.example.com/sticker.pdf"]
    monkeypatch.setattr(
        lps, "ensure_listing_packages_for_car", lambda car_id, **kw: {"ok": True, "car_id": car_id}
    )

    for i in range(4):
        r = post()
        body = r.get_json()
        alive = packages_ensure._packages_ensure_worker_alive(_ENSURE_CAR_ID)
        assert alive is False
        assert body["window_sticker_fetch_in_flight"] is False
        assert body["window_sticker_status"] == STICKER_STATUS_PENDING, body[
            "window_sticker_status"
        ]
        if i:
            assert body["window_sticker_ensure_skipped"] == ENSURE_COOLDOWN
            # Nothing may run until the cooldown expires, and the client is told
            # both that fact and when it lapses.
            assert body["window_sticker_should_retry"] is False
            assert 0 < body["window_sticker_retry_after_seconds"] <= 300


def test_settled_states_tell_the_client_to_stop(monkeypatch, ensure_client):
    """``unavailable`` / ``ready`` must not ask for a retry — that is the spinner bug."""
    import backend.enrichment.listing_packages_service as lps

    post, state = ensure_client
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS", "0")
    monkeypatch.setattr(
        lps, "ensure_listing_packages_for_car", lambda car_id, **kw: {"ok": True, "car_id": car_id}
    )

    r = post()
    body = r.get_json()
    assert body["window_sticker_status"] == STICKER_STATUS_UNAVAILABLE
    assert body["window_sticker_should_retry"] is False
    assert body["window_sticker_retry_after_seconds"] is None

    state["row"]["_stored"] = True
    packages_ensure._packages_ensure_panel_cache.clear()
    r = post()
    body = r.get_json()
    assert body["window_sticker_status"] == STICKER_STATUS_READY
    assert body["window_sticker_should_retry"] is False
    assert body["window_sticker_retry_after_seconds"] is None


def test_polling_client_sees_the_sticker_the_background_worker_stored(monkeypatch, ensure_client):
    """
    The regression this whole change exists for. On the real car 91583 the XHR
    answered "fetching" at 6.01s and the worker stored a Monroney PDF 18.9s
    later that the shopper never saw, because the browser asks once.

    Drive the loop the contract promises — retry while ``should_retry``, sleeping
    ``retry_after_seconds`` — and it must terminate on the stored sticker. The
    client's sleep is faked: the fetch lands during the client's second wait.
    """
    import backend.enrichment.listing_packages_service as lps

    post, state = ensure_client
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "0.1")
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS", "300")
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_PANEL_TTL_SECONDS", "300")
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_POLL_SECONDS", "0.25")

    def slow(car_id, **kw):
        _wait_for_release()  # stands in for the 18.9s real OEM fetch
        state["row"]["_stored"] = True
        return {"ok": True, "car_id": car_id}

    monkeypatch.setattr(lps, "ensure_listing_packages_for_car", slow)

    slept = []

    def client_sleep(seconds: float) -> None:
        slept.append(seconds)
        if len(slept) == 2:
            _release_and_join_workers()

    body = post().get_json()
    assert body["window_sticker_status"] == STICKER_STATUS_FETCHING
    assert body["window_sticker_should_retry"] is True

    seen = [body["window_sticker_status"]]
    for _ in range(40):
        if not body["window_sticker_should_retry"]:
            break
        client_sleep(float(body["window_sticker_retry_after_seconds"]))
        body = post().get_json()
        seen.append(body["window_sticker_status"])
    assert body["window_sticker_should_retry"] is False
    assert body["window_sticker_status"] == STICKER_STATUS_READY, seen
    assert body["window_sticker_available"] is True
    assert seen[:2] == [STICKER_STATUS_FETCHING, STICKER_STATUS_FETCHING], seen
    assert slept == [0.25, 0.25], slept


def test_endpoint_status_is_stable_across_repeat_views(monkeypatch, ensure_client):
    """
    Car 434357 answered "unavailable" then "hidden" on consecutive calls — the
    panel would flicker. The status must be a function of the row, so repeated
    calls on an unchanged row must agree.
    """
    import backend.enrichment.listing_packages_service as lps

    post, _state = ensure_client
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "1.0")
    monkeypatch.setattr(
        lps, "ensure_listing_packages_for_car", lambda car_id, **kw: {"ok": True, "car_id": car_id}
    )

    seen = []
    panel = []
    for _ in range(6):
        r = post()
        body = r.get_json()
        seen.append(body["window_sticker_status"])
        panel.append(
            json.dumps(
                {k: body.get(k) for k in packages_ensure.PACKAGES_ENSURE_PANEL_FIELDS},
                sort_keys=True,
                default=str,
            )
        )
    assert len(set(seen)) == 1, f"status flapped: {seen}"
    assert len(set(panel)) == 1, "panel fields changed between identical calls"


def test_endpoint_replays_snapshot_while_a_worker_is_running(monkeypatch, ensure_client):
    """
    While a fetch is in flight the row is being rewritten under us; answering
    from a half-written row is exactly how the status flipped. Any view landing
    in that window gets the payload from just before the worker started.
    """
    import backend.enrichment.listing_packages_service as lps

    post, state = ensure_client
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "0.1")

    def slow(car_id, **kw):
        # Mutate the row mid-flight, the way the real pipeline does.
        state["row"]["_urls"] = ["https://dealer.example.com/sticker.pdf"]
        _wait_for_release()
        return {"ok": True, "car_id": car_id}

    monkeypatch.setattr(lps, "ensure_listing_packages_for_car", slow)

    first = post().get_json()
    base_panel = {k: first.get(k) for k in packages_ensure.PACKAGES_ENSURE_PANEL_FIELDS}
    # The worker mutated the row on entry; the panel content must still be the
    # pre-worker snapshot, and every view in this window must agree.
    assert first["window_sticker_source_known"] is False
    assert first["window_sticker_status"] == STICKER_STATUS_FETCHING
    # Views in the window answer from the snapshot: no panel build, no budget.
    panel_builds = _spy(monkeypatch, "_packages_ensure_panel_snapshot")
    runs = _spy(monkeypatch, "_run_packages_ensure_with_budget")
    for _ in range(3):
        body = post().get_json()
        assert {k: body.get(k) for k in packages_ensure.PACKAGES_ENSURE_PANEL_FIELDS} == base_panel
        # Fetch state is live and honest: a worker really is running.
        assert body["window_sticker_fetch_in_flight"] is True
        assert body["window_sticker_status"] == STICKER_STATUS_FETCHING
        assert body["window_sticker_should_retry"] is True
    assert panel_builds == [], "an in-flight view rebuilt the panel from the half-written row"
    assert runs == [], "an in-flight view paid the budget again"


def test_endpoint_shows_what_the_worker_stored_on_the_next_view(monkeypatch, ensure_client):
    """
    The cooldown and the panel cache must never hide a real result: when the
    background worker finishes, the very next view has to see it.
    """
    import backend.enrichment.listing_packages_service as lps

    post, state = ensure_client
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_BUDGET_SECONDS", "0.1")
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_COOLDOWN_SECONDS", "300")
    monkeypatch.setenv("CAR_PACKAGES_ENSURE_PANEL_TTL_SECONDS", "300")

    done = threading.Event()

    def slow(car_id, **kw):
        _wait_for_release()
        state["row"]["_urls"] = ["https://dealer.example.com/sticker.pdf"]
        done.set()
        return {"ok": True, "car_id": car_id}

    monkeypatch.setattr(lps, "ensure_listing_packages_for_car", slow)

    first = post()
    assert first.get_json()["window_sticker_status"] == STICKER_STATUS_FETCHING
    # The worker's own completion hook drops the cached panel build.
    _release_and_join_workers()
    assert done.is_set()
    body = post().get_json()
    # It stored a source URL but no sticker file: source known, nothing running.
    assert body["window_sticker_status"] == STICKER_STATUS_PENDING
    assert body["window_sticker_fetch_in_flight"] is False
    assert body["window_sticker_oem_url"] == "https://dealer.example.com/sticker.pdf"


def test_endpoint_always_reports_the_contract_fields(monkeypatch, ensure_client):
    import backend.enrichment.listing_packages_service as lps

    post, _state = ensure_client
    monkeypatch.setattr(
        lps, "ensure_listing_packages_for_car", lambda car_id, **kw: {"ok": True, "car_id": car_id}
    )
    body = post().get_json()
    for field in packages_ensure.PACKAGES_ENSURE_RESPONSE_FIELDS:
        assert field in body, f"{field} missing from the ensure payload"
