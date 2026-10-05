"""Sarrafi Collection — Flask web application."""

from backend.utils.project_env import load_project_dotenv

load_project_dotenv()

from backend.utils.kmac_vault import load_kmac_vault_secrets

load_kmac_vault_secrets()

# Central env access (single read point; see backend/config.py).
from backend.config import Config

import json
import logging
import os
import re
import sqlite3
import time
from datetime import timedelta

from flask import Flask, abort, make_response
from werkzeug.exceptions import HTTPException

from backend.intelligence.ai.agent import run_car_page_chat, run_compare_chat
from backend.auth.apple_oauth import bp as apple_oauth_bp
from backend.auth.google_oauth import bp as google_oauth_bp
from backend.billing.routes import bp as billing_bp
from backend.billing.catalog import (
    FEATURE_AI_CAR_CHAT,
    FEATURE_AI_COMPARE_CHAT,
    FEATURE_MARKET_INTEL,
    FEATURE_NEARBY_DEALERS,
    FEATURE_PACKAGES_ENSURE,
    FEATURE_VEHICLE_HISTORY,
    FEATURE_WINDOW_STICKER,
    get_plan,
    minimum_plan_for_feature,
)
from backend.dealer.admin import store_admin_bp

# Site-admin hub pages (must load before first url_for in templates).
import backend.dealer.admin.data_quality_hub  # noqa: F401
import backend.dealer.admin.scanner_ops_hub  # noqa: F401

from backend.dealer.routes import bp as dealer_portal_bp
from backend.dev.console import register_dev_console
from backend.routes.ai_narrate_bp import ai_narrate_bp
from backend.routes.ai_chat_bp import ai_chat_bp
from backend.routes.health import bp as health_api_bp
from backend.dev.routes import dev_bp
from backend.db.admin_users_db import init_admin_db
from backend.db.dealer_portal_db import init_dealer_portal_db
from backend.db.inventory_db import (
    clear_search_history,
    create_saved_search,
    delete_saved_search,
    delete_search_history_entry,
    get_car_by_id,
    get_cars_by_ids,
    get_filter_options,
    get_saved_car_ids,
    hidden_dealer_ids_for_user,
    hide_dealer,
    init_inventory_db,
    is_car_saved,
    is_dealer_hidden,
    list_hidden_dealers,
    list_saved_searches,
    list_search_history,
    listings_geo_coords_maps,
    record_search_history,
    save_car,
    search_cars,
    search_cars_by_make_model_pairs,
    serialize_car_for_listings_grid,
    serialize_cars_for_listings_grid,
    unhide_dealer,
    unsave_car,
)
from backend.db.user_history_db import (
    count_viewed_cars,
    get_recent_compared_car_ids,
    get_recent_viewed_car_ids,
    record_car_view,
    record_compare_session,
)
from backend.db.users_db import (
    check_user,
    get_user_by_login,
    init_users_db,
)
from backend.enrichment.knowledge_engine import prepare_car_detail_context
from backend.listings.geo_session import (
    apply_listings_geo_to_session,
    listings_geo_kwargs_from_session,
    persist_listings_geo_from_request,
)
from backend.listings.routes import listings_page
from backend.utils.car_serialize import serialize_car_for_api
from backend.utils.listing_completeness import INCOMPLETE_FIELD_LABELS
from backend.utils.car_chat_policy import car_chat_rate_limits, car_chat_user_daily_limit, web_research_playwright_allowed
from backend.utils.client_ip import client_ip as _client_ip_from_request
from backend.utils.client_ip import trust_proxy_headers
from backend.utils.query_parser import parse_natural_query
from backend.utils.runtime_env import is_production_env, session_cookie_secure_default
from backend.utils.roles import is_admin_role

_logger = logging.getLogger(__name__)


from backend.web.security import register_security  # noqa: E402
from backend.web.static import register_static  # noqa: E402
from backend.web.templating import register_templating  # noqa: E402

app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static",
)

_raw_secret = Config.secret_key_raw()
if is_production_env():
    if not _raw_secret:
        raise RuntimeError(
            "SECRET_KEY or FLASK_SECRET_KEY must be set when FLASK_ENV=production (SEC-001)."
        )
    app.secret_key = _raw_secret
else:
    import secrets as _secrets_mod
    app.secret_key = _raw_secret or _secrets_mod.token_hex(32)

# Cap JSON POST bodies (smart search, chat) and allow dealer multipart uploads (8 MiB+).
app.config["MAX_CONTENT_LENGTH"] = Config.MAX_REQUEST_BODY_BYTES

if trust_proxy_headers():
    # Railway's edge terminates TLS and sets X-Forwarded-Proto; without this the app
    # thinks it is served over http and builds http:// redirects and absolute URLs.
    # x_for stays 0: client_ip() reads X-Forwarded-For itself (TRUSTED_PROXY_HOPS).
    from werkzeug.middleware.proxy_fix import ProxyFix

    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=0, x_proto=1, x_host=0)

app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = session_cookie_secure_default()
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=14)
app.config["SESSION_REFRESH_EACH_REQUEST"] = True

# Static assets: year-long cache, ?v= cache-buster, precompressed siblings.
register_static(app)



from backend.utils.production_security import assert_production_security_config

assert_production_security_config()

from backend.db.inventory_pg import assert_inventory_backend_configured

assert_inventory_backend_configured()

init_users_db()
init_admin_db()
init_inventory_db()
init_dealer_portal_db()

from backend.scanner.job_queue import init_job_queue_schema
init_job_queue_schema()


def _prewarm_listings_inventory_cache() -> None:
    """
    Background-warm the EPA dictionary index a CAR page needs (well under a second).

    It used to go on to build the whole-fleet listings grid (214k cars, tens of
    seconds of Python, several GB resident). Owner decision 2026-09-28: the listings
    page asks for the shopper's radius only, served from the persisted card store
    (``grid_cards_repo``), so no process holds the fleet and there is nothing to
    prewarm. Deliberately left OFF -- do not add a grid prewarm back.
    """
    import threading

    def _run() -> None:
        try:
            from backend.enrichment.dictionary_catalog import _epa_paths_by_make_norm

            t0 = time.perf_counter()
            makes = len(_epa_paths_by_make_norm())
            _logger.info(
                "EPA dictionary index prewarmed (%d makes, %.1fs)",
                makes,
                time.perf_counter() - t0,
            )
        except Exception:
            _logger.exception("EPA dictionary index prewarm failed")

    threading.Thread(target=_run, name="listings-prewarm", daemon=True).start()


# Under gunicorn this runs in the ARBITER (--preload imports the app there), which
# would warm an index that serves nothing -- see gunicorn.conf.py, whose
# post_fork hook starts the prewarm in each worker instead. Everything else
# (run.py dev server, tests, one-off scripts) keeps the import-time behaviour.
if not os.environ.get("DS_GUNICORN_ARBITER"):
    _prewarm_listings_inventory_cache()
app.register_blueprint(dev_bp, url_prefix="/dev")
app.register_blueprint(store_admin_bp)

from backend.dealer.admin.operator_api import register_admin_operator_api

register_admin_operator_api(app)
app.register_blueprint(dealer_portal_bp)
app.register_blueprint(billing_bp)
app.register_blueprint(google_oauth_bp)
app.register_blueprint(apple_oauth_bp)
app.register_blueprint(ai_narrate_bp)
app.register_blueprint(ai_chat_bp)
app.register_blueprint(health_api_bp)
register_dev_console(app)

# Route modules extracted from this file. Each exposes ``register(app)`` and
# keeps the original bare endpoint names (templates, the CSRF hook, and the
# billing gate all match endpoints by bare name). The modules resolve
# main-owned helpers through this module at request time so existing
# ``backend.main`` monkeypatch targets and ``importlib.reload(backend.main)``
# keep working — see backend/routes/_shared.py.
from backend.routes import admin_dealer_api as _admin_dealer_api_routes  # noqa: E402
from backend.routes import cars_pages as _cars_pages_routes  # noqa: E402
from backend.routes import community_api as _community_api_routes  # noqa: E402
from backend.routes import dealers_recalls as _dealers_recalls_routes  # noqa: E402
from backend.routes import dealer_reviews as _dealer_reviews_routes  # noqa: E402
from backend.routes import dealership_page as _dealership_page_routes  # noqa: E402
from backend.routes import fuel_api as _fuel_api_routes  # noqa: E402
from backend.routes import home_dashboard as _home_dashboard_routes  # noqa: E402
from backend.routes import listings_api as _listings_api_routes  # noqa: E402
from backend.routes import site_misc as _site_misc_routes  # noqa: E402

_site_misc_routes.register(app)
_home_dashboard_routes.register(app)
_listings_api_routes.register(app)
_cars_pages_routes.register(app)
_community_api_routes.register(app)
_dealers_recalls_routes.register(app)
_dealership_page_routes.register(app)
_dealer_reviews_routes.register(app)
_fuel_api_routes.register(app)
_admin_dealer_api_routes.register(app)

# App-wide hooks, in the original order (pinned by test_app_surface_golden):
# gzip after_request + context processor + filters + 404, then the CSP nonce,
# CSRF and billing-gate before_request hooks and the CSP headers after_request.
register_templating(app)
register_security(app)


# Auth pages + /api/auth/* + MFA stubs, then account pages (bare endpoint names kept).
from backend.auth import pages as _auth_pages  # noqa: E402
from backend.routes import account as _account_routes  # noqa: E402

_auth_pages.register(app)
_account_routes.register(app)
