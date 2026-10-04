"""App-wide web plumbing installed on the Flask app by ``backend.main``.

Split out of ``backend/main.py`` (monolith audit 2026-10-01, W1). Each module
exposes a ``register_*(app)`` that installs its hooks; ``backend.main`` calls
them at fixed points so the before/after_request order is unchanged
(pinned by backend/tests/test_app_surface_golden.py):

* :mod:`backend.web.static` -- static cache-buster, precompressed static view,
  static cache headers.
* :mod:`backend.web.templating` -- gzip of large JSON/HTML, the global template
  context processor, jinja filters, the 404 handler.
* :mod:`backend.web.security` -- CSP nonce, CSRF hook, paid-org billing gate,
  security/CSP response headers.
"""
