"""Golden test: the Flask app's public surface must not drift while backend/main.py is split.

Captures URL rules (rule, endpoint, methods), request hooks per blueprint in
registration order, template context processors, non-default jinja filters,
error handlers and config keys. Function *names* are compared (not modules), so
moving a hook into another module keeps the golden green as long as the name and
order are kept.

Regenerate only on an intentional surface change:
    python -c "from backend.tests.test_app_surface_golden import capture_surface; import json; \
print(json.dumps(capture_surface(), indent=1))" > backend/tests/fixtures/app_surface.json
"""
from __future__ import annotations

import json
from pathlib import Path

from flask import Flask

FIXTURE = Path(__file__).parent / "fixtures" / "app_surface.json"
_HOOK_ATTRS = (
    "before_request_funcs",
    "after_request_funcs",
    "teardown_request_funcs",
    "template_context_processors",
)


def _name(fn) -> str:
    return getattr(fn, "__name__", repr(fn))


def capture_surface() -> dict:
    from backend.main import app

    rules = sorted(
        [r.rule, r.endpoint, sorted(set(r.methods or ()) - {"HEAD", "OPTIONS"})]
        for r in app.url_map.iter_rules()
    )
    hooks = {
        attr: {str(bp): [_name(f) for f in funcs] for bp, funcs in getattr(app, attr).items() if funcs}
        for attr in _HOOK_ATTRS
    }
    default_filters = set(Flask("golden_probe").jinja_env.filters)
    filters = sorted(set(app.jinja_env.filters) - default_filters)
    # error_handler_spec is a nested defaultdict: handling a request adds empty
    # entries for every code Flask looked up, so only non-empty ones count.
    errorhandlers = {}
    for bp, codes in app.error_handler_spec.items():
        by_code = {
            str(code): sorted(_name(f) for f in by_cls.values())
            for code, by_cls in codes.items()
            if by_cls
        }
        if by_code:
            errorhandlers[str(bp)] = by_code
    return {
        "rules": rules,
        "hooks": hooks,
        "filters": filters,
        "errorhandlers": errorhandlers,
        "config_keys": sorted(app.config.keys()),
    }


def _golden() -> dict:
    return json.loads(FIXTURE.read_text())


def test_url_rules_match_golden():
    assert capture_surface()["rules"] == _golden()["rules"]


def test_request_hooks_match_golden():
    assert capture_surface()["hooks"] == _golden()["hooks"]


def test_jinja_filters_match_golden():
    assert capture_surface()["filters"] == _golden()["filters"]


def test_error_handlers_match_golden():
    assert capture_surface()["errorhandlers"] == _golden()["errorhandlers"]


def test_config_keys_present():
    keys = set(capture_surface()["config_keys"])
    missing = [k for k in _golden()["config_keys"] if k not in keys]
    assert not missing
