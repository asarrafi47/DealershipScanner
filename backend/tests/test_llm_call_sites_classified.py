"""C2 review: every place that calls a model is classified, so a new chat path cannot skip the budget gate.

A model call site is a call of ``complete()`` (from ``backend.llm.client`` or ``backend.utils.llm_client``, under
any import name), a ``.messages.create`` or ``.messages.stream``, or an Anthropic client constructor. Each file
that has one must be in ALLOWED with a class and a reason. A new file, or a file whose class is wrong, fails here:
add it to the list on purpose (and, for anything a shopper can trigger, wrap it in ``chat_budget.guard``).

Classes: ``guarded-web`` (reachable from a shopper's request; card C3 wraps its route in the gate),
``operator-only`` (staff or admin pages), ``scanner-batch`` (the scan and enrichment pipeline, no shopper),
``script`` (run by hand), ``definition`` (the model client itself).
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_LLM_MODULES = {"backend.llm.client", "backend.llm", "backend.utils.llm_client"}
_CTORS = {"Anthropic", "AsyncAnthropic"}

ALLOWED: dict[str, tuple[str, str]] = {
    "backend/llm/client.py": ("definition", "complete() and the SDK client; reports usage to an active guard"),
    "backend/utils/llm_client.py": ("definition", "provider router: forwards to complete(), so usage reaches the guard"),
    "backend/intelligence/ai/agent.py": ("guarded-web", "car-page and compare chat answers; the chat routes get the guard in card C3"),
    "backend/routes/ai_chat_bp.py": ("guarded-web", "general chat route and its query rewrite; guarded in card C3"),
    "backend/utils/vehicle_narrator.py": ("guarded-web", "car narrate route (/api/car/<id>/narrate); guarded in card C3"),
    "backend/enrichment/service.py": ("scanner-batch", "catalog and spec enrichment, run by the scan pipeline"),
    "backend/enrichment/window_sticker_service.py": ("scanner-batch", "window sticker reading, run by the scan pipeline"),
    "backend/scanner/job_diagnosis.py": ("scanner-batch", "scan job diagnosis, operator tooling inside the scanner"),
    "backend/scanner/post_scan/pipeline.py": ("scanner-batch", "post-scan colour reading"),
    "backend/utils/listing_description_extract.py": ("scanner-batch", "listing description claims, run at scan time"),
    "backend/vision/claude_vision.py": ("scanner-batch", "photo classification, run at scan time"),
    "backend/vision/claude_rate_limit.py": ("scanner-batch", "back-compat rate-limited messages.create shim for scan-time vision"),
    "backend/scripts/analyze_brochure_with_llm.py": ("script", "brochure analysis, run by hand"),
}
CLASSES = {"guarded-web", "operator-only", "scanner-batch", "script", "definition"}


def _scan_tree(tree: ast.AST) -> set[str]:
    """The model calls in one parsed file (empty when it has none)."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _LLM_MODULES:
            names.update(a.asname or a.name for a in node.names if a.name in {"complete", "call_with_retry"})
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Name) and (f.id in names or f.id in _CTORS):
            found.add(f.id)
        elif isinstance(f, ast.Attribute):
            if f.attr in _CTORS:
                found.add(f.attr)
            elif f.attr == "complete" and isinstance(f.value, ast.Name) and f.value.id in {"llm_client", "llm", "client"}:
                found.add("complete")
            elif f.attr in {"create", "stream"} and isinstance(f.value, ast.Attribute) and f.value.attr == "messages":
                found.add("messages." + f.attr)
    return found


def _call_sites() -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for path in (ROOT / "backend").rglob("*.py"):
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith("backend/tests/"):
            continue
        hits = _scan_tree(ast.parse(path.read_text(encoding="utf-8")))
        if hits:
            found[rel] = hits
    return found


def test_llm_call_sites_classified():
    sites = _call_sites()
    assert sites, "the scan found no model call at all: the scanner of this test is broken"
    unclassified = sorted(set(sites) - set(ALLOWED))
    assert not unclassified, (
        "model call site(s) not in the allow-list of test_llm_call_sites_classified: %s. Classify each with a reason "
        "(guarded-web, operator-only, scanner-batch, script) and wrap anything a shopper can reach in chat_budget.guard."
        % unclassified
    )


def test_every_allow_list_entry_is_real_and_has_a_class_and_a_reason():
    sites = _call_sites()
    for rel, (klass, reason) in ALLOWED.items():
        assert klass in CLASSES, rel
        assert len(reason) > 15, rel
        assert rel in sites, f"{rel} no longer calls a model: drop it from the allow-list"


def test_the_scan_sees_aliases_constructors_and_raw_sdk_calls():
    """The main test would pass for the wrong reason if its scanner missed these shapes."""
    alias = "from backend.llm.client import complete as ask\nask('hi')\n"
    assert _scan_tree(ast.parse(alias)) == {"ask"}
    router = "from backend.utils.llm_client import complete\ncomplete('hi')\n"
    assert _scan_tree(ast.parse(router)) == {"complete"}
    sdk = "import anthropic\nc = anthropic.Anthropic()\nc.messages.create(model='m')\nc.messages.stream()\n"
    assert _scan_tree(ast.parse(sdk)) == {"Anthropic", "messages.create", "messages.stream"}
    method = "from backend.llm import client as llm_client\nllm_client.complete('x')\n"
    assert _scan_tree(ast.parse(method)) == {"complete"}
    assert _scan_tree(ast.parse("def complete(x):\n    return x\ncomplete(1)\n")) == set()  # an unrelated local helper


def test_a_guarded_web_file_that_is_not_yet_wrapped_is_named_in_the_pr_follow_up_list():
    """Documentation guard: card C3 owns wrapping these, and the task file says so."""
    tasks = (ROOT / "docs" / "auth_sms" / "TASKS.md").read_text(encoding="utf-8")
    assert "test_llm_call_sites_classified" in tasks
    assert "### C3." in tasks
