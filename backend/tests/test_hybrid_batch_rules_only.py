"""run_hybrid_batch is rules-only after the LLM adjudication branch was removed (P17B.9, D-DC6)."""
import importlib
import inspect

import pytest

from backend.intelligence.pipeline import orchestrator
from backend.scraping.models import SiteResult


def test_llm_adjudication_symbols_gone():
    for mod in ("backend.intelligence.llm", "backend.intelligence.agents"):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(mod)
    assert not hasattr(orchestrator, "default_llm_client")
    assert not hasattr(orchestrator, "run_llm_adjudication")
    params = inspect.signature(orchestrator.run_hybrid_batch).parameters
    assert not {"use_adjudicator", "llm_client", "llm_model"} & set(params)


def test_batch_runs_rules_only_without_network():
    sr = SiteResult(url="https://a.example", fetch_mode="requests", homepage_loaded=False, final_url="https://a.example", final_status="fetch_failed")
    res = orchestrator.run_hybrid_batch(lambda url: sr, [{"url": "https://a.example"}])
    assert len(res.sites) == 1
    out = res.sites[0]
    assert out.source == "rules_only"
    assert out.adjudication is None
    assert res.summary.adjudication_invoked == 0
