"""Structured models for dealer-group evidence and adjudication."""

from .adjudication_result import AdjudicationResult
from .dealership import DealerCreate
from .evidence_package import EvidencePackage, FetchedPage, HeuristicScores
from .run_summary import RunSummary

__all__ = [
    "AdjudicationResult",
    "DealerCreate",
    "EvidencePackage",
    "FetchedPage",
    "HeuristicScores",
    "RunSummary",
]
