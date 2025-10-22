from __future__ import annotations

"""Dataclasses used across the scoring pipeline."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(slots=True)
class BankProfile:
    """Basic metadata about a bank sourced from the registry."""

    bank_id: str
    name: str
    country: str
    regulator: str


@dataclass(slots=True)
class IndicatorSnapshot:
    """Latest normalized indicator observation for a bank."""

    bank_id: str
    indicator_id: str
    pillar: str
    period: Optional[str]
    value: Optional[float]
    unit: Optional[str]
    source_id: Optional[str]
    normalization_run_id: Optional[str]
    metadata: Dict[str, object]


@dataclass(slots=True)
class IndicatorScore:
    """Scored value for a single indicator."""

    bank_id: str
    indicator_id: str
    pillar: str
    period: Optional[str]
    value: Optional[float]
    score: float
    rating: str
    weight: float
    source_id: Optional[str]
    normalization_run_id: Optional[str]
    unit: Optional[str]
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class PillarScore:
    """Aggregated score for a CAMELS pillar."""

    bank_id: str
    pillar: str
    score: float
    rating: str
    weight: float
    period: Optional[str]
    indicators: List[IndicatorScore]
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class CompositeScore:
    """Composite CAMELS score for a bank."""

    bank_id: str
    score: float
    rating: str
    period: Optional[str]
    pillars: List[PillarScore]
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ScoringSummary:
    """Summary values returned by the scoring pipeline."""

    banks_evaluated: int
    banks_with_data: int
    indicators_with_values: int
    latest_period: Optional[str]


@dataclass(slots=True)
class ScoringOutput:
    """Container for the scoring engine results."""

    scores: List[CompositeScore]
    banks_with_values: int
    indicators_with_values: int
    latest_period: Optional[str]


__all__ = [
    "BankProfile",
    "IndicatorSnapshot",
    "IndicatorScore",
    "PillarScore",
    "CompositeScore",
    "ScoringSummary",
    "ScoringOutput",
]
