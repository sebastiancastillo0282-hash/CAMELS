from __future__ import annotations

"""Scoring pipeline orchestration."""

import logging
from dataclasses import dataclass, field
from pathlib import Path

from .config import load_scoring_config
from .engine import ScoringEngine
from .models import CompositeScore, ScoringSummary
from .repository import IndicatorRepository
from .storage import ScoringStore

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PipelineSummary(ScoringSummary):
    """Alias of :class:`ScoringSummary` for stage logging."""

    scores: list[CompositeScore] = field(default_factory=list)


def run_pipeline(*, sqlite_path: Path, config_path: Path, run_id: str) -> PipelineSummary:
    """Execute the scoring pipeline."""

    config = load_scoring_config(config_path)
    repository = IndicatorRepository(sqlite_path)
    banks = repository.bank_profiles()
    if not banks:
        logger.warning("No banks available in registry; skipping scoring stage.")
        return PipelineSummary(0, 0, 0, None)

    snapshots = repository.latest_snapshots()
    engine = ScoringEngine(config)
    output = engine.score_all(banks, snapshots)

    store = ScoringStore(sqlite_path)
    store.persist(run_id, output.scores)

    if output.banks_with_values < len(banks):
        logger.warning(
            "%d bank(s) lacked sufficient indicator history for scoring.",
            len(banks) - output.banks_with_values,
        )
    logger.info(
        "Scoring complete for %d bank(s); %d indicator value(s) evaluated.",
        len(banks),
        output.indicators_with_values,
    )
    return PipelineSummary(
        banks_evaluated=len(banks),
        banks_with_data=output.banks_with_values,
        indicators_with_values=output.indicators_with_values,
        latest_period=output.latest_period,
        scores=output.scores,
    )


__all__ = ["PipelineSummary", "run_pipeline"]
