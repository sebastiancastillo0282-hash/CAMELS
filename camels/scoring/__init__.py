"""CAMELS scoring stage."""
from __future__ import annotations

import logging

from camels.audit.storage import AuditStore
from camels.core import register_stage
from camels.core.stage import StageContext
from camels.core.utils import pipeline_version

from .pipeline import run_pipeline

logger = logging.getLogger(__name__)


@register_stage("score", "Compute CAMELS pillar and composite scores.")
def run(context: StageContext) -> None:
    """Execute the scoring pipeline."""

    logger.info(
        "Starting scoring with database %s and config %s",
        context.settings.sqlite_path,
        context.settings.scoring_config,
    )
    try:
        summary = run_pipeline(
            sqlite_path=context.settings.sqlite_path,
            config_path=context.settings.scoring_config,
            run_id=context.run_id,
        )
    except FileNotFoundError as exc:
        logger.error("Scoring configuration missing: %s", exc)
        raise
    logger.info(
        "Scoring summary: %d bank(s) evaluated, %d bank(s) with data, %d indicator value(s) scored, latest period %s",
        summary.banks_evaluated,
        summary.banks_with_data,
        summary.indicators_with_values,
        summary.latest_period or "n/a",
    )

    store = AuditStore(context.settings.sqlite_path)
    store.prepare_stage(context.run_id, "score")
    recorded = store.record_scores(
        context.run_id,
        summary.scores,
        pipeline_version=pipeline_version(),
    )
    logger.info("Recorded %d scoring audit entries for run %s", recorded, context.run_id)
