"""CAMELS scoring stage."""
from __future__ import annotations

import logging

from camels.core import register_stage
from camels.core.stage import StageContext

logger = logging.getLogger(__name__)


@register_stage("score", "Compute CAMELS pillar scores and composite results.")
def run(context: StageContext) -> None:
    """Placeholder scoring implementation."""

    logger.info("Scores will persist to: %s", context.settings.sqlite_path)
    logger.info("Scoring stage placeholder executed. Implement scoring engine here.")
