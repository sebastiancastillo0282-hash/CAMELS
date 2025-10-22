"""CAMELS export stage."""
from __future__ import annotations

import logging

from camels.core import register_stage
from camels.core.stage import StageContext

logger = logging.getLogger(__name__)


@register_stage("export", "Generate committee-ready CSV/Excel extracts.")
def run(context: StageContext) -> None:
    """Placeholder export implementation."""

    logger.info("Exports will be saved to: %s", context.settings.output_dir)
    logger.info("Export stage placeholder executed. Implement export writers here.")
