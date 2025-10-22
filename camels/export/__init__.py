"""CAMELS export stage."""
from __future__ import annotations

import logging

from camels.core import register_stage
from camels.core.stage import StageContext

from .generators import ExportGenerator

logger = logging.getLogger(__name__)


@register_stage("export", "Generate committee-ready CSV/Excel extracts.")
def run(context: StageContext) -> None:
    """Produce consolidated CSV/Excel files for the latest scoring run."""

    generator = ExportGenerator(context.settings.sqlite_path, context.settings.output_dir)
    summary = generator.generate(context.run_id)
    if summary.portfolio_rows == 0 and summary.indicator_rows == 0:
        logger.warning(
            "No scoring results available for export in run %s; skipping file generation.",
            context.run_id,
        )
        return
    logger.info(
        "Generated %d portfolio row(s) and %d indicator row(s) for run %s.",
        summary.portfolio_rows,
        summary.indicator_rows,
        context.run_id,
    )
    for artifact in summary.files:
        logger.info("Export artifact written to %s", artifact)
