"""CAMELS normalization stage."""
from __future__ import annotations

import logging

from camels.core import register_stage
from camels.core.stage import StageContext

from .pipeline import run_pipeline

logger = logging.getLogger(__name__)


@register_stage("normalize", "Standardize CAMELS indicators and store them in SQLite.")
def run(context: StageContext) -> None:
    """Execute the normalization pipeline."""

    logger.info(
        "Starting normalization with database %s and data directory %s",
        context.settings.sqlite_path,
        context.settings.data_dir,
    )
    summary = run_pipeline(
        sqlite_path=context.settings.sqlite_path,
        data_dir=context.settings.data_dir,
        workspace=context.workspace,
        run_id=context.run_id,
    )
    logger.info(
        "Normalization summary: %d sources processed, %d skipped, %d inserted, %d updated",
        summary.processed_sources,
        summary.skipped_sources,
        summary.normalized_records,
        summary.updated_records,
    )
