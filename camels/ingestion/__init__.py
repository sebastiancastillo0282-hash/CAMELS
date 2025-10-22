"""CAMELS ingestion stage."""
from __future__ import annotations

import logging

from camels.core import register_stage
from camels.core.stage import StageContext

from .pipeline import run_pipeline

logger = logging.getLogger(__name__)


@register_stage("ingest", "Download, validate, and archive raw supervisory data.")
def run(context: StageContext) -> None:
    """Execute the ingestion pipeline."""

    logger.info("Starting ingestion with data directory %s", context.settings.data_dir)
    results = run_pipeline(context)
    successes = sum(1 for entry in results if entry.status == "success")
    failures = len(results) - successes
    logger.info(
        "Ingestion completed: %d success, %d failure(s)",
        successes,
        failures,
    )
