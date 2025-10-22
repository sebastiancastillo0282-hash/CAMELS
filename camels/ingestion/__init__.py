"""CAMELS ingestion stage."""
from __future__ import annotations

import logging
import os
import sys

from camels.audit.storage import AuditStore
from camels.core import register_stage
from camels.core.stage import StageContext
from camels.core.utils import pipeline_version

from .pipeline import run_pipeline

logger = logging.getLogger(__name__)


def _command_line() -> str:
    return os.getenv("CAMELS_COMMAND") or " ".join(sys.argv)


@register_stage("ingest", "Download, validate, and archive raw supervisory data.")
def run(context: StageContext) -> None:
    """Execute the ingestion pipeline and capture audit metadata."""

    logger.info("Starting ingestion with data directory %s", context.settings.data_dir)
    results = run_pipeline(context)
    successes = sum(1 for entry in results if entry.status == "success")
    failures = len(results) - successes
    logger.info(
        "Ingestion completed: %d success, %d failure(s)",
        successes,
        failures,
    )

    store = AuditStore(context.settings.sqlite_path)
    store.prepare_stage(context.run_id, "ingest")
    recorded = store.record_ingestions(
        context.run_id,
        results,
        pipeline_version=pipeline_version(),
        command=_command_line(),
        workspace=context.workspace,
    )
    logger.info("Recorded %d ingestion audit entries for run %s", recorded, context.run_id)
