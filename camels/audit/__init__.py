"""CAMELS audit stage."""
from __future__ import annotations

import logging
from typing import Dict

from camels.core import register_stage
from camels.core.stage import StageContext

from .storage import AuditStore

logger = logging.getLogger(__name__)


def _summarize(records: int) -> Dict[str, int]:
    return {"records": records}


@register_stage("audit", "Collect and publish ingestion audit trails.")
def run(context: StageContext) -> None:
    """Generate persisted audit artifacts for the current run."""

    store = AuditStore(context.settings.sqlite_path)
    summary = store.export_records(run_id=context.run_id, output_dir=context.settings.output_dir)
    if summary.records == 0:
        logger.warning(
            "No audit records found for run %s; skipping artifact generation.",
            context.run_id,
        )
        return
    logger.info("Audit trail exported for run %s", context.run_id)
    for artifact in summary.files:
        logger.info("Audit artifact written to %s", artifact)
    logger.debug("Audit summary: %s", _summarize(summary.records))
