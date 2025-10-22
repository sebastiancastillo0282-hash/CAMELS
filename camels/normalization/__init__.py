"""CAMELS normalization stage."""
from __future__ import annotations

import logging

from camels.core import register_stage
from camels.core.stage import StageContext

logger = logging.getLogger(__name__)


@register_stage("normalize", "Standardize CAMELS indicators and store them in SQLite.")
def run(context: StageContext) -> None:
    """Placeholder normalization implementation."""

    logger.info("Writing normalized outputs to: %s", context.settings.sqlite_path)
    logger.info("Normalization stage placeholder executed. Implement transformation logic here.")
