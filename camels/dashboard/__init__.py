"""CAMELS dashboard stage."""
from __future__ import annotations

import logging

from camels.core import register_stage
from camels.core.stage import StageContext

logger = logging.getLogger(__name__)


@register_stage("dashboard", "Serve the local CAMELS analytics dashboard.")
def run(context: StageContext) -> None:
    """Placeholder dashboard implementation."""

    logger.info(
        "Dashboard placeholder listening on %s:%s",
        context.settings.dashboard_host,
        context.settings.dashboard_port,
    )
    logger.info("Dashboard stage placeholder executed. Implement UI startup here.")
