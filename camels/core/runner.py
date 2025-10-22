"""Stage runner implementation for the CAMELS CLI."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable, List, Sequence

from .registry import StageRegistry
from .stage import StageContext

logger = logging.getLogger(__name__)


class StageRunner:
    """Execute registered stages sequentially."""

    def __init__(self, registry: StageRegistry) -> None:
        self._registry = registry

    def available(self) -> List[str]:
        """Return available stage names."""

        return self._registry.names()

    def run(self, stages: Sequence[str], context: StageContext) -> None:
        """Run each stage listed in *stages* with the provided context."""

        for name in stages:
            definition = self._registry.get(name)
            stage_logger = logging.getLogger(definition.module)
            stage_logger.info(
                "Starting stage '%s' (run_id=%s, timestamp=%s)",
                definition.name,
                context.run_id,
                context.timestamp.isoformat(),
            )
            start_time = datetime.utcnow()
            try:
                definition.callable(context)
            except Exception:  # pragma: no cover - pipeline failure bubble-up
                stage_logger.exception("Stage '%s' failed", definition.name)
                raise
            duration = datetime.utcnow() - start_time
            stage_logger.info(
                "Completed stage '%s' in %.2fs",
                definition.name,
                duration.total_seconds(),
            )

    def resolve(self, requested: Iterable[str] | None) -> List[str]:
        """Return a validated list of stage names based on *requested*."""

        if not requested:
            return self.available()
        missing = [name for name in requested if name not in self._registry]
        if missing:
            raise ValueError(f"Unknown stages requested: {', '.join(missing)}")
        # Preserve order while removing duplicates
        result: List[str] = []
        for name in requested:
            if name not in result:
                result.append(name)
        return result
