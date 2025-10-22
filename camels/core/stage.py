"""Stage primitives for the CAMELS orchestration runtime."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from camels.settings import Settings


class StageCallable(Protocol):
    """Callable protocol for a pipeline stage."""

    def __call__(self, context: "StageContext") -> None:
        """Execute the stage logic."""


@dataclass(slots=True)
class StageContext:
    """Context object passed to every stage run."""

    settings: Settings
    run_id: str
    timestamp: datetime
    workspace: Path


@dataclass(slots=True, frozen=True)
class StageDefinition:
    """Metadata about a registered stage."""

    name: str
    callable: StageCallable
    description: str
    module: str
