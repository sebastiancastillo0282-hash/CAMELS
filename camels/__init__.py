"""CAMELS - Coordinated Analytics for Metrics, Evaluation, and Lifecycle Scoring."""
from __future__ import annotations

from datetime import datetime
from importlib import metadata
from pathlib import Path

from camels.core import StageContext, StageRunner, registry
from camels.settings import Settings

__all__ = [
    "__version__",
    "StageContext",
    "StageRunner",
    "Settings",
    "registry",
    "bootstrap",
    "create_default_context",
]


def __getattr__(name: str):  # pragma: no cover - passthrough to package metadata
    if name == "__version__":
        try:
            return metadata.version("camels")
        except metadata.PackageNotFoundError:
            return "0.0.0"
    raise AttributeError(name)


def bootstrap() -> None:
    """Import stage modules to ensure registration has occurred."""

    from camels import audit, dashboard, export, ingestion, normalization, scoring  # noqa: F401


def create_default_context(settings: Settings | None = None) -> StageContext:
    """Construct a default :class:`StageContext` for command-line runs."""

    settings = settings or Settings.load()
    settings.ensure_directories()
    run_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return StageContext(
        settings=settings,
        run_id=run_id,
        timestamp=datetime.utcnow(),
        workspace=Path.cwd(),
    )
