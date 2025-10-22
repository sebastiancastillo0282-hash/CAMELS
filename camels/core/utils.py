"""Miscellaneous helpers for the CAMELS runtime."""
from __future__ import annotations

from importlib import metadata

__all__ = ["pipeline_version"]


def pipeline_version() -> str:
    """Return the installed CAMELS package version or a sensible default."""

    try:
        return metadata.version("camels")
    except metadata.PackageNotFoundError:  # pragma: no cover - fallback path
        return "0.0.0"
