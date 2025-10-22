"""CAMELS - Coordinated Analytics for Metrics, Evaluation, and Lifecycle Scoring."""

from importlib import metadata

__all__ = ["__version__"]


def __getattr__(name: str):
    if name == "__version__":
        try:
            return metadata.version("camels")
        except metadata.PackageNotFoundError:
            return "0.0.0"
    raise AttributeError(name)
