"""Stage registry utilities for the CAMELS runtime."""
from __future__ import annotations

from typing import Callable, Dict, Iterable, Iterator, List

from .stage import StageCallable, StageDefinition


class StageRegistry:
    """Keeps track of the available pipeline stages."""

    def __init__(self) -> None:
        self._stages: Dict[str, StageDefinition] = {}

    def register(self, name: str, func: StageCallable, description: str = "") -> StageCallable:
        """Register a new stage and return the callable for decorator usage."""

        if name in self._stages:
            raise ValueError(f"Stage '{name}' is already registered")
        self._stages[name] = StageDefinition(
            name=name,
            callable=func,
            description=description,
            module=func.__module__,
        )
        return func

    def get(self, name: str) -> StageDefinition:
        """Return the stage definition for *name*."""

        try:
            return self._stages[name]
        except KeyError as exc:  # pragma: no cover - defensive path
            raise KeyError(f"Stage '{name}' is not registered") from exc

    def __contains__(self, name: str) -> bool:
        return name in self._stages

    def __iter__(self) -> Iterator[StageDefinition]:
        return iter(self._stages.values())

    def names(self) -> List[str]:
        """Return registered stage names preserving insertion order."""

        return list(self._stages.keys())

    def items(self) -> Iterable[StageDefinition]:
        """Return definitions for iteration."""

        return list(self._stages.values())

    def clear(self) -> None:
        """Remove all stage registrations."""

        self._stages.clear()


registry = StageRegistry()


def register_stage(name: str, description: str = "") -> Callable[[StageCallable], StageCallable]:
    """Decorator to register a stage when defining the function."""

    def decorator(func: StageCallable) -> StageCallable:
        return registry.register(name, func, description=description)

    return decorator
