from __future__ import annotations

import pytest

from camels.core.registry import StageRegistry
from camels.core.runner import StageRunner
from camels.core.stage import StageContext


def _dummy_stage(context: StageContext) -> None:  # pragma: no cover - simple stub
    del context


def test_registry_prevents_duplicate_registration() -> None:
    registry = StageRegistry()
    registry.register("demo", _dummy_stage)
    with pytest.raises(ValueError):
        registry.register("demo", _dummy_stage)


def test_stage_runner_resolve_filters_duplicates() -> None:
    registry = StageRegistry()
    registry.register("one", _dummy_stage)
    registry.register("two", _dummy_stage)
    runner = StageRunner(registry)

    assert runner.resolve(["two", "one", "two"]) == ["two", "one"]
    assert runner.resolve(None) == ["one", "two"]

    with pytest.raises(ValueError):
        runner.resolve(["missing"])
