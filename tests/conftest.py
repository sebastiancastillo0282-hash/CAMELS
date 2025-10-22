from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys
import types

import pytest

from camels.core.stage import StageContext
from camels.settings import Settings


if "yaml" not in sys.modules:  # Provide a lightweight stub when PyYAML is unavailable.
    yaml_stub = types.ModuleType("yaml")

    class YAMLError(Exception):
        """Fallback error used by the stub implementation."""

    def safe_load(_: object) -> dict:
        return {}

    yaml_stub.safe_load = safe_load
    yaml_stub.YAMLError = YAMLError
    sys.modules["yaml"] = yaml_stub


@pytest.fixture
def stage_context(tmp_path: Path) -> StageContext:
    """Create a temporary stage context for tests."""

    settings = Settings(
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "artifacts",
        sqlite_path=tmp_path / "camels.sqlite",
        scoring_config=Path("config/camels_thresholds.yaml"),
        dashboard_host="127.0.0.1",
        dashboard_port=8501,
        log_level="INFO",
    )
    settings.ensure_directories()
    return StageContext(
        settings=settings,
        run_id="test-run",
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        workspace=tmp_path,
    )
