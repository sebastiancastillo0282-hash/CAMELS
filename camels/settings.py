"""Environment-driven configuration for CAMELS."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    """Runtime settings loaded from environment variables."""

    data_dir: Path
    output_dir: Path
    sqlite_path: Path
    scoring_config: Path
    dashboard_host: str
    dashboard_port: int
    log_level: str

    @classmethod
    def load(cls) -> "Settings":
        """Load settings from environment variables with sensible defaults."""

        data_dir = Path(os.getenv("CAMELS_DATA_DIR", "data"))
        output_dir = Path(os.getenv("CAMELS_OUTPUT_DIR", "artifacts"))
        sqlite_path = Path(os.getenv("CAMELS_DB_PATH", "camels.sqlite"))
        dashboard_host = os.getenv("CAMELS_DASHBOARD_HOST", "127.0.0.1")
        dashboard_port = int(os.getenv("CAMELS_DASHBOARD_PORT", "8501"))
        log_level = os.getenv("LOG_LEVEL", "INFO")
        scoring_config = Path(
            os.getenv("CAMELS_SCORING_CONFIG", "config/camels_thresholds.yaml")
        )
        return cls(
            data_dir=data_dir,
            output_dir=output_dir,
            sqlite_path=sqlite_path,
            scoring_config=scoring_config,
            dashboard_host=dashboard_host,
            dashboard_port=dashboard_port,
            log_level=log_level,
        )

    def ensure_directories(self) -> None:
        """Create directories required for the runtime to operate."""

        for path in {self.data_dir, self.output_dir, self.sqlite_path.parent}:
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
