"""Command line interface for the CAMELS workflow."""
from __future__ import annotations

import importlib
import logging
import logging.config
import os
from pathlib import Path
from typing import Iterable, List, Optional

import typer
import yaml
from dotenv import load_dotenv
from rich.console import Console

load_dotenv()

console = Console()


def configure_logging() -> None:
    """Configure logging using YAML/INI files or basic configuration."""
    config_candidates = []
    if config_env := os.getenv("LOGGING_CONFIG"):
        config_candidates.append(Path(config_env))
    config_candidates.extend(
        Path(name) for name in ("logging.yaml", "logging.yml", "logging.ini")
    )

    for config_path in config_candidates:
        if not config_path.exists():
            continue
        suffix = config_path.suffix.lower()
        try:
            if suffix in {".yaml", ".yml"}:
                with config_path.open("r", encoding="utf-8") as handle:
                    config = yaml.safe_load(handle)
                logging.config.dictConfig(config)
            else:
                logging.config.fileConfig(
                    config_path, disable_existing_loggers=False
                )
            return
        except Exception as exc:  # pragma: no cover - safety net for config errors
            console.print(
                f"[red]Failed to load logging config {config_path}: {exc}. Falling back to basic logging.[/]"
            )
            break

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


configure_logging()

app = typer.Typer(help="Run the CAMELS analytics workflow.")

STAGES = {
    "ingest": ("camels.ingestion", "run"),
    "normalize": ("camels.normalization", "run"),
    "score": ("camels.scoring", "run"),
    "dashboard": ("camels.dashboard", "run"),
    "export": ("camels.export", "run"),
    "audit": ("camels.audit", "run"),
}


def _execute(stage: str) -> None:
    module_name, attr = STAGES[stage]
    module = importlib.import_module(module_name)
    stage_fn = getattr(module, attr)
    console.log(f"Running [bold]{stage}[/] stage via {module_name}.{attr}()")
    stage_fn()


def _resolve_stages(stages: Optional[Iterable[str]]) -> List[str]:
    if not stages:
        return list(STAGES.keys())
    invalid = [stage for stage in stages if stage not in STAGES]
    if invalid:
        raise typer.BadParameter(f"Unknown stages: {', '.join(invalid)}")
    return list(dict.fromkeys(stages))


@app.command()
def run(
    stages: Optional[List[str]] = typer.Argument(
        None, help="Stages to run in order. Defaults to all."
    )
) -> None:
    """Run the full pipeline, optionally limiting to specific stages."""
    for stage in _resolve_stages(stages):
        _execute(stage)


@app.command()
def ingest() -> None:
    """Run only the ingestion stage."""
    _execute("ingest")


@app.command()
def normalize() -> None:
    """Run only the normalization stage."""
    _execute("normalize")


@app.command()
def score() -> None:
    """Run only the scoring stage."""
    _execute("score")


@app.command()
def dashboard() -> None:
    """Run only the dashboard stage."""
    _execute("dashboard")


@app.command()
def export() -> None:
    """Run only the export stage."""
    _execute("export")


@app.command()
def audit() -> None:
    """Run only the audit stage."""
    _execute("audit")


if __name__ == "__main__":
    app()
