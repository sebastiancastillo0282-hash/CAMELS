"""Command line interface for the CAMELS workflow."""
from __future__ import annotations

import logging
import logging.config
import os
from pathlib import Path
from typing import List, Optional

import typer
import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from camels import StageContext, StageRunner, bootstrap, create_default_context, registry
from camels.settings import Settings

def load_environment() -> None:
    candidates = []
    if env_file := os.getenv("ENV_FILE"):
        candidates.append(Path(env_file))
    candidates.append(Path(".env"))

console = Console()
app = typer.Typer(help="Run the CAMELS analytics workflow.")


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
            if suffix in {".ini", ".cfg"}:
                logging.config.fileConfig(config_path, disable_existing_loggers=False)
            else:
                print(
                    f"Skipping unsupported logging config {config_path}. Using default logging configuration."
                )
                continue
            return
        except Exception as exc:  # pragma: no cover - safety net for config errors
            print(
                f"Failed to load logging config {config_path}: {exc}. Falling back to basic logging."
            )
            break

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


configure_logging()
bootstrap()
runner = StageRunner(registry)


def _context() -> tuple[Settings, StageContext]:
    settings = Settings.load()
    settings.ensure_directories()
    context = create_default_context(settings)
    return settings, context


@app.command()
def run(
    stages: Optional[List[str]] = typer.Argument(
        None, help="Stages to run in order. Defaults to all registered stages."
    )
) -> None:
    """Run the full pipeline, optionally limiting to specific stages."""

    settings, context = _context()
    try:
        resolved = runner.resolve(stages)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.log(
        f"Running stages {resolved} with data directory {settings.data_dir} and output {settings.output_dir}."
    )
    runner.run(resolved, context)


@app.command()
def stages() -> None:
    """List registered stages and descriptions."""

    table = Table(title="CAMELS Registered Stages")
    table.add_column("Stage", style="cyan", no_wrap=True)
    table.add_column("Module", style="magenta")
    table.add_column("Description", style="green")
    for definition in registry.items():
        table.add_row(definition.name, definition.module, definition.description)
    console.print(table)


def _single_stage(stage: str) -> None:
    settings, context = _context()
    runner.run([stage], context)


@app.command()
def ingest() -> None:
    """Run only the ingestion stage."""

    _single_stage("ingest")


@app.command()
def normalize() -> None:
    """Run only the normalization stage."""

    _single_stage("normalize")


@app.command()
def score() -> None:
    """Run only the scoring stage."""

    _single_stage("score")


@app.command()
def dashboard() -> None:
    """Run only the dashboard stage."""

    _single_stage("dashboard")

    for name, func in (
        ("ingest", command_ingest),
        ("normalize", command_normalize),
        ("score", command_score),
        ("dashboard", command_dashboard),
        ("export", command_export),
        ("audit", command_audit),
    ):
        sub = subparsers.add_parser(name, help=f"Run only the {name} stage")
        sub.set_defaults(func=func)

@app.command()
def export() -> None:
    """Run only the export stage."""

    _single_stage("export")


@app.command()
def audit() -> None:
    """Run only the audit stage."""

    _single_stage("audit")


if __name__ == "__main__":  # pragma: no cover - entry point for CLI usage
    raise SystemExit(main())
