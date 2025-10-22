"""Command line interface for the CAMELS workflow."""
from __future__ import annotations

import argparse
import logging
import logging.config
import os
from pathlib import Path
from typing import Iterable, List, Optional

from camels import StageContext, StageRunner, bootstrap, create_default_context, registry
from camels.settings import Settings

def load_environment() -> None:
    candidates = []
    if env_file := os.getenv("ENV_FILE"):
        candidates.append(Path(env_file))
    candidates.append(Path(".env"))

    for path in candidates:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"')
                os.environ.setdefault(key, value)


load_environment()

logger = logging.getLogger(__name__)


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


def _run_pipeline(stages: Optional[Iterable[str]]) -> None:
    settings, context = _context()
    try:
        resolved = runner.resolve(None if stages is None else list(stages))
    except ValueError as exc:
        print(f"Error: {exc}")
        raise SystemExit(2) from exc
    logger.info(
        "Running stages %s with data directory %s and output %s.",
        resolved,
        settings.data_dir,
        settings.output_dir,
    )
    runner.run(resolved, context)


def _single_stage(stage: str) -> None:
    settings, context = _context()
    runner.run([stage], context)


def command_run(args: argparse.Namespace) -> None:
    stages: Optional[List[str]] = args.stages if args.stages else None
    _run_pipeline(stages)


def command_stages(_: argparse.Namespace) -> None:
    print("CAMELS Registered Stages:")
    for definition in registry.items():
        print(f"- {definition.name}: {definition.description} ({definition.module})")


def command_ingest(_: argparse.Namespace) -> None:
    _single_stage("ingest")


def command_normalize(_: argparse.Namespace) -> None:
    _single_stage("normalize")


def command_score(_: argparse.Namespace) -> None:
    _single_stage("score")


def command_dashboard(_: argparse.Namespace) -> None:
    _single_stage("dashboard")


def command_export(_: argparse.Namespace) -> None:
    _single_stage("export")


def command_audit(_: argparse.Namespace) -> None:
    _single_stage("audit")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the CAMELS analytics workflow.")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    parser_run = subparsers.add_parser("run", help="Run the full pipeline")
    parser_run.add_argument(
        "stages",
        nargs="*",
        help="Optional ordered list of stages to run instead of all registered stages.",
    )
    parser_run.set_defaults(func=command_run)

    parser_stages = subparsers.add_parser("stages", help="List registered stages")
    parser_stages.set_defaults(func=command_stages)

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

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":  # pragma: no cover - entry point for CLI usage
    raise SystemExit(main())
