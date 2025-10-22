# CAMELS

CAMELS (Coordinated Analytics for Metrics, Evaluation, and Lifecycle Scoring) is a local
command-line toolkit that orchestrates data ingestion, normalization, scoring, dashboard
serving, exports, and audit logging for the CAMELS risk methodology.

## Quickstart (≤500 words total)
1. **Install dependencies**
   ```bash
   make install
   ```
2. **Copy environment defaults**
   ```bash
   cp .env.example .env
   ```
3. **Activate the virtual environment and run the pipeline**
   ```bash
   . .venv/bin/activate
   camels run
   ```

## Configuration
The CLI loads `.env` automatically via `python-dotenv`. Key variables:

- `CAMELS_DATA_DIR`: directory for raw regulator downloads (default `./data`).
- `CAMELS_OUTPUT_DIR`: directory for generated exports and audit artifacts (default
  `./artifacts`).
- `CAMELS_DB_PATH`: SQLite database that stores normalized indicators and scores (default
  `./camels.sqlite`).
- `CAMELS_DASHBOARD_HOST` / `CAMELS_DASHBOARD_PORT`: interface where the Streamlit
  dashboard should listen during Phase 4.
- `LOG_LEVEL`: root logging verbosity (default `INFO`).

The `Settings.ensure_directories()` helper automatically creates the data, output, and
SQLite parent directories on every CLI invocation, so fresh checkouts work without extra
setup.

## Single Command Workflow
The Typer-based CLI (`scripts/camels.py`) exposes the `camels` entry point defined in
`pyproject.toml`. A single command runs the full Phase 0 pipeline skeleton:

```bash
camels run              # executes every registered stage
camels run ingest score # executes only the requested subset
camels stages           # prints the registry of stages and descriptions
```

Each module under `camels/` registers a stage via the orchestration infrastructure in
`camels.core`. The `StageRunner` resolves requested stages, constructs a `StageContext`
(with run ID, timestamp, workspace, and environment-driven settings), and logs lifecycle
events. The ingestion stage now consumes `config/sources.yaml`, downloads regulator
documents into `data/raw/<fecha>/`, parses CSV/XLSX/PDF payloads, and records provenance
metadata in SQLite under `ingestion_log` for traceability.

The normalization stage provisions the SQLite schema (`banks`, `indicators`, `indicator_history`, `normalization_log`), syncs the >50-bank seed list in `data/reference/banks.csv`, loads CAMELS indicator definitions, and reprocesses the most recent successful ingestions to generate quarter-aligned indicator history. It flags duplicates, outliers, and coverage gaps (<8 quarters) while persisting normalized values with provenance metadata.

## Tooling
- `requirements.txt` / `pyproject.toml`: runtime dependencies (Typer, Rich, PyYAML,
  python-dotenv) and the `camels` console script binding.
- `.env.example`: template for environment configuration.
- `logging.yaml` and `logging.ini`: ready-to-use logging profiles. You may override via
  `LOGGING_CONFIG`.
- `Makefile`: helper targets for installation, execution, Docker workflows, and cleanup.
- `Dockerfile`: builds a lightweight image that runs `python -m scripts.camels run` by
  default.

The repository currently fulfills Phases 0, 1, and 2 of the roadmap: a portable
project scaffold with stage packages, centralized configuration, logging, automated
ingestion, and a normalization layer that curates bank registries and standardized
indicator history ready for downstream scoring, dashboard, export, and auditing phases.
