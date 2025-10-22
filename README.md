# CAMELS

CAMELS (Coordinated Analytics for Metrics, Evaluation, and Lifecycle Scoring) is a local
command-line toolkit that orchestrates the CAMELS pipeline end-to-end: regulator data
ingestion, indicator normalization, scoring, dashboard bootstrapping, consolidated exports,
and audit logging.

## Quickstart (≤500 words total)
1. **Install dependencies**
   ```bash
   make install
   ```
2. **Configure the environment** (optional overrides)
   ```bash
   cp .env.example .env
   ```
3. **Run the pipeline**
   ```bash
   camels run              # executes every registered stage
   camels run ingest score # run selected stages only
   camels stages           # inspect the registry
   ```

## Configuration
`python-dotenv` auto-loads variables from `.env`:
- `CAMELS_DATA_DIR`: directory for raw regulator downloads (default `./data`).
- `CAMELS_OUTPUT_DIR`: directory for generated exports and audit artifacts (default
  `./artifacts`).
- `CAMELS_DB_PATH`: SQLite database for normalized indicators, scores, and audit tables
  (default `./camels.sqlite`).
- `CAMELS_SCORING_CONFIG`: YAML thresholds/weights for Phase 3 scoring (default
  `./config/camels_thresholds.yaml`).
- `CAMELS_DASHBOARD_HOST` / `CAMELS_DASHBOARD_PORT`: interface where the Streamlit
  dashboard will listen during Phase 4.
- `LOG_LEVEL`: root logging verbosity (default `INFO`).

`Settings.ensure_directories()` creates the data, output, and database parent directories
so fresh checkouts work without extra setup.

## Quality assurance & demo data
- `make lint` runs Ruff, `make format` applies Black, `make test` executes the pytest suite
  that covers ingestion (catalog orchestration), normalization transformers, scoring engine,
  and dashboard stage wiring. `make qa` runs lint + tests.
- `make demo` (or `python -m scripts.demo_seed`) seeds the SQLite database with eight
  quarters of synthetic history for two seed banks, records matching ingestion log entries,
  and executes the scoring pipeline so the dashboard/export layers have realistic data.
- The QA suite lives under `tests/` and focuses on deterministic unit behaviour to keep the
  local workflow fast and self-contained.

## Implementation status
The repository now fulfils Phases 0–5 of the roadmap and adds Phase 6 readiness:
- Stage packages (`camels.ingestion`, `camels.normalization`, `camels.scoring`,
  `camels.export`, `camels.audit`, `camels.dashboard`) register with the orchestration
  runtime (`camels.core`).
- Ingestion consumes `config/sources.yaml`, downloads CSV/XLSX/PDF payloads into
  `data/raw/<fecha>/`, parses content via format-specific loaders, and records provenance in
  `ingestion_log`.
- Normalization provisions the SQLite schema (`banks`, `indicators`, `indicator_history`,
  `normalization_log`), syncs the >50-bank seed list, loads indicator definitions, and
  regenerates quarter-aligned history from the latest successful ingestions.
- Scoring applies the configurable thresholds in `config/camels_thresholds.yaml`, persists
  composite/pillar/indicator results, and feeds exports plus audit storage.
- Audit and export stages generate JSON/CSV artifacts, consolidated Excel/CSV packs, and
  link every metric back to its source metadata.
- Phase 6 layers automated linting/tests and ships the demo seeding script so teams can run
  offline QA with predictable data snapshots ahead of the dashboard implementation.
