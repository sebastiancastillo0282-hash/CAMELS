# CAMELS

CAMELS (Coordinated Analytics for Metrics, Evaluation, and Lifecycle Scoring) provides a
command-line toolkit for orchestrating data ingestion, normalization, scoring, dashboard
publication, exporting, and auditing tasks.

## Installation (≤5 minutes)
1. Ensure Python 3.9+ is installed.
2. Clone this repository and create a virtual environment:
   ```bash
   make install
   ```
   The command creates `.venv/` and installs the runtime dependencies listed in
   `requirements.txt`.

## Configuration
Copy `.env.example` to `.env` and adjust the values for your environment:
```bash
cp .env.example .env
```
Key variables include `CAMELS_DATA_DIR`, `CAMELS_OUTPUT_DIR`, and dashboard host/port
settings. The CLI automatically loads `.env` through `python-dotenv` when commands run.
Logging defaults to the `logging.yaml`/`logging.ini` configurations in the repository; you
may point to either file through the standard `LOGGING_CONFIG` environment variable if
needed.

## Usage
Activate the environment and run the pipeline:
```bash
. .venv/bin/activate
camels run
```
`camels run` executes every stage sequentially. Provide stage names to limit execution,
e.g. `camels run ingest score`. Each stage also has a direct shortcut (`camels ingest`,
`camels normalize`, `camels score`, `camels dashboard`, `camels export`, and
`camels audit`). The placeholders currently log their invocation so you can wire your own
business logic inside the corresponding modules under `camels/`.

## Docker
Build and execute the workflow inside a container:
```bash
make docker-build
make docker-run
```
The Docker image runs `python -m scripts.camels run` by default and loads variables from
`.env.example`. Mount custom data directories or override environment variables as needed
when invoking `docker run`.

## Project Structure
- `camels/`: Python package with placeholder modules for each pipeline step.
- `scripts/camels.py`: Typer-based CLI exposing the `camels` command.
- `logging.ini` / `logging.yaml`: Ready-to-use logging configurations.
- `Makefile`: Convenience targets for installation, execution, and container workflows.

You are now ready to customize each module, integrate your data sources, and automate
CAMELS end to end—all from a single, consistent interface.
