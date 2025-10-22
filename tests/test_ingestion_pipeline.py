from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

from camels.ingestion.catalog import SourceDefinition
from camels.ingestion.download import DownloadResult
from camels.ingestion.parsers import ParsedDataset
from camels.ingestion.pipeline import run_pipeline


def test_ingestion_pipeline_records_success(monkeypatch, stage_context) -> None:
    source = SourceDefinition(
        id="demo-source",
        name="Demo Source",
        country="Guatemala",
        regulator="SIB",
        bank="Banco G&T Continental, S.A.",
        url="file:///tmp/demo.csv",
        format="csv",
        frequency="quarterly",
        indicators=("CET1/RWA",),
    )

    def fake_load_catalog(path: Path | None = None):  # pragma: no cover - simple stub
        return [source]

    def fake_download_source(src, directory: Path, **_: object) -> DownloadResult:
        file_path = directory / "demo.csv"
        file_path.write_text("period,value\n2024Q1,12\n", encoding="utf-8")
        checksum = hashlib.sha256(file_path.read_bytes()).hexdigest()
        return DownloadResult(
            source=src,
            path=file_path,
            sha256=checksum,
            size_bytes=file_path.stat().st_size,
            content_type="text/csv",
            elapsed=0.01,
        )

    parsed_dataset = ParsedDataset(
        records=[{"period": "2024Q1", "CET1/RWA": "12"}],
        metadata={"columns": ["period", "CET1/RWA"]},
    )

    def fake_parse_file(path: Path, src: SourceDefinition) -> ParsedDataset:
        assert path.exists()
        assert src is source
        return parsed_dataset

    monkeypatch.setattr("camels.ingestion.pipeline.load_catalog", fake_load_catalog)
    monkeypatch.setattr("camels.ingestion.pipeline.download_source", fake_download_source)
    monkeypatch.setattr("camels.ingestion.pipeline.parse_file", fake_parse_file)

    entries = run_pipeline(stage_context)

    assert len(entries) == 1
    entry = entries[0]
    assert entry.status == "success"
    assert entry.metadata["parse_summary"]["rows"] == 1

    with sqlite3.connect(stage_context.settings.sqlite_path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM ingestion_log").fetchone()[0]
    assert count == 1
