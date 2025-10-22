"""CSV parser for ingestion."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List

from .base import ParsedDataset


def parse_csv(path: Path, *, encoding: str | None = None) -> ParsedDataset:
    encoding = encoding or "utf-8"
    with path.open("r", encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        records: List[Dict[str, str]] = [dict(row) for row in reader]
    metadata = {
        "columns": reader.fieldnames or [],
        "encoding": encoding,
    }
    return ParsedDataset(records=records, metadata=metadata)
