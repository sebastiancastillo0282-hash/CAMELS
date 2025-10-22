"""PDF parser for ingestion."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from pypdf import PdfReader

from .base import ParsedDataset


def parse_pdf(path: Path) -> ParsedDataset:
    reader = PdfReader(str(path))
    records: List[Dict[str, object]] = []
    for index, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        records.append({"page": index, "text": text})
    metadata = {
        "pages": len(records),
        "source": path.name,
    }
    return ParsedDataset(records=records, metadata=metadata)
