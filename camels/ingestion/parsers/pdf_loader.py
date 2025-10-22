"""PDF parser for ingestion."""
from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path
from typing import Dict, List

_PYPDF_SPEC = importlib.util.find_spec("pypdf")
if _PYPDF_SPEC is not None:  # pragma: no cover - depends on environment availability
    _pypdf = importlib.import_module("pypdf")
    PdfReader = _pypdf.PdfReader
else:  # pragma: no cover - exercised when dependency is unavailable
    PdfReader = None

from .base import ParsedDataset


def parse_pdf(path: Path) -> ParsedDataset:
    if PdfReader is None:
        metadata = {"source": path.name, "warning": "pypdf not available"}
        return ParsedDataset(records=[], metadata=metadata)

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
