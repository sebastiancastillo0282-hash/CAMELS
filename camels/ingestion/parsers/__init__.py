"""Parser dispatch for ingestion."""
from __future__ import annotations

from pathlib import Path

from camels.ingestion.catalog import SourceDefinition

from .base import ParsedDataset


def parse_file(path: Path, source: SourceDefinition) -> ParsedDataset:
    """Parse *path* according to the format declared in *source*."""

    format_name = source.format.lower()
    if format_name == "csv":
        from .csv_loader import parse_csv

        return parse_csv(path, encoding=source.encoding)
    if format_name in {"xlsx", "xls"}:
        from .xlsx_loader import parse_xlsx

        return parse_xlsx(path, worksheet=source.worksheet)
    if format_name == "pdf":
        from .pdf_loader import parse_pdf

        return parse_pdf(path)
    raise ValueError(f"Unsupported format '{source.format}' for source {source.id}")


__all__ = ["ParsedDataset", "parse_file"]
