"""Common parsing primitives for ingestion."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass(slots=True)
class ParsedDataset:
    """Container for parsed rows and associated metadata."""

    records: List[Dict[str, Any]]
    metadata: Dict[str, Any]

    @property
    def row_count(self) -> int:
        return len(self.records)
