"""Source catalog loader for ingestion."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, Sequence

import yaml


class CatalogError(RuntimeError):
    """Raised when the source catalog cannot be loaded."""


@dataclass(slots=True)
class SourceDefinition:
    """Normalized definition for a regulator data source."""

    id: str
    name: str
    country: str
    regulator: str
    bank: str
    url: str
    format: str
    frequency: str
    indicators: Sequence[str]
    description: str | None = None
    encoding: str | None = None
    worksheet: str | None = None

    @property
    def slug(self) -> str:
        return self.id.replace(" ", "_")


def _validate(entry: Mapping[str, object]) -> SourceDefinition:
    required = {"id", "name", "country", "regulator", "bank", "url", "format", "frequency"}
    missing = required - set(entry)
    if missing:
        raise CatalogError(f"Missing required keys {sorted(missing)} for source definition")
    indicators = entry.get("indicators") or []
    if not isinstance(indicators, Iterable):
        raise CatalogError("'indicators' must be an iterable")
    indicators_list = [str(item) for item in indicators]
    return SourceDefinition(
        id=str(entry["id"]),
        name=str(entry["name"]),
        country=str(entry["country"]),
        regulator=str(entry["regulator"]),
        bank=str(entry["bank"]),
        url=str(entry["url"]),
        format=str(entry["format"]).lower(),
        frequency=str(entry["frequency"]),
        indicators=tuple(indicators_list),
        description=str(entry.get("description")) if entry.get("description") else None,
        encoding=str(entry.get("encoding")) if entry.get("encoding") else None,
        worksheet=str(entry.get("worksheet")) if entry.get("worksheet") else None,
    )


def load_catalog(path: Path | None = None) -> List[SourceDefinition]:
    """Load the YAML catalog located at *path* or the default location."""

    catalog_path = path or Path("config/sources.yaml")
    if not catalog_path.exists():
        raise CatalogError(f"Source catalog not found at {catalog_path}")
    try:
        with catalog_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - parser errors are rare
        raise CatalogError(f"Failed to parse catalog: {exc}") from exc
    entries = payload.get("sources") if isinstance(payload, Mapping) else None
    if not entries:
        raise CatalogError("Catalog does not define any sources under 'sources'")
    return [_validate(entry) for entry in entries]
