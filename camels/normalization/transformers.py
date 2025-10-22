from __future__ import annotations

"""Transformation utilities for normalization."""

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional

from camels.ingestion.catalog import SourceDefinition
from camels.ingestion.parsers import ParsedDataset

from .indicators import IndicatorCatalog, IndicatorDefinition

logger = logging.getLogger(__name__)

_PERIOD_KEYS = ("period", "periodo", "quarter", "trimestre", "fecha", "date")
_YEAR_KEYS = ("year", "anio", "año")
_QUARTER_KEYS = ("quarter", "q", "trim", "trimestre")
_QUARTER_PATTERN = re.compile(r"(?P<year>\d{4}).*?q(?P<quarter>[1-4])", re.IGNORECASE)
_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%Y/%m/%d")


@dataclass(slots=True)
class NormalizedRecord:
    bank_id: str
    indicator_id: str
    period: str
    period_start: str | None
    period_end: str | None
    value: float | None
    unit: str
    raw_value: str | None
    source_id: str
    run_id: str
    metadata: Dict[str, Any]


def slugify(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def _first_day_of_quarter(year: int, quarter: int) -> date:
    month = (quarter - 1) * 3 + 1
    return date(year, month, 1)


def _last_day_of_quarter(year: int, quarter: int) -> date:
    start = _first_day_of_quarter(year, quarter)
    if quarter == 4:
        return date(year, 12, 31)
    next_start = _first_day_of_quarter(year, quarter + 1)
    return next_start - date.resolution


def _parse_date(value: str) -> Optional[date]:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def _extract_year_quarter(row: Mapping[str, Any]) -> tuple[Optional[int], Optional[int]]:
    year = None
    quarter = None
    for key, value in row.items():
        if value is None:
            continue
        key_lower = key.lower()
        if key_lower in _YEAR_KEYS:
            try:
                year = int(str(value).strip())
            except ValueError:
                continue
        if key_lower in _QUARTER_KEYS:
            match = re.search(r"([1-4])", str(value))
            if match:
                quarter = int(match.group(1))
    return year, quarter


def _extract_period(row: Mapping[str, Any]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    year, quarter = _extract_year_quarter(row)
    for key, value in row.items():
        if value is None:
            continue
        key_lower = key.lower()
        if any(marker in key_lower for marker in _PERIOD_KEYS):
            text = str(value).strip()
            if not text:
                continue
            match = _QUARTER_PATTERN.search(text)
            if match:
                year = int(match.group("year"))
                quarter = int(match.group("quarter"))
                break
            parsed_date = _parse_date(text)
            if parsed_date:
                year = parsed_date.year
                quarter = (parsed_date.month - 1) // 3 + 1
                break
    if year is None or quarter is None:
        return None, None, None
    period_label = f"{year}Q{quarter}"
    start = _first_day_of_quarter(year, quarter).isoformat()
    end = _last_day_of_quarter(year, quarter).isoformat()
    return period_label, start, end


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace("%", "").replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _normalize_value(value: float, definition: IndicatorDefinition) -> float:
    if definition.unit == "ratio" and abs(value) > 2:
        return value / 100.0
    return value


def _indicator_keys(record: Mapping[str, Any]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for key in record:
        mapping[slugify(key)] = key
    return mapping


class NormalizationTransformer:
    """Convert parsed datasets into normalized indicator records."""

    def __init__(
        self,
        indicators: IndicatorCatalog,
        bank_lookup: Dict[str, str],
    ) -> None:
        self._indicators = indicators
        self._bank_lookup = bank_lookup

    def _resolve_bank_id(self, bank_name: str) -> Optional[str]:
        normalized = slugify(bank_name)
        return self._bank_lookup.get(normalized)

    def transform(
        self,
        dataset: ParsedDataset,
        source: SourceDefinition,
        ingestion: Mapping[str, Any],
        run_id: str,
    ) -> List[NormalizedRecord]:
        bank_id = self._resolve_bank_id(source.bank)
        if not bank_id:
            logger.warning("Bank '%s' not found in registry; skipping.", source.bank)
            return []
        records: List[NormalizedRecord] = []
        indicator_names = list(source.indicators)
        indicator_lookup = {slugify(name): name for name in indicator_names}
        indicator_catalog: Dict[str, IndicatorDefinition] = {
            slugify(definition.name): definition for definition in self._indicators.values()
        }
        for record in dataset.records:
            period, period_start, period_end = _extract_period(record)
            if not period:
                continue
            key_map = _indicator_keys(record)
            for indicator_key, indicator_name in indicator_lookup.items():
                if indicator_key not in key_map:
                    continue
                column_name = key_map[indicator_key]
                definition = indicator_catalog.get(indicator_key)
                if not definition:
                    continue
                raw = record[column_name]
                numeric = _to_float(raw)
                if numeric is None:
                    continue
                normalized_value = _normalize_value(numeric, definition)
                if (
                    definition.min_value is not None
                    and normalized_value < definition.min_value
                ) or (
                    definition.max_value is not None
                    and normalized_value > definition.max_value
                ):
                    logger.warning(
                        "Value %.4f for %s (%s) falls outside expected range %.2f-%.2f",
                        normalized_value,
                        indicator_name,
                        period,
                        definition.min_value if definition.min_value is not None else float("-inf"),
                        definition.max_value if definition.max_value is not None else float("inf"),
                    )
                records.append(
                    NormalizedRecord(
                        bank_id=bank_id,
                        indicator_id=definition.indicator_id,
                        period=period,
                        period_start=period_start,
                        period_end=period_end,
                        value=normalized_value,
                        unit=definition.unit,
                        raw_value=str(raw) if raw is not None else None,
                        source_id=source.id,
                        run_id=run_id,
                        metadata={
                            "column": column_name,
                            "source_run": ingestion.get("run_id"),
                            "checksum": ingestion.get("checksum"),
                        },
                    )
                )
        return records


__all__ = ["NormalizedRecord", "NormalizationTransformer", "slugify"]
