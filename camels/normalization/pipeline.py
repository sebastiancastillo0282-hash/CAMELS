from __future__ import annotations

"""Normalization pipeline implementation."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable

from camels.ingestion.catalog import load_catalog
from camels.ingestion.parsers import parse_file

from .banks import BankRecord, BankRepository, load_seed_banks
from .indicators import (
    IndicatorCatalog,
    indicator_catalog,
    sync_indicator_catalog,
)
from .inputs import IngestionRepository
from .schema import NormalizationSchema
from .storage import NormalizedStore
from .transformers import NormalizationTransformer, NormalizedRecord, slugify

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PipelineSummary:
    processed_sources: int
    normalized_records: int
    updated_records: int
    skipped_sources: int



def _resolve_local_path(local_path: Path, data_dir: Path, workspace: Path) -> Path:
    candidates = [local_path]
    if not local_path.is_absolute():
        candidates.append(workspace / local_path)
        try:
            relative = local_path.relative_to(data_dir)
            candidates.append((data_dir / relative).resolve())
        except ValueError:
            candidates.append((data_dir / local_path.name).resolve())
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return local_path

def _bank_lookup(banks: Iterable[BankRecord]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for bank in banks:
        mapping[slugify(bank.name)] = bank.bank_id
        mapping[slugify(bank.bank_id)] = bank.bank_id
    return mapping


def _record_events(store: NormalizedStore, records: Iterable[NormalizedRecord]) -> None:
    for record in records:
        store.log_event(
            run_id=record.run_id,
            source_id=record.source_id,
            bank_id=record.bank_id,
            indicator_id=record.indicator_id,
            period=record.period,
            status="success",
        )


def _warn_on_coverage(store: NormalizedStore, *, minimum_periods: int = 8) -> None:
    coverage = store.coverage()
    for entry in coverage:
        periods = int(entry.get("periods", 0))
        if periods < minimum_periods:
            logger.warning(
                "Bank %s indicator %s has only %d period(s); minimum expected is %d.",
                entry.get("bank_id"),
                entry.get("indicator_id"),
                periods,
                minimum_periods,
            )


def run_pipeline(*, sqlite_path: Path, data_dir: Path, workspace: Path, run_id: str) -> PipelineSummary:
    """Execute the normalization pipeline."""

    NormalizationSchema(sqlite_path).ensure()

    seed_banks = load_seed_banks()
    if len(seed_banks) < 50:
        logger.warning(
            "Seed bank registry contains %d entries; expected > 50.", len(seed_banks)
        )
    BankRepository(sqlite_path).sync(seed_banks)

    catalog = IndicatorCatalog(indicator_catalog())
    sync_indicator_catalog(sqlite_path, catalog)

    bank_map = _bank_lookup(seed_banks)
    ingestion_repo = IngestionRepository(sqlite_path)
    ingestion_records = ingestion_repo.latest_successful()
    if not ingestion_records:
        logger.warning("No successful ingestion runs found; nothing to normalize.")
        return PipelineSummary(0, 0, 0, 0)

    catalog_definitions = {definition.id: definition for definition in load_catalog()}
    store = NormalizedStore(sqlite_path)
    transformer = NormalizationTransformer(catalog, bank_map)

    processed = 0
    skipped = 0
    inserted_total = 0
    updated_total = 0

    for source_id, ingestion in ingestion_records.items():
        definition = catalog_definitions.get(source_id)
        if not definition:
            logger.warning("Source %s missing from catalog; skipping normalization.", source_id)
            skipped += 1
            continue
        local_path = _resolve_local_path(Path(ingestion.local_path), data_dir, workspace)
        if not local_path.exists():
            logger.warning("Ingested file %s not found; skipping.", local_path)
            skipped += 1
            continue
        dataset = parse_file(local_path, definition)
        payload = transformer.transform(
            dataset,
            definition,
            {
                "run_id": ingestion.run_id,
                "checksum": ingestion.checksum,
            },
            run_id,
        )
        if not payload:
            logger.warning(
                "No indicators extracted for source %s (%s).", source_id, definition.bank
            )
            skipped += 1
            continue
        summary = store.upsert(payload)
        _record_events(store, payload)
        processed += 1
        inserted_total += summary.inserted
        updated_total += summary.updated
        logger.info(
            "Normalized %d record(s) for %s (%s); %d inserted, %d updated.",
            len(payload),
            definition.bank,
            source_id,
            summary.inserted,
            summary.updated,
        )

    _warn_on_coverage(store)

    return PipelineSummary(
        processed_sources=processed,
        normalized_records=inserted_total,
        updated_records=updated_total,
        skipped_sources=skipped,
    )


__all__ = ["PipelineSummary", "run_pipeline"]
