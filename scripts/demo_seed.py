"""Generate synthetic CAMELS data for local QA and demos."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Iterable, Tuple

from camels.ingestion.storage import IngestionLogEntry, IngestionStore
from camels.normalization.banks import BankRecord, BankRepository, load_seed_banks
from camels.normalization.indicators import (
    IndicatorCatalog,
    indicator_catalog,
    sync_indicator_catalog,
)
from camels.normalization.schema import NormalizationSchema
from camels.normalization.storage import NormalizedStore
from camels.normalization.transformers import NormalizedRecord
from camels.scoring.config import (
    IndicatorRule,
    PillarRule,
    ScoringConfig,
    load_scoring_config,
)
from camels.scoring.pipeline import run_pipeline as run_scoring_pipeline
from camels.settings import Settings


@dataclass(slots=True)
class DemoContext:
    settings: Settings
    timestamp: datetime
    ingestion_run_id: str
    normalization_run_id: str
    scoring_run_id: str


def _quarter_bounds(year: int, quarter: int) -> Tuple[str, str]:
    month = (quarter - 1) * 3 + 1
    start = datetime(year, month, 1)
    if quarter == 4:
        end = datetime(year, 12, 31)
    else:
        next_month = month + 3
        next_year = year
        if next_month > 12:
            next_month -= 12
            next_year += 1
        end = datetime(next_year, next_month, 1) - timedelta(days=1)
    return start.date().isoformat(), end.date().isoformat()


def _chronological_periods(periods: int) -> list[Tuple[int, int]]:
    now = datetime.utcnow()
    current_quarter = (now.month - 1) // 3 + 1
    year = now.year
    quarter = current_quarter
    results: list[Tuple[int, int]] = []
    for _ in range(periods):
        results.append((year, quarter))
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
    results.reverse()
    return results


def _sample_value(rule: IndicatorRule) -> float:
    green = rule.thresholds.get("green")
    if green is None:
        return 0.0
    if green.min is not None and green.max is not None:
        return (green.min + green.max) / 2.0
    if green.min is not None:
        bump = abs(green.min) * 0.1 or 0.02
        return green.min + bump
    if green.max is not None:
        return green.max * 0.8
    return 0.0


def _adjust_for_bank(value: float, *, bank_index: int, period_index: int) -> float:
    return value + (bank_index * 0.003) - (period_index * 0.001)


def _record_for_indicator(
    bank: BankRecord,
    rule: IndicatorRule,
    pillar: PillarRule,
    base_value: float,
    period: Tuple[int, int],
    context: DemoContext,
    source_id: str,
    unit: str,
) -> NormalizedRecord:
    year, quarter = period
    period_label = f"{year}Q{quarter}"
    period_start, period_end = _quarter_bounds(year, quarter)
    value = base_value
    return NormalizedRecord(
        bank_id=bank.bank_id,
        indicator_id=rule.indicator_id,
        period=period_label,
        period_start=period_start,
        period_end=period_end,
        value=round(value, 6),
        unit=unit,
        raw_value=f"{value:.6f}",
        source_id=source_id,
        run_id=context.normalization_run_id,
        metadata={
            "source_run": context.ingestion_run_id,
            "checksum": "demo",
            "pillar": pillar.name,
        },
    )


def _ensure_ingestion_records(
    context: DemoContext,
    banks: Iterable[BankRecord],
    source_map: Dict[str, str],
) -> None:
    store = IngestionStore(context.settings.sqlite_path)
    started = context.timestamp
    completed = started + timedelta(minutes=5)
    for bank in banks:
        entry = IngestionLogEntry(
            run_id=context.ingestion_run_id,
            source_id=source_map[bank.bank_id],
            bank=bank.name,
            country=bank.country,
            regulator=bank.regulator,
            url="https://example.com/demo.csv",
            format="csv",
            frequency="quarterly",
            local_path=str(context.settings.data_dir / "demo" / f"{bank.bank_id}.csv"),
            checksum="demo",
            record_count=32,
            status="success",
            error=None,
            started_at=started,
            completed_at=completed,
            metadata={"generated": True},
        )
        store.record(entry)


def _persist_normalized_records(
    context: DemoContext,
    banks: list[BankRecord],
    config: ScoringConfig,
    periods: list[Tuple[int, int]],
) -> None:
    catalog = IndicatorCatalog(indicator_catalog())
    sync_indicator_catalog(context.settings.sqlite_path, catalog)
    store = NormalizedStore(context.settings.sqlite_path)
    records: list[NormalizedRecord] = []
    source_map = {bank.bank_id: f"demo_source_{bank.bank_id}" for bank in banks}

    _ensure_ingestion_records(context, banks, source_map)

    for bank_index, bank in enumerate(banks):
        for pillar_name, pillar_rule in config.pillars.items():
            for indicator_id, rule in pillar_rule.indicators.items():
                definition = catalog.by_id(indicator_id)
                if definition is None:
                    continue
                base_value = _sample_value(rule)
                for period_index, period in enumerate(periods):
                    adjusted = _adjust_for_bank(
                        base_value,
                        bank_index=bank_index,
                        period_index=period_index,
                    )
                    if definition.min_value is not None:
                        adjusted = max(adjusted, definition.min_value)
                    if definition.max_value is not None:
                        adjusted = min(adjusted, definition.max_value)
                    record = _record_for_indicator(
                        bank,
                        rule,
                        pillar_rule,
                        adjusted,
                        period,
                        context,
                        source_map[bank.bank_id],
                        definition.unit,
                    )
                    records.append(record)
                    store.log_event(
                        run_id=context.normalization_run_id,
                        source_id=record.source_id,
                        bank_id=record.bank_id,
                        indicator_id=record.indicator_id,
                        period=record.period,
                        status="success",
                        message="demo",
                    )
    store.upsert(records)


def seed_demo_data(periods: int = 8) -> None:
    settings = Settings.load()
    settings.ensure_directories()
    timestamp = datetime.utcnow()
    context = DemoContext(
        settings=settings,
        timestamp=timestamp,
        ingestion_run_id=f"demo-ingest-{timestamp:%Y%m%d%H%M%S}",
        normalization_run_id=f"demo-norm-{timestamp:%Y%m%d%H%M%S}",
        scoring_run_id=f"demo-score-{timestamp:%Y%m%d%H%M%S}",
    )

    NormalizationSchema(settings.sqlite_path).ensure()
    seed_banks = load_seed_banks()
    BankRepository(settings.sqlite_path).sync(seed_banks)
    selected_banks = seed_banks[:2]

    config = load_scoring_config(settings.scoring_config)
    period_list = _chronological_periods(periods)

    _persist_normalized_records(context, selected_banks, config, period_list)

    summary = run_scoring_pipeline(
        sqlite_path=settings.sqlite_path,
        config_path=settings.scoring_config,
        run_id=context.scoring_run_id,
    )

    print("Demo data seeded:")
    print(f"  Banks evaluated: {summary.banks_evaluated}")
    print(f"  Banks with data: {summary.banks_with_data}")
    print(f"  Indicators evaluated: {summary.indicators_with_values}")
    if summary.latest_period:
        print(f"  Latest period: {summary.latest_period}")


if __name__ == "__main__":
    seed_demo_data()
