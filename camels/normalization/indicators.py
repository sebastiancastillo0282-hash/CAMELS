from __future__ import annotations

"""Standard CAMELS indicator definitions."""

from dataclasses import dataclass
from typing import Dict, Iterable, List

from pathlib import Path

import sqlite3


@dataclass(slots=True)
class IndicatorDefinition:
    """Metadata describing a normalized CAMELS indicator."""

    indicator_id: str
    name: str
    pillar: str
    unit: str
    description: str | None = None
    min_value: float | None = None
    max_value: float | None = None

    @property
    def key(self) -> str:
        return _normalize_key(self.name)


def _normalize_key(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def indicator_catalog() -> List[IndicatorDefinition]:
    """Return the static CAMELS indicator catalog."""

    return [
        IndicatorDefinition(
            indicator_id="cet1_rwa",
            name="CET1/RWA",
            pillar="capital",
            unit="ratio",
            description="Capital de nivel 1 sobre activos ponderados por riesgo.",
            min_value=0.0,
            max_value=1.0,
        ),
        IndicatorDefinition(
            indicator_id="tcr",
            name="TCR",
            pillar="capital",
            unit="ratio",
            description="Total Capital Ratio reportado por el banco.",
            min_value=0.0,
            max_value=1.5,
        ),
        IndicatorDefinition(
            indicator_id="leverage",
            name="Leverage",
            pillar="capital",
            unit="ratio",
            description="Índice de apalancamiento regulatorio.",
            min_value=0.0,
            max_value=0.25,
        ),
        IndicatorDefinition(
            indicator_id="npl",
            name="NPL",
            pillar="assets",
            unit="ratio",
            description="Non-performing loans sobre cartera total.",
            min_value=0.0,
            max_value=0.5,
        ),
        IndicatorDefinition(
            indicator_id="npl_coverage",
            name="Cobertura NPL",
            pillar="assets",
            unit="ratio",
            description="Cobertura de provisiones para cartera vencida.",
            min_value=0.0,
            max_value=5.0,
        ),
        IndicatorDefinition(
            indicator_id="cost_of_risk",
            name="Cost of Risk",
            pillar="assets",
            unit="ratio",
            description="Costo de riesgo contra cartera promedio.",
            min_value=-0.5,
            max_value=0.5,
        ),
        IndicatorDefinition(
            indicator_id="efficiency_ratio",
            name="Efficiency ratio",
            pillar="management",
            unit="ratio",
            description="Gastos operativos sobre ingresos operativos.",
            min_value=0.0,
            max_value=2.0,
        ),
        IndicatorDefinition(
            indicator_id="regulatory_events",
            name="Eventos regulatorios",
            pillar="management",
            unit="count",
            description="Número de eventos regulatorios materializados.",
            min_value=0.0,
            max_value=50.0,
        ),
        IndicatorDefinition(
            indicator_id="roe",
            name="ROE",
            pillar="earnings",
            unit="ratio",
            description="Return on Equity anualizado.",
            min_value=-1.0,
            max_value=1.0,
        ),
        IndicatorDefinition(
            indicator_id="roa",
            name="ROA",
            pillar="earnings",
            unit="ratio",
            description="Return on Assets anualizado.",
            min_value=-0.5,
            max_value=0.5,
        ),
        IndicatorDefinition(
            indicator_id="nim",
            name="NIM",
            pillar="earnings",
            unit="ratio",
            description="Net Interest Margin promedio trimestral.",
            min_value=-0.2,
            max_value=0.5,
        ),
        IndicatorDefinition(
            indicator_id="lcr",
            name="LCR",
            pillar="liquidity",
            unit="ratio",
            description="Liquidity Coverage Ratio.",
            min_value=0.0,
            max_value=3.0,
        ),
        IndicatorDefinition(
            indicator_id="nsfr",
            name="NSFR",
            pillar="liquidity",
            unit="ratio",
            description="Net Stable Funding Ratio.",
            min_value=0.0,
            max_value=3.0,
        ),
        IndicatorDefinition(
            indicator_id="loans_deposits",
            name="Loans/Deposits",
            pillar="liquidity",
            unit="ratio",
            description="Relación de cartera de créditos sobre depósitos.",
            min_value=0.0,
            max_value=2.0,
        ),
        IndicatorDefinition(
            indicator_id="fx_open_position",
            name="FX open position",
            pillar="sensitivity",
            unit="ratio",
            description="Posición abierta en moneda extranjera sobre patrimonio.",
            min_value=-0.5,
            max_value=0.5,
        ),
        IndicatorDefinition(
            indicator_id="duration_gap",
            name="Duration gap proxy",
            pillar="sensitivity",
            unit="ratio",
            description="Diferencia de duración activo-pasivo.",
            min_value=-5.0,
            max_value=5.0,
        ),
    ]


class IndicatorCatalog:
    """Lookup helper for indicator definitions."""

    def __init__(self, indicators: Iterable[IndicatorDefinition]):
        self._definitions: Dict[str, IndicatorDefinition] = {
            definition.indicator_id: definition for definition in indicators
        }
        self._by_key: Dict[str, IndicatorDefinition] = {
            definition.key: definition for definition in indicators
        }

    def __iter__(self):  # pragma: no cover - convenience wrapper
        return iter(self._definitions.values())

    def by_id(self, indicator_id: str) -> IndicatorDefinition | None:
        return self._definitions.get(indicator_id)

    def by_name(self, name: str) -> IndicatorDefinition | None:
        return self._by_key.get(_normalize_key(name))



    def values(self) -> List[IndicatorDefinition]:
        return list(self._definitions.values())

    def items(self):  # pragma: no cover - helper for persistence
        return self._definitions.items()

def sync_indicator_catalog(path: Path, catalog: IndicatorCatalog) -> None:
    """Persist indicator definitions into SQLite."""

    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        for definition in catalog.values():
            connection.execute(
                """
                INSERT INTO indicators (indicator_id, name, pillar, unit, description, min_value, max_value)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(indicator_id) DO UPDATE SET
                    name=excluded.name,
                    pillar=excluded.pillar,
                    unit=excluded.unit,
                    description=excluded.description,
                    min_value=excluded.min_value,
                    max_value=excluded.max_value,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (
                    definition.indicator_id,
                    definition.name,
                    definition.pillar,
                    definition.unit,
                    definition.description,
                    definition.min_value,
                    definition.max_value,
                ),
            )


__all__ = [
    "IndicatorCatalog",
    "IndicatorDefinition",
    "indicator_catalog",
    "sync_indicator_catalog",
]
