from __future__ import annotations

"""Helpers to load and validate the scoring configuration file."""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping

import yaml


@dataclass(slots=True)
class ThresholdBand:
    """Represents a threshold band (green, yellow, etc.)."""

    name: str
    min: float | None = None
    max: float | None = None

    def matches(self, value: float) -> bool:
        if self.min is not None and value < self.min:
            return False
        if self.max is not None and value > self.max:
            return False
        return True


@dataclass(slots=True)
class IndicatorRule:
    """Configuration for a single indicator."""

    indicator_id: str
    weight: float
    thresholds: Dict[str, ThresholdBand]


@dataclass(slots=True)
class PillarRule:
    """Configuration for an entire CAMELS pillar."""

    name: str
    weight: float
    indicators: Dict[str, IndicatorRule]


@dataclass(slots=True)
class ScoringDefaults:
    """Default numeric values used by the scoring engine."""

    scores: Dict[str, float]
    rating_thresholds: Dict[str, float]


@dataclass(slots=True)
class ScoringConfig:
    """Parsed scoring configuration."""

    version: int
    defaults: ScoringDefaults
    composite_weights: Dict[str, float]
    pillars: Dict[str, PillarRule]

    def pillar(self, name: str) -> PillarRule:
        return self.pillars[name]


def _ensure_default_scores(raw: Mapping[str, float] | None) -> Dict[str, float]:
    scores = {"green": 100.0, "yellow": 60.0, "red": 20.0, "missing": 0.0}
    if raw:
        for key, value in raw.items():
            try:
                scores[key] = float(value)
            except (TypeError, ValueError):
                continue
    return scores


def _ensure_rating_thresholds(raw: Mapping[str, float] | None) -> Dict[str, float]:
    thresholds = {"green": 80.0, "yellow": 50.0}
    if raw:
        for key, value in raw.items():
            try:
                thresholds[key] = float(value)
            except (TypeError, ValueError):
                continue
    return thresholds


def _load_indicator_rule(indicator_id: str, payload: Mapping[str, object]) -> IndicatorRule:
    weight = float(payload.get("weight", 0.0))
    thresholds: Dict[str, ThresholdBand] = {}
    for band_name, band_payload in (payload.get("thresholds") or {}).items():
        if not isinstance(band_payload, Mapping):
            continue
        thresholds[band_name] = ThresholdBand(
            name=band_name,
            min=float(band_payload["min"]) if "min" in band_payload else None,
            max=float(band_payload["max"]) if "max" in band_payload else None,
        )
    return IndicatorRule(indicator_id=indicator_id, weight=weight, thresholds=thresholds)


def _load_pillar_rule(name: str, payload: Mapping[str, object]) -> PillarRule:
    weight = float(payload.get("weight", 0.0))
    indicators: Dict[str, IndicatorRule] = {}
    for indicator_id, indicator_payload in (payload.get("indicators") or {}).items():
        if not isinstance(indicator_payload, Mapping):
            continue
        indicators[indicator_id] = _load_indicator_rule(indicator_id, indicator_payload)
    return PillarRule(name=name, weight=weight, indicators=indicators)


def load_scoring_config(path: Path) -> ScoringConfig:
    """Load the scoring configuration from *path*."""

    if not path.exists():
        raise FileNotFoundError(f"Scoring configuration not found at {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    version = int(payload.get("version", 1))
    defaults_raw = payload.get("defaults", {})
    defaults = ScoringDefaults(
        scores=_ensure_default_scores(defaults_raw.get("scores")),
        rating_thresholds=_ensure_rating_thresholds(
            defaults_raw.get("rating_thresholds")
        ),
    )
    composite_weights_raw = payload.get("composite", {}).get("weights", {})
    composite_weights = {
        key: float(value) for key, value in composite_weights_raw.items()
    }
    pillars: Dict[str, PillarRule] = {}
    for name, pillar_payload in (payload.get("pillars") or {}).items():
        if not isinstance(pillar_payload, Mapping):
            continue
        pillars[name] = _load_pillar_rule(name, pillar_payload)
    if not pillars:
        raise ValueError("No pillars defined in scoring configuration")
    return ScoringConfig(
        version=version,
        defaults=defaults,
        composite_weights=composite_weights,
        pillars=pillars,
    )


__all__ = [
    "IndicatorRule",
    "PillarRule",
    "ScoringConfig",
    "ScoringDefaults",
    "ThresholdBand",
    "load_scoring_config",
]
