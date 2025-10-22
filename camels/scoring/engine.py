from __future__ import annotations

"""Core scoring logic for CAMELS indicators."""

import logging
from typing import Dict, Iterable, Tuple

from .config import IndicatorRule, PillarRule, ScoringConfig
from .models import (
    BankProfile,
    CompositeScore,
    IndicatorScore,
    IndicatorSnapshot,
    PillarScore,
    ScoringOutput,
)

logger = logging.getLogger(__name__)


class ScoringEngine:
    """Apply configured thresholds to normalized indicators."""

    def __init__(self, config: ScoringConfig) -> None:
        self.config = config
        self._score_map = config.defaults.scores
        self._rating_thresholds = config.defaults.rating_thresholds

    def score_all(
        self,
        banks: Iterable[BankProfile],
        snapshots: Dict[str, Dict[str, IndicatorSnapshot]],
    ) -> ScoringOutput:
        results: list[CompositeScore] = []
        banks_with_values = 0
        indicators_with_values = 0
        latest_period: str | None = None

        for bank in banks:
            bank_snapshots = snapshots.get(bank.bank_id, {})
            composite, value_count, indicator_values = self._score_bank(
                bank, bank_snapshots
            )
            if value_count > 0:
                banks_with_values += 1
            indicators_with_values += indicator_values
            if composite.period and (latest_period is None or composite.period > latest_period):
                latest_period = composite.period
            results.append(composite)

        return ScoringOutput(
            scores=results,
            banks_with_values=banks_with_values,
            indicators_with_values=indicators_with_values,
            latest_period=latest_period,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _score_bank(
        self,
        bank: BankProfile,
        indicator_data: Dict[str, IndicatorSnapshot],
    ) -> Tuple[CompositeScore, int, int]:
        pillar_scores: list[PillarScore] = []
        pillar_value_count = 0
        indicator_values = 0
        period_candidates: list[str] = []
        composite_weight = 0.0
        composite_total = 0.0
        missing_pillars: list[str] = []

        for pillar_name, pillar_rule in self.config.pillars.items():
            pillar_score, value_count, indicator_count = self._score_pillar(
                bank.bank_id, pillar_name, pillar_rule, indicator_data
            )
            pillar_scores.append(pillar_score)
            pillar_value_count += value_count
            indicator_values += indicator_count
            if pillar_score.period:
                period_candidates.append(pillar_score.period)
            pillar_weight = self.config.composite_weights.get(
                pillar_name, pillar_rule.weight
            )
            if pillar_score.rating != "missing":
                composite_weight += pillar_weight
                composite_total += pillar_score.score * pillar_weight
            else:
                missing_pillars.append(pillar_name)

        period = max(period_candidates) if period_candidates else None
        if composite_weight > 0:
            composite_score = composite_total / composite_weight
            composite_rating = self._rating_for_score(composite_score)
        else:
            composite_score = 0.0
            composite_rating = "missing"

        composite_metadata = {
            "expected_weight": sum(
                self.config.composite_weights.get(name, rule.weight)
                for name, rule in self.config.pillars.items()
            ),
            "available_weight": composite_weight,
            "missing_pillars": missing_pillars,
        }

        composite = CompositeScore(
            bank_id=bank.bank_id,
            score=composite_score,
            rating=composite_rating,
            period=period,
            pillars=pillar_scores,
            metadata=composite_metadata,
        )
        return composite, pillar_value_count, indicator_values

    def _score_pillar(
        self,
        bank_id: str,
        pillar_name: str,
        pillar_rule: PillarRule,
        indicator_data: Dict[str, IndicatorSnapshot],
    ) -> Tuple[PillarScore, int, int]:
        indicators: list[IndicatorScore] = []
        period_candidates: list[str] = []
        available_weight = 0.0
        expected_weight = 0.0
        weighted_total = 0.0
        values_present = 0
        indicators_with_values = 0
        missing_indicators: list[str] = []

        for indicator_id, indicator_rule in pillar_rule.indicators.items():
            expected_weight += indicator_rule.weight
            snapshot = indicator_data.get(indicator_id)
            indicator_score = self._evaluate_indicator(
                bank_id, pillar_name, indicator_rule, snapshot
            )
            indicators.append(indicator_score)
            if indicator_score.period:
                period_candidates.append(indicator_score.period)
            if indicator_score.rating != "missing":
                available_weight += indicator_rule.weight
                weighted_total += indicator_score.score * indicator_rule.weight
                values_present += 1
                indicators_with_values += 1
            else:
                missing_indicators.append(indicator_id)

        period = max(period_candidates) if period_candidates else None
        if available_weight > 0:
            pillar_score = weighted_total / available_weight
            pillar_rating = self._rating_for_score(pillar_score)
        else:
            pillar_score = 0.0
            pillar_rating = "missing"

        pillar_metadata = {
            "expected_weight": expected_weight,
            "available_weight": available_weight,
            "missing_indicators": missing_indicators,
        }

        result = PillarScore(
            bank_id=bank_id,
            pillar=pillar_name,
            score=pillar_score,
            rating=pillar_rating,
            weight=pillar_rule.weight,
            period=period,
            indicators=indicators,
            metadata=pillar_metadata,
        )
        return result, values_present, indicators_with_values

    def _evaluate_indicator(
        self,
        bank_id: str,
        pillar_name: str,
        rule: IndicatorRule,
        snapshot: IndicatorSnapshot | None,
    ) -> IndicatorScore:
        metadata = {
            "thresholds": {
                name: {k: v for k, v in {"min": band.min, "max": band.max}.items() if v is not None}
                for name, band in rule.thresholds.items()
            }
        }
        period = snapshot.period if snapshot else None
        value = snapshot.value if snapshot else None
        source_id = snapshot.source_id if snapshot else None
        normalization_run_id = snapshot.normalization_run_id if snapshot else None
        unit = snapshot.unit if snapshot else None

        if snapshot and snapshot.metadata:
            metadata["source_metadata"] = snapshot.metadata

        if value is None:
            metadata["reason"] = "missing_value"
            return IndicatorScore(
                bank_id=bank_id,
                indicator_id=rule.indicator_id,
                pillar=pillar_name,
                period=period,
                value=None,
                score=self._score_map.get("missing", 0.0),
                rating="missing",
                weight=rule.weight,
                source_id=source_id,
                normalization_run_id=normalization_run_id,
                unit=unit,
                metadata=metadata,
            )

        rating = self._determine_rating(value, rule)
        if rating == "red":
            metadata["reason"] = "outside_thresholds"

        indicator_score = IndicatorScore(
            bank_id=bank_id,
            indicator_id=rule.indicator_id,
            pillar=pillar_name,
            period=period,
            value=value,
            score=self._score_map.get(rating, 0.0),
            rating=rating,
            weight=rule.weight,
            source_id=source_id,
            normalization_run_id=normalization_run_id,
            unit=unit,
            metadata=metadata,
        )
        return indicator_score

    def _determine_rating(self, value: float, rule: IndicatorRule) -> str:
        for candidate in ("green", "yellow"):
            band = rule.thresholds.get(candidate)
            if band and band.matches(value):
                return candidate
        return "red"

    def _rating_for_score(self, score: float) -> str:
        green_threshold = self._rating_thresholds.get("green", 80.0)
        yellow_threshold = self._rating_thresholds.get("yellow", 50.0)
        if score >= green_threshold:
            return "green"
        if score >= yellow_threshold:
            return "yellow"
        return "red"


__all__ = ["ScoringEngine"]
