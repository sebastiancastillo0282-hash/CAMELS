from __future__ import annotations

import pytest

from camels.scoring.config import (
    IndicatorRule,
    PillarRule,
    ScoringConfig,
    ScoringDefaults,
    ThresholdBand,
)
from camels.scoring.engine import ScoringEngine
from camels.scoring.models import BankProfile, IndicatorSnapshot


def test_scoring_engine_computes_composite_rating() -> None:
    config = ScoringConfig(
        version=1,
        defaults=ScoringDefaults(
            scores={"green": 100.0, "yellow": 60.0, "red": 20.0, "missing": 0.0},
            rating_thresholds={"green": 80.0, "yellow": 50.0},
        ),
        composite_weights={"capital": 1.0},
        pillars={
            "capital": PillarRule(
                name="capital",
                weight=1.0,
                indicators={
                    "cet1_rwa": IndicatorRule(
                        indicator_id="cet1_rwa",
                        weight=1.0,
                        thresholds={
                            "green": ThresholdBand(name="green", min=0.12),
                            "yellow": ThresholdBand(name="yellow", min=0.08),
                            "red": ThresholdBand(name="red", min=0.0),
                        },
                    )
                },
            )
        },
    )
    engine = ScoringEngine(config)
    bank = BankProfile(
        bank_id="bank1",
        name="Banco Test",
        country="Guatemala",
        regulator="SIB",
    )
    snapshot = IndicatorSnapshot(
        bank_id="bank1",
        indicator_id="cet1_rwa",
        pillar="capital",
        period="2024Q1",
        value=0.14,
        unit="ratio",
        source_id="demo-source",
        normalization_run_id="norm-run",
        metadata={"source": "demo"},
    )

    output = engine.score_all([bank], {"bank1": {"cet1_rwa": snapshot}})
    assert output.banks_with_values == 1
    assert output.indicators_with_values == 1
    assert output.latest_period == "2024Q1"

    composite = output.scores[0]
    assert composite.rating == "green"
    assert composite.metadata["available_weight"] == pytest.approx(1.0)

    pillar = composite.pillars[0]
    assert pillar.rating == "green"
    assert pillar.metadata["missing_indicators"] == []

    indicator = pillar.indicators[0]
    assert indicator.rating == "green"
    assert indicator.metadata["thresholds"]["green"]["min"] == 0.12
    assert indicator.metadata["source_metadata"]["source"] == "demo"
