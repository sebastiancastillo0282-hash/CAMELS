from __future__ import annotations

import pytest

from camels.ingestion.catalog import SourceDefinition
from camels.ingestion.parsers import ParsedDataset
from camels.normalization.indicators import IndicatorCatalog, IndicatorDefinition
from camels.normalization.transformers import NormalizationTransformer, slugify


def test_normalization_transformer_extracts_quarter() -> None:
    indicator = IndicatorDefinition(
        indicator_id="cet1_rwa",
        name="CET1/RWA",
        pillar="capital",
        unit="ratio",
        description=None,
        min_value=0.0,
        max_value=1.0,
    )
    catalog = IndicatorCatalog([indicator])
    bank_lookup = {slugify("Banco G&T Continental, S.A."): "gt-conti"}

    transformer = NormalizationTransformer(catalog, bank_lookup)
    source = SourceDefinition(
        id="demo-source",
        name="Demo",
        country="Guatemala",
        regulator="SIB",
        bank="Banco G&T Continental, S.A.",
        url="https://example.com/demo.csv",
        format="csv",
        frequency="quarterly",
        indicators=("CET1/RWA",),
    )

    dataset = ParsedDataset(
        records=[{"Year": "2024", "Quarter": "Q1", "CET1/RWA": "12%"}],
        metadata={"columns": ["Year", "Quarter", "CET1/RWA"]},
    )

    records = transformer.transform(
        dataset,
        source,
        {"run_id": "ing-run", "checksum": "abc"},
        run_id="norm-run",
    )

    assert len(records) == 1
    record = records[0]
    assert record.bank_id == "gt-conti"
    assert record.indicator_id == "cet1_rwa"
    assert record.period == "2024Q1"
    assert record.period_start == "2024-01-01"
    assert record.period_end == "2024-03-31"
    assert record.value == pytest.approx(0.12)
    assert record.metadata["column"] == "CET1/RWA"
    assert record.metadata["checksum"] == "abc"
