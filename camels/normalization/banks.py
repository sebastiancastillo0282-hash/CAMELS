from __future__ import annotations

"""Bank registry synchronization for normalization."""

import csv
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BankRecord:
    bank_id: str
    name: str
    country: str
    regulator: str


def load_seed_banks(path: Path | None = None) -> List[BankRecord]:
    """Load the seed bank list from *path* or the default location."""

    csv_path = path or Path("data/reference/banks.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"Seed bank registry not found at {csv_path}")
    with csv_path.open("r", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        banks = [
            BankRecord(
                bank_id=row["bank_id"].strip(),
                name=row["name"].strip(),
                country=row["country"].strip(),
                regulator=row["regulator"].strip(),
            )
            for row in reader
        ]
    return banks


class BankRepository:
    """Persist the seed bank registry into SQLite."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def sync(self, banks: Iterable[BankRecord]) -> None:
        entries = list(banks)
        if not entries:
            logger.warning("No seed banks provided; registry will remain unchanged.")
            return
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            for bank in entries:
                connection.execute(
                    """
                    INSERT INTO banks (bank_id, name, country, regulator)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(bank_id) DO UPDATE SET
                        name=excluded.name,
                        country=excluded.country,
                        regulator=excluded.regulator,
                        updated_at=CURRENT_TIMESTAMP
                    """,
                    (bank.bank_id, bank.name, bank.country, bank.regulator),
                )
        logger.info("Synchronized %d banks into the registry", len(entries))


__all__ = ["BankRecord", "BankRepository", "load_seed_banks"]
