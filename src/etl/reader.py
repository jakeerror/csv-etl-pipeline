import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"order_id", "product", "category", "quantity", "price", "sale_date", "region"}


def read_csv(filepath: str) -> list[dict]:
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    if path.suffix.lower() != ".csv":
        raise ValueError(f"Expected .csv file, got: {path.suffix}")

    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = set(reader.fieldnames or [])

        missing = REQUIRED_COLUMNS - columns
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        for i, row in enumerate(reader, start=2):
            rows.append(dict(row))

    logger.info(f"Read {len(rows)} rows, columns: {list(columns)}")
    return rows
