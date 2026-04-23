import pytest
import csv
import os
from pathlib import Path
from src.etl.reader import read_csv


@pytest.fixture
def valid_csv(tmp_path):
    filepath = tmp_path / "test.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "order_id", "product", "category", "quantity", "price", "sale_date", "region"
        ])
        writer.writeheader()
        writer.writerow({
            "order_id": "ORD-001", "product": "Mouse", "category": "periphery",
            "quantity": "1", "price": "1500", "sale_date": "2024-01-10", "region": "spb"
        })
    return filepath


def test_read_csv_valid(valid_csv):
    rows = read_csv(str(valid_csv))
    assert len(rows) == 1
    assert rows[0]["order_id"] == "ORD-001"


def test_read_csv_file_not_found():
    with pytest.raises(FileNotFoundError):
        read_csv("nonexistent.csv")


def test_read_csv_wrong_extension(tmp_path):
    f = tmp_path / "data.txt"
    f.write_text("hello")
    with pytest.raises(ValueError, match="Expected .csv"):
        read_csv(str(f))


def test_read_csv_missing_columns(tmp_path):
    filepath = tmp_path / "bad.csv"
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["order_id", "product"])
        writer.writeheader()
        writer.writerow({"order_id": "1", "product": "X"})
    with pytest.raises(ValueError, match="Missing required columns"):
        read_csv(str(filepath))
