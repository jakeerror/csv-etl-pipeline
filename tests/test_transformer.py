import pytest
from src.etl.transformer import transform


VALID_ROW = {
    "order_id": "ORD-001",
    "product": "Laptop Pro",
    "category": "electronics",
    "quantity": "2",
    "price": "49999.99",
    "sale_date": "2024-03-15",
    "region": "moscow",
}


def test_transform_valid_row():
    result = transform([VALID_ROW])
    assert len(result) == 1
    row = result[0]
    assert row["order_id"] == "ORD-001"
    assert row["quantity"] == 2
    assert float(row["price"]) == 49999.99
    assert float(row["total"]) == 99999.98
    assert row["category"] == "Electronics"
    assert row["region"] == "Moscow"


def test_transform_skips_zero_quantity():
    row = {**VALID_ROW, "quantity": "0"}
    result = transform([row])
    assert result == []


def test_transform_skips_negative_price():
    row = {**VALID_ROW, "price": "-100"}
    result = transform([row])
    assert result == []


def test_transform_skips_invalid_date():
    row = {**VALID_ROW, "sale_date": "15-03-2024"}
    result = transform([row])
    assert result == []


def test_transform_skips_empty_order_id():
    row = {**VALID_ROW, "order_id": "  "}
    result = transform([row])
    assert result == []


def test_transform_multiple_rows_partial_valid():
    bad_row = {**VALID_ROW, "order_id": "ORD-002", "quantity": "abc"}
    result = transform([VALID_ROW, bad_row])
    assert len(result) == 1
    assert result[0]["order_id"] == "ORD-001"


def test_transform_price_with_thousands_separator():
    # "1,500.00" — запятая как разделитель тысяч
    row = {**VALID_ROW, "price": "1,500.00"}
    result = transform([row])
    assert len(result) == 1
    assert float(result[0]["price"]) == 1500.0


def test_transform_empty_input():
    assert transform([]) == []
