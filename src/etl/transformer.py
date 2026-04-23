import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)


def transform(rows: list[dict]) -> list[dict]:
    clean = []
    skipped = 0

    for i, row in enumerate(rows, start=2):
        try:
            order_id = row["order_id"].strip()
            if not order_id:
                raise ValueError("order_id is empty")

            product = row["product"].strip()
            category = row["category"].strip().title()
            region = row["region"].strip().title()

            quantity = int(row["quantity"])
            if quantity <= 0:
                raise ValueError(f"quantity must be positive, got {quantity}")

            price_str = row["price"].strip()
            if price_str.count(",") == 1 and price_str.count(".") == 0:
                price_str = price_str.replace(",", ".")
            else:
                price_str = price_str.replace(",", "")
            price = Decimal(price_str)
            if price <= 0:
                raise ValueError(f"price must be positive, got {price}")

            total = round(price * quantity, 2)

            sale_date = datetime.strptime(row["sale_date"].strip(), "%Y-%m-%d").date()

            clean.append({
                "order_id": order_id,
                "product": product,
                "category": category,
                "quantity": quantity,
                "price": price,
                "total": total,
                "sale_date": sale_date,
                "region": region,
            })

        except (ValueError, InvalidOperation, KeyError) as e:
            logger.warning(f"Row {i} skipped: {e} | data: {row}")
            skipped += 1

    if skipped:
        logger.info(f"Skipped {skipped} invalid rows")

    return clean
