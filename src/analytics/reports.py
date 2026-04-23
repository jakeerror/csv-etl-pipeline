import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy import text
from src.db.connection import get_session

logger = logging.getLogger(__name__)
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)


TOP_PRODUCTS_SQL = """
WITH product_totals AS (
    SELECT
        product,
        category,
        SUM(quantity)           AS total_qty,
        SUM(total)              AS revenue,
        COUNT(*)                AS orders_count
    FROM sales
    GROUP BY product, category
),
ranked AS (
    SELECT *,
        RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS rank_in_category
    FROM product_totals
)
SELECT
    rank_in_category  AS rank,
    category,
    product,
    total_qty,
    ROUND(revenue, 2) AS revenue,
    orders_count
FROM ranked
WHERE rank_in_category <= 5
ORDER BY category, rank_in_category;
"""

MONTHLY_DYNAMICS_SQL = """
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', sale_date)  AS month,
        region,
        SUM(total)                       AS revenue,
        COUNT(*)                         AS orders
    FROM sales
    GROUP BY 1, 2
)
SELECT
    TO_CHAR(month, 'YYYY-MM')          AS month,
    region,
    ROUND(revenue, 2)                   AS revenue,
    orders,
    ROUND(
        revenue - LAG(revenue) OVER (PARTITION BY region ORDER BY month), 2
    )                                   AS revenue_delta
FROM monthly
ORDER BY region, month;
"""

REGION_SHARE_SQL = """
WITH totals AS (
    SELECT region, ROUND(SUM(total), 2) AS revenue
    FROM sales
    GROUP BY region
),
grand AS (
    SELECT SUM(revenue) AS grand_total FROM totals
)
SELECT
    t.region,
    t.revenue,
    ROUND(t.revenue / g.grand_total * 100, 2) AS share_pct
FROM totals t, grand g
ORDER BY revenue DESC;
"""


def _fetch(sql: str) -> list[dict]:
    session = get_session()
    try:
        result = session.execute(text(sql))
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]
    finally:
        session.close()


def _export(data: list[dict], name: str, fmt: str):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if fmt == "json":
        path = REPORTS_DIR / f"{name}_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    else:
        path = REPORTS_DIR / f"{name}_{ts}.csv"
        if data:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
    logger.info(f"Report saved: {path}")


def run_reports(export_format: str = "csv"):
    reports = [
        ("top_products_by_category", TOP_PRODUCTS_SQL),
        ("monthly_dynamics_by_region", MONTHLY_DYNAMICS_SQL),
        ("region_revenue_share", REGION_SHARE_SQL),
    ]
    for name, sql in reports:
        data = _fetch(sql)
        _export(data, name, export_format)
        logger.info(f"{name}: {len(data)} rows")
