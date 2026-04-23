import argparse
import logging
from src.etl.reader import read_csv
from src.etl.transformer import transform
from src.etl.loader import load_to_db
from src.analytics.reports import run_reports
from src.db.connection import init_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main(filepath: str, export: str = "csv"):
    logger.info("Starting ETL pipeline")

    init_db()

    raw = read_csv(filepath)
    logger.info(f"Loaded {len(raw)} rows from {filepath}")

    clean = transform(raw)
    logger.info(f"After transformation: {len(clean)} rows")

    load_to_db(clean)
    logger.info("Data loaded to PostgreSQL")

    run_reports(export_format=export)
    logger.info("Reports generated")

    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSV ETL Pipeline")
    parser.add_argument("--file", required=True, help="Path to CSV file")
    parser.add_argument("--export", default="csv", choices=["csv", "json"], help="Export format")
    args = parser.parse_args()
    main(args.file, args.export)
