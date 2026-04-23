import logging
from sqlalchemy.dialects.postgresql import insert
from src.db.connection import get_session
from src.db.models import Sale

logger = logging.getLogger(__name__)


def load_to_db(rows: list[dict]) -> int:
    if not rows:
        logger.warning("No data to load")
        return 0

    session = get_session()
    inserted = 0

    try:
        for chunk_start in range(0, len(rows), 500):
            chunk = rows[chunk_start:chunk_start + 500]

            stmt = insert(Sale).values(chunk)
            stmt = stmt.on_conflict_do_nothing(index_elements=["order_id"])
            result = session.execute(stmt)
            session.commit()
            inserted += result.rowcount

        logger.info(f"Inserted {inserted} rows (duplicates skipped)")
        return inserted

    except Exception as e:
        session.rollback()
        logger.error(f"DB load failed: {e}")
        raise
    finally:
        session.close()
