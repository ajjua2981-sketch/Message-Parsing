import logging
import oracledb
from config import OracleConfig

logger = logging.getLogger(__name__)


class OracleHandler:
    def __init__(self):
        self._conn = None
        self._table = OracleConfig.TABLE

    def connect(self):
        self._conn = oracledb.connect(
            user=OracleConfig.USER,
            password=OracleConfig.PASSWORD,
            dsn=OracleConfig.DSN,
        )
        logger.info("Connected to Oracle database")

    def disconnect(self):
        if self._conn:
            self._conn.close()
            logger.info("Oracle connection closed")

    def fetch_by_reference_id(self, reference_id: str) -> list[dict]:
        """Return all rows matching the reference_id, ordered by insertion order."""
        sql = f"""
            SELECT *
            FROM {self._table}
            WHERE REFERENCE_ID = :ref_id
            ORDER BY ROWID ASC
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, ref_id=reference_id)
            columns = [col[0] for col in cur.description]
            rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def insert_record(self, data: dict):
        """Insert a new record. Adjust columns to match your table schema."""
        # TODO: replace column names and :bind variables with your actual schema
        sql = f"""
            INSERT INTO {self._table}
                (REFERENCE_ID, PAYLOAD, STATUS)
            VALUES
                (:reference_id, :payload, :status)
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, {
                "reference_id": data.get("REFERENCE_ID"),
                "payload": data.get("PAYLOAD"),
                "status": data.get("STATUS", "NEW"),
            })
        self._conn.commit()
        logger.info("Inserted new record for REFERENCE_ID=%s", data.get("REFERENCE_ID"))

    def update_record(self, reference_id: str, rowid: str, data: dict):
        """Update the first matching record identified by its ROWID."""
        # TODO: replace SET clause columns with your actual schema
        sql = f"""
            UPDATE {self._table}
            SET
                PAYLOAD = :payload,
                STATUS  = :status
            WHERE ROWID = :rowid
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, {
                "payload": data.get("PAYLOAD"),
                "status": data.get("STATUS", "PROCESSED"),
                "rowid": rowid,
            })
        self._conn.commit()
        logger.info("Updated record ROWID=%s for REFERENCE_ID=%s", rowid, reference_id)
