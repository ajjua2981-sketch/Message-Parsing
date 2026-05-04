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
        """Fetch all records matching the reference ID, ordered by insertion order."""
        sql = f"""
            SELECT ROWID, {self._table}.*
            FROM {self._table}
            WHERE REFERENCE_ID = :ref_id
            ORDER BY ROWID ASC
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, ref_id=reference_id)
            columns = [col[0] for col in cur.description]
            rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def update_payload(self, rowid: str, reference_id: str, payload: str):
        """Update the PAYLOAD column of the record identified by ROWID."""
        sql = f"""
            UPDATE {self._table}
            SET PAYLOAD = :payload
            WHERE ROWID = :rowid
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, {
                "payload": payload,
                "rowid":   rowid,
            })
        self._conn.commit()
        logger.info("Updated PAYLOAD for REFERENCE_ID=%s", reference_id)
