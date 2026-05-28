import logging
import re
import oracledb
from config import OracleConfig

logger = logging.getLogger(__name__)

_VALID_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_$#]{0,127}$')


class OracleHandler:
    def __init__(self):
        self._conn = None
        table = OracleConfig.TABLE
        if not _VALID_IDENTIFIER.match(table):
            raise ValueError(f"ORACLE_TABLE contains an invalid identifier: {table!r}")
        self._table = table
        self._payload_col = OracleConfig.PAYLOAD_COLUMN

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
        """Update the payload column of the record identified by ROWID."""
        sql = f"""
            UPDATE {self._table}
            SET {self._payload_col} = :payload
            WHERE ROWID = :rowid
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, {
                "payload": payload,
                "rowid":   rowid,
            })
        self._conn.commit()
        logger.info("Updated %s for REFERENCE_ID=%s", self._payload_col, reference_id)
