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
        """Insert a new record. TODO: align column names with the real table schema."""
        sql = f"""
            INSERT INTO {self._table}
                (REFERENCE_ID, STATUS_CODE, STATUS_MESSAGE, PA_REFERENCE_TS, GUID, PAYLOAD)
            VALUES
                (:reference_id, :status_code, :status_message, :pa_reference_ts, :guid, :payload)
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, {
                "reference_id":    data.get("REFERENCE_ID"),
                "status_code":     data.get("STATUS_CODE"),
                "status_message":  data.get("STATUS_MESSAGE"),
                "pa_reference_ts": data.get("PA_REFERENCE_TS"),
                "guid":            data.get("GUID"),
                "payload":         data.get("PAYLOAD"),
            })
        self._conn.commit()
        logger.info("Inserted new record for REFERENCE_ID=%s", data.get("REFERENCE_ID"))

    def update_record(self, reference_id: str, rowid: str, data: dict):
        """Update the first matching record identified by its ROWID. TODO: align column names."""
        sql = f"""
            UPDATE {self._table}
            SET
                STATUS_CODE     = :status_code,
                STATUS_MESSAGE  = :status_message,
                PA_REFERENCE_TS = :pa_reference_ts,
                GUID            = :guid,
                PAYLOAD         = :payload
            WHERE ROWID = :rowid
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, {
                "status_code":     data.get("STATUS_CODE"),
                "status_message":  data.get("STATUS_MESSAGE"),
                "pa_reference_ts": data.get("PA_REFERENCE_TS"),
                "guid":            data.get("GUID"),
                "payload":         data.get("PAYLOAD"),
                "rowid":           rowid,
            })
        self._conn.commit()
        logger.info("Updated record ROWID=%s for REFERENCE_ID=%s", rowid, reference_id)
