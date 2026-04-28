import logging
from config import AppConfig
from parser import xml_to_json
from db import OracleHandler

logger = logging.getLogger(__name__)


def _extract_reference_id(data: dict) -> str:
    """Walk the fixed dot-notation path defined in REFERENCE_ID_PATH to extract the reference ID.

    Each segment in the path must be an exact key in the parsed XML dict.
    Raises KeyError with a descriptive message if any segment is missing.
    """
    path = AppConfig.REFERENCE_ID_PATH
    segments = path.split(".")
    current = data

    for segment in segments:
        if not isinstance(current, dict) or segment not in current:
            raise KeyError(
                f"Could not resolve path '{path}' — segment '{segment}' not found. "
                f"Available keys: {list(current.keys()) if isinstance(current, dict) else type(current).__name__}"
            )
        current = current[segment]

    return str(current)


def _flatten_for_db(data: dict, json_string: str) -> dict:
    """Map parsed message fields to Oracle column values.

    TODO: expand with real column mappings once the table schema is shared.
    The full JSON payload is stored in PAYLOAD for now.
    """
    return {
        "REFERENCE_ID": _extract_reference_id(data),
        "PAYLOAD": json_string,
        "STATUS": "NEW",
    }


def process_message(xml_string: str, db: OracleHandler):
    """Full pipeline: XML → JSON → Oracle upsert."""
    # 1. Parse
    data, json_string = xml_to_json(xml_string)
    logger.debug("Parsed JSON:\n%s", json_string)

    # 2. Extract reference ID
    reference_id = _extract_reference_id(data)
    logger.info("Processing message with REFERENCE_ID=%s", reference_id)

    # 3. Build the row dict for DB operations
    row_data = _flatten_for_db(data, json_string)

    # 4. Fetch existing records
    existing = db.fetch_by_reference_id(reference_id)
    count = len(existing)
    logger.info("Found %d existing record(s) for REFERENCE_ID=%s", count, reference_id)

    if count == 0:
        # No existing record — insert fresh
        db.insert_record(row_data)

    else:
        # One or more records exist — update the first one (lowest ROWID / insertion order)
        first_row = existing[0]
        rowid = first_row.get("ROWID") or first_row.get("rowid")

        if rowid is None:
            # Fallback: re-query to get the ROWID explicitly
            rowid = _fetch_first_rowid(db, reference_id)

        db.update_record(reference_id, rowid, row_data)

        if count > 1:
            logger.warning(
                "Multiple records (%d) found for REFERENCE_ID=%s — updated the first one",
                count, reference_id,
            )


def _fetch_first_rowid(db: OracleHandler, reference_id: str) -> str:
    """Fetch the ROWID of the first inserted record when it wasn't in SELECT *."""
    from config import OracleConfig
    sql = f"""
        SELECT ROWID FROM {OracleConfig.TABLE}
        WHERE REFERENCE_ID = :ref_id
        ORDER BY ROWID ASC
        FETCH FIRST 1 ROWS ONLY
    """
    with db._conn.cursor() as cur:
        cur.execute(sql, ref_id=reference_id)
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Could not find ROWID for REFERENCE_ID={reference_id}")
    return row[0]
