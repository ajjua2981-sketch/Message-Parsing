import logging
from config import AppConfig
from parser import xml_to_json
from db import OracleHandler

logger = logging.getLogger(__name__)


def _extract_reference_id(data: dict) -> str:
    """Navigate the parsed XML dict to pull out the reference ID.

    xmltodict wraps everything under the root element tag, e.g.:
        {"Message": {"ReferenceID": "ABC123", ...}}
    Adjust the key path once you know your XML schema.
    """
    field = AppConfig.REFERENCE_ID_FIELD
    # Try the top-level first, then one level deep under the root element
    if field in data:
        return str(data[field])
    for root_value in data.values():
        if isinstance(root_value, dict) and field in root_value:
            return str(root_value[field])
    raise KeyError(f"Reference ID field '{field}' not found in message")


def _flatten_for_db(data: dict, json_string: str) -> dict:
    """Map parsed message fields to Oracle column values.

    TODO: expand this once you share the real XML structure and table schema.
    For now, the entire JSON payload is stored in the PAYLOAD column.
    """
    field = AppConfig.REFERENCE_ID_FIELD
    ref_id = _extract_reference_id(data)

    # Walk one level into the root element if needed
    inner = data
    for v in data.values():
        if isinstance(v, dict):
            inner = v
            break

    return {
        "REFERENCE_ID": ref_id,
        "PAYLOAD": json_string,
        "STATUS": inner.get("Status", "NEW"),
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
