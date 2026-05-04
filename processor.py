import logging
from config import AppConfig
from parser import xml_to_json
from db import OracleHandler

logger = logging.getLogger(__name__)


def _extract_reference_id(data: dict) -> str:
    """Walk the fixed dot-notation path defined in REFERENCE_ID_PATH to extract the reference ID."""
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


def process_message(xml_string: str, db: OracleHandler):
    """Full pipeline: XML → JSON → fetch by reference ID → update PAYLOAD."""

    # 1. Parse XML to JSON
    data, json_string = xml_to_json(xml_string)

    # 2. Extract reference ID
    reference_id = _extract_reference_id(data)
    logger.info("Processing REFERENCE_ID=%s", reference_id)

    # 3. Fetch existing records by reference ID
    existing = db.fetch_by_reference_id(reference_id)
    count = len(existing)

    if count == 0:
        logger.warning("No record found for REFERENCE_ID=%s — skipping", reference_id)
        return

    # 4. Update PAYLOAD of the first record
    first_row = existing[0]
    rowid = first_row.get("ROWID") or first_row.get("rowid")
    db.update_payload(rowid, reference_id, json_string)

    if count > 1:
        logger.warning(
            "Multiple records (%d) found for REFERENCE_ID=%s — updated the first one",
            count, reference_id,
        )
