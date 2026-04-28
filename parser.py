import json
import logging
import xmltodict

logger = logging.getLogger(__name__)


def xml_to_dict(xml_string: str) -> dict:
    """Parse XML string into a Python dict."""
    try:
        parsed = xmltodict.parse(xml_string)
        logger.debug("XML parsed successfully")
        return parsed
    except Exception as e:
        logger.error("Failed to parse XML: %s", e)
        raise


def dict_to_json(data: dict) -> str:
    """Serialize a dict to a JSON string."""
    return json.dumps(data, indent=2)


def xml_to_json(xml_string: str) -> tuple[dict, str]:
    """Convert XML string to both a dict and a JSON string.

    Returns:
        (data_dict, json_string)
    """
    data = xml_to_dict(xml_string)
    return data, dict_to_json(data)
