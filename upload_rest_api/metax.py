"""Create Metax client."""
from metax_access import Metax

from upload_rest_api.config import CONFIG


def get_metax_client():
    """Get Metax client."""
    url = CONFIG.get("METAX_URL")
    user = CONFIG.get("METAX_USER")
    password = CONFIG.get("METAX_PASSWORD")
    verify = CONFIG.get("METAX_SSL_VERIFICATION", True)

    return Metax(url, user, password, verify=verify)


metax_client = get_metax_client()
