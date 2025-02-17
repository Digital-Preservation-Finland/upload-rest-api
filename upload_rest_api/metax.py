"""Create Metax client."""
from metax_access import Metax

from upload_rest_api.config import CONFIG


singleton = {'metax_client': None}


def get_metax_client():
    """Get Metax client."""
    if not singleton['metax_client']:
        # Create metax client
        url = CONFIG.get("METAX_URL")
        token = CONFIG.get("METAX_TOKEN")
        verify = CONFIG.get("METAX_SSL_VERIFICATION", True)
        singleton['metax_client'] = Metax(url=url, token=token, verify=verify)

    return singleton['metax_client']
