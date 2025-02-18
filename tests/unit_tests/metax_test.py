"""Unit tests for metadata generation."""
import pytest

import upload_rest_api.metax
from tests.metax_data.utils import TEMPLATE_DATASET


@pytest.mark.parametrize('verify', [True, False])
def test_metax_ssl_verification(requests_mock, verify, mock_config):
    """Test Metax HTTPS connection verification.

    HTTPS connection to Metax should be verified if
    METAX_SSL_VERIFICATION option is used.

    :param requests_mock: HTTP request mocker
    :param verify: value for MetaxClient `verify` parameter
    :param mock_config: Configuration
    """
    # Mock metax
    requests_mock.get('/v3/datasets/qux', json=TEMPLATE_DATASET)

    # Set METAX_SSL_VERIFICATION option
    mock_config['METAX_SSL_VERIFICATION'] = verify

    # "Reset" Metax client so that new Metax instance is created using
    # the mocked configuration
    upload_rest_api.metax.singleton = {'metax_client': None}

    # Try to get a dataset from Metax.
    client = upload_rest_api.metax.get_metax_client()
    client.get_dataset('qux')
    assert requests_mock.last_request.verify is verify
