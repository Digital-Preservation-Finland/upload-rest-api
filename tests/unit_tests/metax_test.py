"""Unit tests for metadata generation."""
import pytest

from upload_rest_api.metax import get_metax_client


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
    requests_mock.get('/rest/v2/datasets/qux', json={})

    # Set METAX_SSL_VERIFICATION option
    mock_config['METAX_SSL_VERIFICATION'] = verify

    # Try to get a dataset from Metax.
    client = get_metax_client()
    client.get_dataset('qux')
    assert requests_mock.last_request.verify is verify
