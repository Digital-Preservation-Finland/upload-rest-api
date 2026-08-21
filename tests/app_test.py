"""Tests for `upload_rest_api.app` module."""
import logging


def test_access_log_includes_username(test_client, test_auth, caplog):
    """Test that the access log includes the username of the authenticated
    user.

    :param test_client: Flask test client
    :param test_auth: Authentication headers for "test_user"
    :param caplog: Log catcher
    """
    caplog.set_level(logging.INFO)

    response = test_client.get("/v1/users/projects", headers=test_auth)

    assert response.status_code == 200
    assert any(
        record.message == "user=test_user"
        for record in caplog.records
    )


def test_access_log_shows_dash_for_unauthenticated_request(
        test_client, caplog):
    """Test that the access log shows '-' as username when the request
    is not authenticated.

    :param test_client: Flask test client
    :param caplog: Log catcher
    """
    caplog.set_level(logging.INFO)

    response = test_client.get("/v1/users/projects")

    assert response.status_code == 401
    assert any(
        record.message == "user=-"
        for record in caplog.records
    )
