"""Tests for ``upload_rest_api.app`` module"""

import os
from upload_rest_api.app import create_app


def test_index():
    """Test the application index page.

    :returns: None
    """
    app = create_app()
    with app.test_client() as client:
        response = client.get("/")

    assert response.status_code == 404
