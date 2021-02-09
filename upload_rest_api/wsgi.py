"""Module that allows deployment using WSGI."""

from upload_rest_api import app

application = app.create_app()
