"""Tests for directory API endpoints"""
from pathlib import Path


def test_create_directory(app, test_auth, test_client):
    """Test creating a directory"""
    project_dir = Path(app.config.get("UPLOAD_PROJECTS_PATH")) / "test_project"

    assert not (project_dir / "foo" / "bar").is_dir()

    response = test_client.post(
        "/v1/directories/test_project/foo/bar",
        headers=test_auth
    )

    assert response.json == {
        "dir_path": "/foo/bar",
        "status": "created"
    }

    assert (project_dir / "foo" / "bar").is_dir()


def test_create_existing_directory(app, test_auth, test_client):
    """Attempt to create an existing directory and ensure it returns an error.
    """
    project_dir = Path(app.config.get("UPLOAD_PROJECTS_PATH")) / "test_project"
    (project_dir / "foo" / "bar").mkdir(parents=True)

    response = test_client.post(
        "/v1/directories/test_project/foo/bar",
        headers=test_auth
    )

    assert response.status_code == 409
    assert response.json == {
        "code": 409,
        "error": "Directory already exists"
    }


def test_no_rights(user2_token_auth, test_client):
    """
    Test that attempting to access a project without permission results
    in a 403 Forbidden response
    """
    response = test_client.post(
        "/v1/directories/test_project/foo/bar", headers=user2_token_auth
    )

    assert response.status_code == 403
