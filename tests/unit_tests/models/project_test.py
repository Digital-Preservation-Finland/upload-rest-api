from pathlib import Path

from upload_rest_api.models.project import Project, _get_dir_size


def test_correct_document_structure(projects_col):
    """
    Test that saved Project has the same structure as the pre-MongoEngine
    implementation
    """
    project = Project(
        id="test_project",
        used_quota=0,
        quota=1234
    )
    project.save()

    docs = list(projects_col.find())
    assert len(docs) == 1
    assert docs[0] == {
        "_id": "test_project",
        "used_quota": 0,
        "quota": 1234
    }


def test_dir_size():
    """Test that dir sizes are calculated correctly.

    Dirs that do not exist should return size 0.
    """
    # Existing dir
    assert _get_dir_size("tests/data/get_dir_size") == 8

    # Non-existent dir
    assert _get_dir_size("tests/data/test") == 0


def test_create_project(test_mongo, mock_config):
    """Test creating new project."""
    Project.create("test_project")

    project_dict = test_mongo.upload.projects.find_one({"_id": "test_project"})

    assert project_dict["quota"] == 5 * 1024**3
    assert project_dict["used_quota"] == 0

    # Project directory should be created
    project_directory \
        = Path(mock_config["UPLOAD_PROJECTS_PATH"]) / "test_project"
    assert project_directory.is_dir()
