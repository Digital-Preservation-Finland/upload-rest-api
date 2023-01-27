"""Tests for file_entry module."""
from mongoengine import ValidationError
import pytest

from upload_rest_api.config import CONFIG
from upload_rest_api.models.file_entry import FileEntry


def test_correct_document_structure(files_col, monkeypatch):
    """Test document structure.

    Test that saved FileEntry has the same structure as the
    pre-MongoEngine implementation
    """
    # Configure UPLOAD_PROJECTS_PATH
    monkeypatch.setitem(CONFIG, "UPLOAD_PROJECTS_PATH",
                        '/upload_projects_path')

    file = FileEntry(
        path="/upload_projects_path/fake/path",
        checksum="6d48b69215369ecd27c1add71746989c",
        identifier="urn:uuid:foo_bar"
    )
    file.save()

    docs = list(files_col.find())

    assert len(docs) == 1
    assert docs[0] == {
        "_id": "/upload_projects_path/fake/path",
        "checksum": "6d48b69215369ecd27c1add71746989c",
        "identifier": "urn:uuid:foo_bar"
    }


@pytest.mark.parametrize(
    'path,error',
    [
        # Empty path
        ('', 'File path is not absolute'),
        # Relative path
        ('foo/bar',  'File path is not absolute'),
        # Not absolute path
        ('/foo/../bar', 'File path is not absolute'),
        # Not subpath of project root directory
        ('/foo/bar', 'File path is not subpath of any project directory'),
        # Project root directory
        ('/upload_projects_path/project_id',
         'File path is not subpath of any project directory'),
        # Projects directory
        ('/upload_projects_path',
         'File path is not subpath of any project directory'),
    ]
)
def test_path_validation(path, error, monkeypatch):
    """Test that creating FileEntry with invalid path raises error.

    :param path: absolute path of FileEntry
    :param error: error message
    """
    # Configure UPLOAD_PROJECTS_PATH
    monkeypatch.setitem(CONFIG, "UPLOAD_PROJECTS_PATH",
                        '/upload_projects_path')

    with pytest.raises(ValidationError) as exception_info:
        file_entry = FileEntry(path=path, checksum='foo', identifier='bar')
        file_entry.save()
    assert exception_info.value.errors['path'].message == error
