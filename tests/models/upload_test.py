"""Unit tests for upload module."""
from __future__ import annotations

import hashlib
import pathlib
import urllib
import tempfile

import pytest

from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.models.resource import File, Directory
from upload_rest_api.models.upload import (Upload, UploadConflictError,
                                           UploadError)
from tests.utils import update_nested_dict
from metax_access.template_data import FILE as TEMPLATE_FILE


@pytest.fixture(scope="function")
def clone_test_file(tmp_path):
    """
    Fixture function to create a copy of the given file in a temporary
    location.

    Needed because upload process will often attempt to delete the file;
    we cannot give a path to a test file directly.
    """
    def func(path: str | pathlib.Path):
        path = pathlib.Path(path)
        new_path = (tmp_path / path.name)
        new_path.write_bytes(path.read_bytes())

        return new_path

    return func


@pytest.mark.usefixtures('app')
@pytest.mark.parametrize(
    'file_path',
    (
        'tests/data/test.txt',
        'tests/data/test.zip',
    )
)
def test_store_file(
    requests_mock, clone_test_file, file_path
):
    """Test that Upload object creates correct metadata."""
    # Mock metax
    mock_post_metadata = requests_mock.post(
        "/v3/files/post-many", json = {}
    )
    requests_mock.get('/v3/files', json={'next': None, 'results': []})

    # Create an incomplete upload with one text file uploaded to
    # source path
    test_file = pathlib.Path(file_path)
    upload = Upload.create(
        File(project_id='test_project', path='foo/bar'), 123
    )
    upload.add_source(clone_test_file(test_file), None)

    # Create metadata
    upload.store_files(verify_source=False)

    # Check the metadata that was posted to Metax
    assert mock_post_metadata.call_count == 1
    metadata = mock_post_metadata.last_request.json()[0]

    assert metadata["storage_identifier"].startswith('urn:uuid:')
    assert metadata["filename"] == "bar"
    assert metadata["size"] == test_file.stat().st_size
    assert metadata["pathname"] == "/foo/bar"
    assert metadata["csc_project"] == 'test_project'
    assert "modified" in metadata
    assert "frozen" in metadata

    checksum = hashlib.md5(test_file.read_bytes()).hexdigest()
    assert metadata["checksum"] == f"md5:{checksum}"


@pytest.mark.usefixtures('app')
def test_file_metadata_conflict(mock_config, requests_mock, clone_test_file):
    """Test uploading a file that already has metadata in Metax."""
    # Create an incomplete upload with a text file copied to source path
    upload = Upload.create(
        File(project_id='test_project', path='path/file1'), 123
    )
    upload.add_source(clone_test_file("tests/data/test.txt"), None)

    # Mock metax.
    file = {
        'id': 2,
        'pathname': '/path/file1',
        'storage_identifier': 'foo',
        'is_sensitive': False
    }
    metax_files_api = requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get(
        '/v3/files?pathname=%2Fpath%2Ffile1&csc_project=test_project',
        json={
            'next': None,
            'results': [
                update_nested_dict(TEMPLATE_FILE, file)
            ]
        }
    )

    # Try to create metadata. Metadata creation should fail.
    with pytest.raises(UploadConflictError) as error:
        upload.store_files(verify_source=False)
    assert str(error.value) == ('Metadata could not be created because the'
                                ' file already has metadata')
    assert error.value.files == ['/path/file1']

    # Nothing should have been posted to Metax
    assert not metax_files_api.called

    # Temporary files should be removed
    tmp_dir = pathlib.Path(mock_config['UPLOAD_TMP_PATH'])
    assert not any(tmp_dir.iterdir())


@pytest.mark.usefixtures('app')
def test_file_metadata_multiple_conflicts(
        mock_config, requests_mock, clone_test_file):
    """
    Test uploading multiple files, some of which already have metadata in Metax.
    """
    # Try uploading files `file{1..5}` while `file3` and `file4` already exist
    # in Metax

    # Create an incomplete upload with a text file copied to source path
    upload = Upload.create(
        Directory(project_id='test_project', path='path'), 123
    )
    upload.add_source(clone_test_file("tests/data/5_files.tar"), None)

    # Mock metax.
    metax_files_api = requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get(
        '/v3/files?csc_project=test_project',
        json={
            'next': None,
            'results': [
                update_nested_dict(
                    TEMPLATE_FILE,
                    {
                        "id": 3, "pathname": "/path/file3.txt",
                        "storage_identifier": "foo3"
                    }
                ),
                update_nested_dict(
                    TEMPLATE_FILE,
                    {
                        "id": 4, "pathname": "/path/file4.txt",
                        "storage_identifier": "foo4"
                    }
                )
            ]
        }
    )

    # Try to create metadata. Metadata creation should fail.
    with pytest.raises(UploadConflictError) as error:
        upload.store_files(verify_source=False)
    assert str(error.value) == ('Metadata could not be created because some '
                                'files already have metadata')
    assert error.value.files == ['path/file3.txt', 'path/file4.txt']

    # Nothing should have been posted to Metax
    assert not metax_files_api.called

    # Temporary files should be removed
    tmp_dir = pathlib.Path(mock_config['UPLOAD_TMP_PATH'])
    assert not any(tmp_dir.iterdir())


@pytest.mark.usefixtures('app')
def test_add_source(clone_test_file):
    """Test add_source method.

    Test that checksum is saved to database.
    """
    new_upload = Upload.create(
        File(project_id='test_project', path='path/file1'), 123
    )
    new_upload.add_source(
        clone_test_file("tests/data/test.txt"), checksum='foobar'
    )

    upload_from_database = Upload.get(id=new_upload.id)
    assert upload_from_database.source_checksum == 'foobar'

    # Release the file lock
    lock_manager = ProjectLockManager()
    lock_manager.release(new_upload.project.id, new_upload.storage_path)


@pytest.mark.usefixtures('app')
@pytest.mark.parametrize(
    ['checksum', 'verify'],
    (
        ['foo', True],
        ['foo', False],
        [None, False],
        ['wrong-checksum', False]
    )
)
def test_checksum(
        checksum, verify, requests_mock, clone_test_file, mock_get_file_checksum):
    """Test that checksum is not computed needlessly."""
    # Mock metax.
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get('/v3/files?pathname=%2Fpath%2Ffile1&csc_project=test_project',
                      json={'next': None, 'results': []})

    # Upload a file. Checksum should be computed only if it must be
    # verified.
    upload = Upload.create(
        File(project_id='test_project', path='path/file1'), 123
    )
    upload.add_source(clone_test_file("tests/data/test.txt"), checksum=checksum)
    upload.store_files(verify_source=verify)

    if verify or not checksum:
        mock_get_file_checksum.assert_called_once()
    else:
        mock_get_file_checksum.assert_not_called()


@pytest.mark.usefixtures('app')
def test_invalid_checksum(clone_test_file):
    """Test that upload fails if invalid source checksum is provided."""
    upload = Upload.create(
        File(project_id='test_project', path='path/file1'), 123
    )
    upload.add_source(
        clone_test_file("tests/data/test.txt"), checksum='wrong-checksum'
    )

    with pytest.raises(
        UploadError,
        match='Checksum of uploaded file does not match provided checksum.'
    ):
        upload.store_files(verify_source=True)


@pytest.mark.usefixtures('app')  # Creates test_project
@pytest.mark.parametrize(
    ['archive', "existing_file", "conflicts"],
    [
        # tar archive writes directory to existing file path
        ("tests/data/test.tar.gz",
         'foo/test',
         ["/foo/test"]),
        # tar archive writes file in place of to existing directory
        ("tests/data/test.tar.gz",
         'foo/test/test.txt/bar',
         ["/foo/test/test.txt"]),
        # tar archive writes file to existing file path
        ("tests/data/test.tar.gz",
         'foo/test/test.txt',
         ["/foo/test/test.txt"]),
        # zip archive writes directory to existing file path
        ("tests/data/test.zip",
         'foo/test',
         ["/foo/test/"]),
        # zip archive writes file in place of to existing directory
        ("tests/data/test.zip",
         'foo/test/test.txt/bar',
         ["/foo/test/test.txt"]),
        # zip archive writes file to existing file path
        ("tests/data/test.zip",
         'foo/test/test.txt',
         ["/foo/test/test.txt"]),
    ]
)
def test_upload_archive_conflict(
    clone_test_file, archive, existing_file, conflicts, requests_mock
):
    """Test uploading archive that would overwrite files or directories.

    :param archive: path to test archive
    :param existing_file: File path that will be populated before
                          uploading the the test archive
    :param conflicts: List of expected file conflicts in HTTP response
    :param test_client: Flask test client
    :param test_auth: authentication headers
    :param requests_mock: HTTP request mocker
    """
    # Mock metax
    requests_mock.post('/v3/files/post-many', json={})
    requests_mock.get(f'/v3/files?pathname=%2F{urllib.parse.quote(existing_file, safe="")}&csc_project=test_project',
                      json={'next': None, 'results': []})


    # Upload a file to a path that will cause a conflict when the
    # archive is uploaded
    upload = Upload.create(
        File(project_id='test_project', path=existing_file), 123
    )
    upload.add_source(clone_test_file("tests/data/test.txt"), checksum=None)
    upload.store_files(verify_source=False)

    # Try to upload an archive that will overwrite the file that was
    # just uploaded (or its parent directory).
    upload = Upload.create(Directory(project_id='test_project', path='foo'), 123)
    upload.add_source(clone_test_file(archive), checksum=None)
    with pytest.raises(UploadConflictError) as error:
        upload.store_files(verify_source=False)
    assert str(error.value) == 'Some files already exist'
    assert error.value.files == conflicts


@pytest.mark.usefixtures('app')  # Creates test_project
def test_upload_file_as_archive(clone_test_file):
    """Test uploading a reqular file as an archive."""
    upload = Upload.create(
        Directory(project_id='test_project', path='foo'), 123
    )
    upload.add_source(clone_test_file("tests/data/test.txt"), checksum=None)
    with pytest.raises(UploadError) as error:
        upload.store_files(verify_source=False)

    assert str(error.value) == 'Uploaded file is not a supported archive'
