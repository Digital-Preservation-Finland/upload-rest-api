"""Unit tests for upload module."""
import hashlib
import pathlib

import pytest

from upload_rest_api.models.upload import (Upload, UploadConflictError,
                                           UploadError)


@pytest.mark.usefixtures('app')
@pytest.mark.parametrize(
    ['file_path', 'mimetype'],
    (
        ['tests/data/test.txt', 'text/plain'],
        ['tests/data/test.zip', 'application/zip'],
    )
)
def test_store_file(
    mock_config, requests_mock, file_path, mimetype
):
    """Test that Upload object creates correct metadata."""
    # Mock metax
    metax_files_api = requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'next': None, 'results': []})

    # Create an incomplete upload with one text file uploaded to
    # source path
    test_file = pathlib.Path(file_path)
    project = 'test_project'
    upload = Upload.create(project, 'foo/bar', 123)
    with open(test_file, 'rb') as file:
        upload.add_source(file, None)

    # Create metadata
    upload.store_files(verify_source=False)

    # Check the metadata that was posted to Metax
    metadata = metax_files_api.last_request.json()[0]
    assert metadata["identifier"].startswith('urn:uuid:')
    assert metadata["file_name"] == "bar"
    assert metadata["file_format"] == mimetype
    assert metadata["byte_size"] == test_file.stat().st_size
    assert metadata["file_path"] == "/foo/bar"
    assert metadata["project_identifier"] == project
    assert "file_uploaded" in metadata
    assert "file_modified" in metadata
    assert "file_frozen" in metadata
    assert metadata['checksum']["algorithm"] == "MD5"
    assert metadata['checksum']["value"] \
        == hashlib.md5(test_file.read_bytes()).hexdigest()
    assert "checked" in metadata['checksum']
    assert metadata["file_storage"] == mock_config["STORAGE_ID"]


@pytest.mark.usefixtures('app')
def test_file_metadata_conflict(mock_config, requests_mock):
    """Test uploading a file that already has metadata in Metax."""
    # Create an incomplete upload with a text file copied to source path
    upload = Upload.create('test_project', 'path/file1', 123)
    with open('tests/data/test.txt', 'rb') as file:
        upload.add_source(file, None)

    # Mock metax.
    metax_files_api = requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get(
        '/rest/v2/files',
        json={
            'next': None,
            'results': [
                {
                    'file_path': '/path/file1',
                    'id': 2,
                    'identifier': '2',
                    'file_storage': {'identifier': 'foo'}
                }
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
@pytest.mark.parametrize(
    ['checksum', 'verify'],
    (
        ['foo', True],
        ['foo', False],
        [None, False],
        ['wrong-checksum', False]
    )
)
def test_checksum(checksum, verify, requests_mock, mock_get_file_checksum):
    """Test that checksum is not computed needlessly."""
    # Mock metax.
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'results': []})

    # Upload a file. Checksum should be computed only if it must be
    # verified.
    upload = Upload.create('test_project', 'path/file1', 123)
    with open('tests/data/test.txt', 'rb') as source_file:
        upload.add_source(source_file, checksum=checksum)
    upload.store_files(verify_source=verify)

    if verify or not checksum:
        mock_get_file_checksum.assert_called_once()
    else:
        mock_get_file_checksum.assert_not_called()


@pytest.mark.usefixtures('app')
def test_invalid_checksum():
    """Test that upload fails if invalid source checksum is provided."""
    upload = Upload.create('test_project', 'path/file1', 123)
    with open('tests/data/test.txt', 'rb') as source_file:
        upload.add_source(source_file, checksum='wrong-checksum')

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
    archive, existing_file, conflicts, requests_mock
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
    requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'next': None, 'results': []})

    # Upload a file to a path that will cause a conflict when the
    # archive is uploaded
    upload = Upload.create(
        'test_project', existing_file, 123, type_='file'
    )
    with open('tests/data/test.txt', 'rb') as source_file:
        upload.add_source(source_file, checksum=None)
    upload.store_files(verify_source=False)

    # Try to upload an archive that will overwrite the file that was
    # just uploaded (or its parent directory).
    upload = Upload.create(
        'test_project', 'foo', 123, type_='archive'
    )
    with open(archive, 'rb') as source_file:
        upload.add_source(source_file, checksum=None)
    with pytest.raises(UploadConflictError) as error:
        upload.store_files(verify_source=False)
    assert str(error.value) == 'Some files already exist'
    assert error.value.files == conflicts


@pytest.mark.usefixtures('app')  # Creates test_project
def test_upload_file_as_archive():
    """Test uploading a reqular file as an archive."""
    upload = Upload.create(
        'test_project', 'foo', 123, type_='archive'
    )
    with open('tests/data/test.txt', 'rb') as source_file:
        upload.add_source(source_file, checksum=None)
    with pytest.raises(UploadError) as error:
        upload.store_files(verify_source=False)

    assert str(error.value) == 'Uploaded file is not a supported archive'
