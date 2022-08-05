"""Unit tests for metadata generation."""
import pathlib
import shutil

import pytest

import upload_rest_api.upload
import upload_rest_api.jobs.utils


def test_mimetype():
    """Test that _get_mimetype() returns correct MIME types."""
    assert upload_rest_api.upload._get_mimetype("tests/data/test.txt")\
        == "text/plain"
    assert upload_rest_api.upload._get_mimetype("tests/data/test.zip")\
        == "application/zip"


@pytest.mark.usefixtures('app')
def test_store_file(mock_config, requests_mock):
    """Test that Upload object creates correct metadata."""
    # Mock metax
    metax_files_api = requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get('/rest/v2/files', json={'next': None, 'results': []})

    # Create an incomplete upload with one text file in temporary
    # project directory
    project = 'test_project'
    upload = upload_rest_api.upload.Upload(project, 'foo/bar')
    (upload.tmp_project_directory / 'foo').mkdir(parents=True)
    shutil.copy('tests/data/test.txt',
                upload.tmp_project_directory / 'foo/bar')

    # Create metadata
    upload.store_files()

    # Check the metadata that was posted to Metax
    metadata = metax_files_api.last_request.json()[0]
    assert metadata["identifier"].startswith('urn:uuid:')
    assert metadata["file_name"] == "bar"
    assert metadata["file_format"] == "text/plain"
    assert metadata["byte_size"] == 31
    assert metadata["file_path"] == "/foo/bar"
    assert metadata["project_identifier"] == project
    assert "file_uploaded" in metadata
    assert "file_modified" in metadata
    assert "file_frozen" in metadata
    assert metadata['checksum']["algorithm"] == "MD5"
    assert metadata['checksum']["value"] == "150b62e4e7d58c70503bd5fc8a26463c"
    assert "checked" in metadata['checksum']
    assert metadata["file_storage"] == mock_config["STORAGE_ID"]


@pytest.mark.usefixtures('app')
def test_file_metadata_conflict(mock_config, requests_mock):
    """Test uploading a file that already has metadata in Metax."""
    # Create an incomplete upload with two text files in temporary
    # project directory
    project = 'test_project'
    upload = upload_rest_api.upload.Upload(project, 'path')
    (upload.tmp_project_directory / 'path').mkdir(parents=True)
    shutil.copy('tests/data/test.txt',
                upload.tmp_project_directory / 'path/file1')
    shutil.copy('tests/data/test.txt',
                upload.tmp_project_directory / 'path/file2')

    # Mock metax. The first text file does not have metadata in Metax,
    # but the second text file does have.
    metax_files_api = requests_mock.post('/rest/v2/files/', json={})
    requests_mock.get(
        '/rest/v2/files',
        json={
            'next': None,
            'results': [
                {
                    'file_path': '/path/file2',
                    'id': 2,
                    'identifier': '2',
                    'file_storage': {'identifier': 'foo'}
                }
            ]
        }
    )

    # Try to create metadata. Metadata creation should fail.
    with pytest.raises(upload_rest_api.jobs.utils.ClientError) as error:
        upload.store_files()
    assert error.value.message == ('Metadata could not be created because some'
                                   ' files already have metadata')
    assert error.value.files == ['path/file2']

    # Nothing should have been posted to Metax
    assert not metax_files_api.called

    # Temporary files should be removed
    tmp_dir = pathlib.Path(mock_config['UPLOAD_TMP_PATH'])
    assert not any(tmp_dir.iterdir())
