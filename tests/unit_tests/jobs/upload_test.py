"""Tests for upload jobs."""
import pytest
from upload_rest_api.models.resource import Directory
from upload_rest_api.models.task import Task
from upload_rest_api.models.upload import (Upload,
                                           UploadError,
                                           UploadConflictError)
from upload_rest_api.jobs.upload import store_files


@pytest.mark.parametrize(
    'exception,errors',
    [
        (
            UploadError('Invalid archive'),
            [{'message': 'Invalid archive', 'files': None}]
        ),
        (
            UploadConflictError('file1 exists', ['file1']),
            [{'message': 'file1 exists', 'files': ['file1']}]
        )
    ]
)
@pytest.mark.usefixtures('app')  # Create test_project
def test_upload_conflict_error(mocker, exception, errors):
    """Test exception handling of store_files job.

    Creates an archive upload that fails during archive extraction.
    Checks that correct error message is saved in task database.

    :param exception: Exception that occurs when archive is extracted
    :param exception: Error saved in task database
    """
    # Create an upload. Mock Upload class to fail during archive
    # extraction.
    upload = Upload.create(Directory('test_project', '/test'), 1)
    mocker.patch.object(Upload, '_extract_archive', side_effect=exception)

    # Run store_files job for upload
    task = Task.create(project_id='test_project', message="processing")
    store_files(upload.id, verify_source=False, task_id=task.id)

    # Check that correct error is found in task database
    task = task.get(task.id)
    assert task.status.value == 'error'
    assert task.message == 'Task failed'
    assert task.errors == errors
