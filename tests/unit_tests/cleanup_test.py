"""Unit tests for the cleanup.py script."""
import os
import pathlib
import shutil
import time

from datetime import datetime, timezone, timedelta

import pytest
from flask_tus_io.resource import encode_tus_meta
from metax_access.metax import (DS_STATE_INITIALIZED,
                                DS_STATE_IN_DIGITAL_PRESERVATION)

from upload_rest_api.models.project import Project
import upload_rest_api.cleanup as clean
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.models.upload import UploadEntry


@pytest.mark.usefixtures('app')  # Creates test_project
def test_no_expired_files(mock_config):
    """Test that files that are not too old are not removed nor
    are the access times changed by the cleanup itself.
    """
    mock_config["CLEANUP_TIMELIM"] = 10

    # Copy test.txt file to project directory
    dirpath = os.path.join(mock_config["UPLOAD_PROJECTS_PATH"], "test_project")
    fpath = os.path.join(dirpath, "test.txt")
    shutil.copy("tests/data/test.txt", fpath)

    current_time = time.time()
    last_access = int(current_time-5)

    # Modify the file timestamp to make it 10s old
    os.utime(fpath, (last_access, last_access))

    # Clean all files older than 10s. 0 files should be cleaned.
    assert clean.clean_disk() == 0

    # File was not removed
    assert os.path.isfile(fpath)

    # Timestamp was not changed by the cleanup
    assert os.stat(fpath).st_atime == last_access
    assert os.stat(fpath).st_mtime == last_access


@pytest.mark.usefixtures('app')  # Creates test_project
def test_expired_files(test_mongo, mock_config, requests_mock):
    """Test that all the expired files are removed."""
    # Mock metax. Files do not have pending datasets.
    requests_mock.post('/rest/v2/files/datasets?keys=files', json={})
    delete_files_api = requests_mock.delete('/rest/v2/files', json={})

    mock_config["CLEANUP_TIMELIM"] = 10
    project_directory \
        = os.path.join(mock_config["UPLOAD_PROJECTS_PATH"], "test_project")

    # Create two files: one new file and one expired file
    fpath = os.path.join(project_directory, "new_file.txt")
    fpath_expired = os.path.join(project_directory, "expired_file.txt")
    shutil.copy("tests/data/test.txt", fpath)
    shutil.copy("tests/data/test.txt", fpath_expired)
    expired_access = int(time.time() - 100)
    os.utime(fpath_expired, (expired_access, expired_access))

    # Add the files to database
    files = test_mongo.upload.files
    files.insert_many([
        {"_id": fpath, "checksum": "foo", "identifier": "urn:uuid:1"},
        {"_id": fpath_expired, "checksum": "foo", "identifier": "urn:uuid:2"}
    ])

    # Clean all files older than 10s. One file should be cleaned.
    assert clean.clean_disk() == 1

    # The expired file should be removed from filesystem, but the new
    # file should still exist
    assert not os.path.isfile(fpath_expired)
    assert os.path.isfile(fpath)

    # The expired file should be removed from database
    assert files.count({"_id": fpath_expired}) == 0
    assert files.count({"_id": fpath}) == 1

    # The metadata of expired file should be deleted from Metax
    assert delete_files_api.called_once
    assert delete_files_api.request_history[0].json() == ["urn:uuid:2"]

    # Used quota should be updated. One copy of "test.txt" should be
    # left.
    assert Project.get(id='test_project').used_quota \
        == pathlib.Path('tests/data/test.txt').stat().st_size


@pytest.mark.usefixtures('app')  # Creates test_project
def test_cleaning_files_in_datasets(test_mongo, mock_config, requests_mock):
    """Test cleaning files that are included in datasets.

    Files that are inluded in pending dataset should not be cleaned.
    Files that are included in preserved dataset should be cleaned, but
    their metadata should not be removed from Metax (see TPASPKT-749).
    """
    mock_config["CLEANUP_TIMELIM"] = 10
    project_directory \
        = os.path.join(mock_config["UPLOAD_PROJECTS_PATH"], "test_project")

    # Create two expired files: one will be added to a pending dataset
    # and the one will be added to a preserved dataset
    fpath_pending = os.path.join(project_directory, "pending_file.txt")
    fpath_preserved = os.path.join(project_directory, "preserved_file.txt")
    shutil.copy("tests/data/test.txt", fpath_pending)
    shutil.copy("tests/data/test.txt", fpath_preserved)
    expired_access = int(time.time() - 100)
    os.utime(fpath_pending, (expired_access, expired_access))
    os.utime(fpath_preserved, (expired_access, expired_access))

    # Add the files to database
    files = test_mongo.upload.files
    files.insert_many([
        {"_id": fpath_pending, "checksum": "foo", "identifier": "urn:uuid:1"},
        {"_id": fpath_preserved, "checksum": "foo", "identifier": "urn:uuid:2"}
    ])

    # Mock metax. File "pending_file.txt" is added to dataset
    # "pending_dataset" and file "preserved_file.txt" is added to
    # dataset "preserved_dataset". The deletion code will first search
    # files that can be deleted. Before deleting the files it will
    # ensure that the files can be deleted. Therefore, the pending
    # datasets of deleted files will be checked twice.
    requests_mock.post(
        "/rest/v2/files/datasets?keys=files",
        additional_matcher=(
            lambda req: set(req.json()) == {'urn:uuid:1', 'urn:uuid:2'}
        ),
        json={'urn:uuid:1': ["pending_dataset"],
              'urn:uuid:2': ["preserved_dataset"]}
    )
    requests_mock.post(
        "/rest/datasets/list",
        additional_matcher=(
            lambda req: set(req.json()) == {"preserved_dataset",
                                            "pending_dataset"}
        ),
        json={
            "count": 2,
            "results": [
                {
                    "identifier": "pending_dataset",
                    "research_dataset": {"title": {"en": "Dataset"}},
                    "preservation_state":
                        DS_STATE_INITIALIZED
                },
                {
                    "identifier": "preserved_dataset",
                    "research_dataset": {"title": {"en": "Dataset"}},
                    "preservation_state":
                        DS_STATE_IN_DIGITAL_PRESERVATION
                }
            ]
        }
    )
    requests_mock.post(
        "/rest/v2/files/datasets?keys=files",
        additional_matcher=lambda req: req.json() == ['urn:uuid:2'],
        json={'urn:uuid:2': ["preserved_dataset"]}
    )
    requests_mock.post(
        "/rest/datasets/list",
        additional_matcher=lambda req: req.json() == ["preserved_dataset"],
        json={
            "count": 1,
            "results": [
                {
                    "identifier": "preserved_dataset",
                    "research_dataset": {"title": {"en": "Dataset"}},
                    "preservation_state":
                    DS_STATE_IN_DIGITAL_PRESERVATION
                }
            ]
        }
    )
    delete_files_api = requests_mock.delete('/rest/v2/files', json={})

    # Clean all files older than 10s.
    assert clean.clean_disk() == 1

    # "pending_file.txt" should not be removed, but "preserved_file.txt"
    # should be removed.
    assert os.path.isfile(fpath_pending)
    assert not os.path.isfile(fpath_preserved)
    assert files.count({"_id": fpath_pending}) == 1
    assert files.count({"_id": fpath_preserved}) == 0

    # The metadata of any file should not be removed from Metax
    assert not delete_files_api.called


@pytest.mark.usefixtures('app')  # Creates test_project
def test_all_files_expired(test_mongo, mock_config, requests_mock):
    """Test cleanup for expired project.

    Project directory should not be removed even if all files are
    expired.
    """
    # Mock metax. Files do not have pending datasets.
    requests_mock.post('/rest/v2/files/datasets?keys=files', json={})
    requests_mock.delete('/rest/v2/files', json={})

    mock_config["CLEANUP_TIMELIM"] = 10
    upload_path = pathlib.Path(mock_config["UPLOAD_PROJECTS_PATH"])
    project_path = upload_path / "test_project"

    # Create old testfile
    old_file = project_path / "test.txt"
    old_file.write_text('foo')
    os.utime(old_file, (0, 0))

    # Add files to mongo
    files = test_mongo.upload.files
    files.insert_one(
        {"_id": str(old_file), "checksum": "foo", "identifier": "urn:uuid:1"}
    )

    # Clean all files older than 10s. One file should be removed.
    assert clean.clean_disk() == 1

    # The old file should be removed, and project directory should be
    # empty
    assert not old_file.exists()
    assert project_path.exists()
    assert not any(project_path.iterdir())

    # File should have been removed
    assert files.count() == 0


def test_expired_tasks(test_mongo, mock_config):
    """Test that only expired tasks are removed."""
    mock_config["CLEANUP_TIMELIM"] = 1

    # Add tasks to mongo
    tasks = test_mongo.upload.tasks
    tasks.insert_one({"project_id": "project_1",
                      "timestamp": time.time(),
                      "status": 'pending'})
    tasks.insert_one({"project_id": "project_2",
                      "timestamp": time.time(),
                      "status": 'pending'})
    time.sleep(2)
    tasks.insert_one({"project_id": "project_3",
                      "timestamp": time.time(),
                      "status": 'pending'})
    tasks.insert_one({"project_id": "project_4",
                      "timestamp": time.time(),
                      "status": 'pending'})
    assert tasks.count() == 4

    # Clean all tasks older than 1s
    clean.clean_mongo()
    # Verify that latest two task left
    tasks_left = tasks.find()
    projects = []
    for task in tasks_left:
        projects.append(task['project_id'])
    projects.sort()
    assert len(projects) == 2
    assert projects[0] == "project_3"
    assert projects[1] == "project_4"
    time.sleep(2)
    clean.clean_mongo()
    assert tasks.count() == 0


def test_aborted_tus_uploads(app, test_mongo, test_client, test_auth):
    """
    Test that aborted tus uploads are cleaned correctly after their
    tus workspace directory is cleaned up
    """
    for i in range(0, 5):
        resp = test_client.post(
            "/v1/files_tus",
            headers={
                **{
                    "Tus-Resumable": "1.0.0",
                    "Upload-Length": "42",
                    "Upload-Metadata": encode_tus_meta({
                        "type": "file",
                        "project_id": "test_project",
                        "filename": f"test_{i}.txt",
                        "upload_path": f"test_{i}.txt"
                    })
                },
                **test_auth
            }
        )

        assert resp.status_code == 201

    # Delete the tus workspaces for 'test_1.txt' and 'test_2.txt' to make them
    # eligible for removal. In practice this is done by the
    # 'tus-clean-workspaces' script periodically.
    tus_spool_dir = pathlib.Path(app.config["TUS_API_SPOOL_PATH"])

    for path in tus_spool_dir.iterdir():
        name = path.name
        if name.startswith("test_1.txt") or name.startswith("test_2.txt"):
            shutil.rmtree(path)

    # Run the cleanup. 'test_1.txt' and 'test_2.txt' will be cleaned.
    assert clean.clean_other_uploads() == 0
    deleted_count = clean.clean_tus_uploads()
    assert deleted_count == 2
    assert test_mongo.upload.uploads.count() == 3

    # Pending uploads should be found in database, and their upload
    # paths should be locked
    lock_manager = ProjectLockManager()
    project_path \
        = pathlib.Path(app.config["UPLOAD_PROJECTS_PATH"]) / 'test_project'
    for name in ("test_0.txt", "test_3.txt", "test_4.txt"):
        upload_path = str(project_path / name)
        assert test_mongo.upload.uploads.find_one({
            'path': f"/{name}", "is_tus_upload": True
        })
        with pytest.raises(ValueError) as error:
            lock_manager.acquire('test_project', upload_path, timeout=0.1)
        assert str(error.value) == 'File lock could not be acquired'

    # Aborted uploads should not be found in database, and upload paths
    # should not be locked i.e. lock can be acquired
    for name in ("test_1.txt", "test_2.txt"):
        upload_path = str(project_path / name)
        assert not test_mongo.upload.uploads.find_one(
            {'path': f"/{name}"}
        )
        lock_manager.acquire('test_project', upload_path, timeout=0.1)

    # Nothing is cleaned on second run
    assert clean.clean_other_uploads() == 0
    assert clean.clean_tus_uploads() == 0

    # Release the file storage locks
    for i in range(0, 5):
        upload_path = str(project_path / f"test_{i}.txt")
        lock_manager.release('test_project', upload_path)


@pytest.mark.usefixtures("app")
def test_cleanup_only_tus_uploads():
    """
    Test that non-tus uploads are not cleaned
    """
    # Create a fake pending upload manually to fake an unlikely scenario in
    # which the entry was not deleted upon crashing.
    UploadEntry(
        id="upload1", path="/fake/path", project="test_project", size=1,
        type_="file"
    ).save()
    UploadEntry(
        id="upload2", path="/fake/path2", project="test_project", size=2,
        type_="file"
    ).save()
    UploadEntry(
        id="upload3", path="/fake/path3", project="test_project", size=3,
        type_="file", is_tus_upload=True
    ).save()

    # Only tus upload will be deleted
    assert clean.clean_tus_uploads() == 1
    assert clean.clean_other_uploads() == 0

    assert UploadEntry.objects.filter(path="/fake/path").count() == 1
    assert UploadEntry.objects.filter(path="/fake/path2").count() == 1
    assert UploadEntry.objects.filter(path="/fake/path3").count() == 0


@pytest.mark.usefixtures("app")
def test_cleanup_other_uploads(mock_config, monkeypatch):
    for i in range(0, 52):
        UploadEntry(
            id=f"upload{i}", path=f"/fake/path{i}", project="test_project",
            size=1, started_at=datetime.now(timezone.utc) - timedelta(hours=i)
        ).save()

    # Set the lock TTL to 1 hour
    mock_config["UPLOAD_LOCK_TTL"] = 60 * 60

    # Uploads older than 48 hours will be deleted
    assert clean.clean_tus_uploads() == 0
    assert clean.clean_other_uploads() == 3

    assert UploadEntry.objects.filter(path="/fake/path0").count() == 1
    assert UploadEntry.objects.filter(path="/fake/path49").count() == 0
