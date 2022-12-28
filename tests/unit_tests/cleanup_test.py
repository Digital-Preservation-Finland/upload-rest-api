"""Unit tests for the cleanup.py script."""
import os
import pathlib
import shutil
import time

from datetime import datetime, timezone, timedelta

import pytest
from flask_tus_io.resource import encode_tus_meta

import upload_rest_api.cleanup as clean
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.models.upload import UploadEntry


def test_no_expired_files(mock_config):
    """Test that files that are not too old are not removed nor
    are the access times changed by the cleanup itself.
    """
    mock_config["CLEANUP_TIMELIM"] = 10

    # Make test directory with test.txt file
    dirpath = os.path.join(mock_config["UPLOAD_PROJECTS_PATH"], "test")
    fpath = os.path.join(dirpath, "test.txt")
    os.makedirs(dirpath)
    shutil.copy("tests/data/test.txt", fpath)

    current_time = time.time()
    last_access = int(current_time-5)

    # Modify the file timestamp to make it 10s old
    os.utime(fpath, (last_access, last_access))

    # Clean all files older than 10s
    clean.clean_disk(metax=False)

    # File was not removed
    assert os.path.isfile(fpath)

    # Timestamp was not changed by the cleanup
    assert os.stat(fpath).st_atime == last_access
    assert os.stat(fpath).st_mtime == last_access


def test_expired_files(test_mongo, mock_config):
    """Test that all the expired files and empty directories are
    removed.
    """
    mock_config["CLEANUP_TIMELIM"] = 10
    upload_path = mock_config["UPLOAD_PROJECTS_PATH"]

    # Make test directories with test.txt files
    fpath = os.path.join(upload_path, "test/test.txt")
    fpath_expired = os.path.join(upload_path, "test/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test/test/"))
    shutil.copy("tests/data/test.txt", fpath)
    shutil.copy("tests/data/test.txt", fpath_expired)

    # Add files to mongo
    files = test_mongo.upload.files
    files.insert_many([
        {"_id": fpath, "checksum": "foo", "identifier": "urn:uuid:1"},
        {"_id": fpath_expired, "checksum": "foo", "identifier": "urn:uuid:2"}
    ])

    current_time = time.time()
    expired_access = int(current_time - 100)
    os.utime(fpath_expired, (expired_access, expired_access))

    # Clean all files older than 10s
    clean.clean_disk(metax=False)

    # upload_path/test/test/test.txt and its directory should be removed
    assert not os.path.isdir(os.path.join(upload_path, "test/test/"))

    # fpath_expired file should be removed
    assert files.count({"_id": fpath_expired}) == 0
    assert files.count({"_id": fpath}) == 1

    # upload_path/test/test.txt should not be removed
    assert os.path.isfile(fpath)


def test_all_files_expired(test_mongo, mock_config):
    """Test cleanup for expired project.

    Project directory should not be removed even if all files are
    expired.
    """
    mock_config["CLEANUP_TIMELIM"] = 10
    upload_path = pathlib.Path(mock_config["UPLOAD_PROJECTS_PATH"])
    project_path = upload_path / "test_project"
    project_path.mkdir()

    # Create old testfile
    old_file = project_path / "test.txt"
    old_file.write_text('foo')
    os.utime(old_file, (0, 0))

    # Add files to mongo
    files = test_mongo.upload.files
    files.insert_one(
        {"_id": str(old_file), "checksum": "foo", "identifier": "urn:uuid:1"}
    )

    # Clean all files older than 10s
    clean.clean_disk(metax=False)

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


def test_clean_disk(mock_config, requests_mock, test_mongo):
    """Test cleaning disk."""
    project = 'foo'
    project_path = pathlib.Path(mock_config['UPLOAD_PROJECTS_PATH']) / project

    # Mock metax
    requests_mock.get('https://metax.localdomain/rest/v2/files',
                      json={'next': None, 'results': []})

    # Create an old test file in project directory
    project_path.mkdir()
    testfile = project_path / 'testfile1'
    testfile.write_text('foo')
    os.utime(testfile, (0, 0))
    assert testfile.is_file()

    # Add files to mongo
    files = test_mongo.upload.files
    files.insert_one(
        {"_id": str(testfile), "identifier": "urn:uuid:1", "checksum": "foo"}
    )

    # Clean disk and check that the old file has been removed
    clean.clean_disk(metax=True)
    assert not testfile.is_file()

    # Check that file have been removed from mongo
    assert files.count() == 0


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
        id="upload1", path="/fake/path", project="test_project", size=1
    ).save()
    UploadEntry(
        id="upload2", path="/fake/path2", project="test_project", size=2
    ).save()
    UploadEntry(
        id="upload3", path="/fake/path3", project="test_project", size=3,
        is_tus_upload=True
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
