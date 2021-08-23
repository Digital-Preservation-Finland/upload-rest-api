"""Unit tests for the cleanup.py script."""
import os
import pathlib
import shutil
import time

import upload_rest_api.cleanup as clean


def test_no_expired_files(mock_config):
    """Test that files that are not too old are not removed nor
    are the access times changed by the cleanup itself.
    """
    mock_config["CLEANUP_TIMELIM"] = 10

    # Make test directory with test.txt file
    dirpath = os.path.join(mock_config["UPLOAD_PATH"], "test")
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
    upload_path = mock_config["UPLOAD_PATH"]

    # Make test directories with test.txt files
    fpath = os.path.join(upload_path, "test/test.txt")
    fpath_expired = os.path.join(upload_path, "test/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test/test/"))
    shutil.copy("tests/data/test.txt", fpath)
    shutil.copy("tests/data/test.txt", fpath_expired)

    # Add checksums to mongo
    checksums = test_mongo.upload.checksums
    checksums.insert_many([
        {"_id": fpath, "checksum": "foo"},
        {"_id": fpath_expired, "checksum": "foo"}
    ])

    current_time = time.time()
    expired_access = int(current_time - 100)
    os.utime(fpath_expired, (expired_access, expired_access))

    # Clean all files older than 10s
    clean.clean_disk(metax=False)

    # upload_path/test/test/test.txt and its directory should be removed
    assert not os.path.isdir(os.path.join(upload_path, "test/test/"))

    # fpath_expired checksum should be removed
    assert checksums.count({"_id": fpath_expired}) == 0
    assert checksums.count({"_id": fpath}) == 1

    # upload_path/test/test.txt should not be removed
    assert os.path.isfile(fpath)


def test_all_files_expired(test_mongo, mock_config):
    """Test cleanup for expired project.

    Project directory should not be removed even if all files are
    expired.
    """
    mock_config["CLEANUP_TIMELIM"] = 10
    upload_path = pathlib.Path(mock_config["UPLOAD_PATH"])
    project_path = upload_path / "test_project"
    project_path.mkdir()

    # Create old testfile
    old_file = project_path / "test.txt"
    old_file.write_text('foo')
    os.utime(old_file, (0, 0))

    # Add checksums to mongo
    checksums = test_mongo.upload.checksums
    checksums.insert_many([
        {"_id": str(old_file), "checksum": "foo"},
    ])

    # Clean all files older than 10s
    clean.clean_disk(metax=False)

    # The old file should be removed, and project directory should be
    # empty
    assert not old_file.exists()
    assert project_path.exists()
    assert not any(project_path.iterdir())


def test_expired_tasks(test_mongo, requests_mock, mock_config):
    """Test that only expired tasks are removed."""
    # Mock Metax HTTP responses
    requests_mock.get('https://metax.fd-test.csc.fi/rest/v2/files',
                      json={'next': None, 'results': []})

    mock_config["CLEANUP_TIMELIM"] = 1

    # Add tasks to mongo
    tasks = test_mongo.upload.tasks
    tasks.insert_one({"project": "project_1",
                      "timestamp": time.time(),
                      "status": 'pending'})
    tasks.insert_one({"project": "project_2",
                      "timestamp": time.time(),
                      "status": 'pending'})
    time.sleep(2)
    tasks.insert_one({"project": "project_3",
                      "timestamp": time.time(),
                      "status": 'pending'})
    tasks.insert_one({"project": "project_4",
                      "timestamp": time.time(),
                      "status": 'pending'})
    assert tasks.count() == 4

    # Clean all tasks older than 1s
    clean.clean_mongo()
    # Verify that latest two task left
    tasks_left = tasks.find()
    projects = []
    for task in tasks_left:
        projects.append(task['project'])
    projects.sort()
    assert len(projects) == 2
    assert projects[0] == "project_3"
    assert projects[1] == "project_4"
    time.sleep(2)
    clean.clean_mongo()
    assert tasks.count() == 0


def test_clean_project(mock_config, requests_mock):
    """Test cleaning project."""
    project = 'foo'
    project_path = pathlib.Path(mock_config['UPLOAD_PATH']) / project

    # Mock metax
    requests_mock.get('https://metax.fd-test.csc.fi/rest/v2/files',
                      json={'next': None, 'results': []})

    # Create a old test file in project directory
    project_path.mkdir()
    testfile = project_path / 'testfile1'
    testfile.write_text('foo')
    os.utime(testfile, (0, 0))
    assert testfile.is_file()

    # Clean project and check that the old file has been removed
    clean.clean_project(project, project_path, metax=True)
    assert not testfile.is_file()
