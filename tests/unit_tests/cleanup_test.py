"""Unit tests for the cleanup.py script."""
import os
import time
import shutil

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


def test_expired_files(mock_mongo, mock_config):
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
    checksums = mock_mongo.upload.checksums
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


def test_expired_tasks(mock_mongo, requests_mock, mock_config):
    """Test that only expired tasks are removed."""
    # Mock Metax HTTP responses
    requests_mock.get('https://metax.fd-test.csc.fi/rest/v2/files',
                      json={'next': None, 'results': []})

    mock_config["CLEANUP_TIMELIM"] = 1

    # Add tasks to mongo
    tasks = mock_mongo.upload.tasks
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
