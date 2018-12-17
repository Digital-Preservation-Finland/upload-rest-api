"""Unit tests for the dir_cleanup.py script"""
import os
import time
import shutil

from upload_rest_api.dir_cleanup import cleanup


def test_no_expired_files(app):
    """Test that files that are not too old are not removed nor
    are the access times changed by the cleanup itself.
    """
    upload_path = app.config.get("UPLOAD_PATH")

    # Make test directory with test.txt file
    dirpath = os.path.join(upload_path, "test")
    fpath = os.path.join(dirpath, "test.txt")
    os.makedirs(dirpath)
    shutil.copy("tests/data/test.txt", fpath)

    current_time = time.time()
    last_access = int(current_time-10)

    # Modify the file timestamp to make it 10s old
    os.utime(fpath, (last_access, last_access))

    # Clean all files older than 100s
    cleanup(upload_path, 100)

    # File was not removed
    assert os.path.isfile(fpath)

    # Timestamp was not changed by the cleanup
    assert os.stat(fpath).st_atime == last_access
    assert os.stat(fpath).st_mtime == last_access


def test_expired_files(app):
    """Test that all the expired files and empty directories are removed.
    """
    upload_path = app.config.get("UPLOAD_PATH")

    # Make test directories with test.txt files
    fpath = os.path.join(upload_path, "test/test.txt")
    fpath_expired = os.path.join(upload_path, "test/test/test.txt")

    os.makedirs(os.path.join(upload_path, "test/test/"))
    shutil.copy("tests/data/test.txt", fpath)
    shutil.copy("tests/data/test.txt", fpath_expired)

    current_time = time.time()
    expired_access = int(current_time - 100)
    os.utime(fpath_expired, (expired_access, expired_access))

    # Clean all files older than 10s
    cleanup(upload_path, 10)

    # upload_path/test/test/test.txt and its directory should be removed
    assert not os.path.isdir(os.path.join(upload_path, "test/test/"))

    # upload_path/test/test.txt should not be removed
    assert os.path.isfile(fpath)
