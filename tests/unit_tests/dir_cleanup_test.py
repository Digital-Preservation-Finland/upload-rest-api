"""Unit tests for the dir_cleanup.py script"""
import os
import time
import shutil
from runpy import run_path

import upload_rest_api.dir_cleanup as clean


def _parse_conf(fpath):
    """Parse conf from include/etc/upload_rest_api.conf if fpath
    doesn't exist.
    """
    if os.path.isfile(fpath):
        return run_path(fpath)

    return run_path("include/etc/upload_rest_api.conf")


def test_no_expired_files(app, monkeypatch):
    """Test that files that are not too old are not removed nor
    are the access times changed by the cleanup itself.
    """
    monkeypatch.setattr(clean, "parse_conf", _parse_conf)

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
    clean.cleanup("project", upload_path, upload_path, 100, metax=False)

    # File was not removed
    assert os.path.isfile(fpath)

    # Timestamp was not changed by the cleanup
    assert os.stat(fpath).st_atime == last_access
    assert os.stat(fpath).st_mtime == last_access


def test_expired_files(app, monkeypatch):
    """Test that all the expired files and empty directories are removed.
    """
    monkeypatch.setattr(clean, "parse_conf", _parse_conf)

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
    clean.cleanup("project", upload_path, upload_path, 10, metax=False)

    # upload_path/test/test/test.txt and its directory should be removed
    assert not os.path.isdir(os.path.join(upload_path, "test/test/"))

    # upload_path/test/test.txt should not be removed
    assert os.path.isfile(fpath)
