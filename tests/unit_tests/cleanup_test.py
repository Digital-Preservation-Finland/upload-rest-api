"""Unit tests for the cleanup.py script"""
from __future__ import unicode_literals

import os
import time
import shutil
from runpy import run_path

import pytest
import mongomock

import upload_rest_api.cleanup as clean


@pytest.fixture(autouse=True)
def mock_mongo(monkeypatch):
    """Patch pymongo.MongoClient() with mock client"""
    mongoclient = mongomock.MongoClient()
    monkeypatch.setattr('pymongo.MongoClient', lambda *args: mongoclient)
    return mongoclient


def test_no_expired_files(app, monkeypatch):
    """Test that files that are not too old are not removed nor
    are the access times changed by the cleanup itself.
    """
    def _parse_conf(fpath):
        """Parse conf from include/etc/upload_rest_api.conf if fpath
        doesn't exist.
        """
        conf = run_path("include/etc/upload_rest_api.conf")
        conf["CLEANUP_TIMELIM"] = 10
        conf["UPLOAD_PATH"] = app.config.get("UPLOAD_PATH")

        return conf

    monkeypatch.setattr(clean, "parse_conf", _parse_conf)

    upload_path = app.config.get("UPLOAD_PATH")

    # Make test directory with test.txt file
    dirpath = os.path.join(upload_path, "test")
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


def test_expired_files(app, mock_mongo, monkeypatch):
    """Test that all the expired files and empty directories are removed.
    """
    def _parse_conf(fpath):
        """Parse conf from include/etc/upload_rest_api.conf if fpath
        doesn't exist.
        """
        conf = run_path("include/etc/upload_rest_api.conf")
        conf["CLEANUP_TIMELIM"] = 10
        conf["UPLOAD_PATH"] = app.config.get("UPLOAD_PATH")

        return conf

    monkeypatch.setattr(clean, "parse_conf", _parse_conf)

    upload_path = app.config.get("UPLOAD_PATH")

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
    assert checksums.find({"_id": fpath_expired}).count() == 0
    assert checksums.find({"_id": fpath}).count() == 1

    # upload_path/test/test.txt should not be removed
    assert os.path.isfile(fpath)


def test_ida_cleanup(app):
    """Test that all the expired files are removed from ida_files.
    """
    upload_path = app.config.get("UPLOAD_PATH")

    # Create the files to be cleaned
    fpath = os.path.join(upload_path, "identifier1")
    fpath_expired = os.path.join(upload_path, "identifier2")

    shutil.copy("tests/data/test.txt", fpath)
    shutil.copy("tests/data/test.txt", fpath_expired)

    current_time = time.time()
    expired_access = int(current_time - (60*60*24*14 + 1))
    os.utime(fpath_expired, (expired_access, expired_access))

    # Clean all files older than two weeks
    clean.clean_ida(ida_files_path=upload_path)

    # upload_path/identifier1 should not be removed
    assert os.path.isfile(fpath)

    # upload_path/identifier2 should be removed
    assert not os.path.isfile(fpath_expired)
