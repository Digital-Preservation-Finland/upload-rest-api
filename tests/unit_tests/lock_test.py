"""Tests for ``upload_rest_api.lock`` module."""
import pytest
import time

from upload_rest_api.lock import LockAlreadyTaken


def _upload_file(client, url, auth, fpath):
    """Send POST request to given URL with file fpath.

    :returns: HTTP response
    """
    with open(fpath, "rb") as test_file:
        response = client.post(
            url,
            input_stream=test_file,
            headers=auth
        )

    return response


def _test_lock(lock_manager, path):
    """
    Acquire a lock and release it immediately.
    """
    lock_manager.acquire("test_project", path, ttl=5, timeout=0.1)
    lock_manager.release("test_project", path)

    return True


def test_lock_block_related(lock_manager, upload_tmpdir):
    """
    Test that acquiring a lock will block related paths from being used
    """
    project_dir = upload_tmpdir / "projects" / "test_project"

    lock_manager.acquire("test_project", project_dir / "foo" / "bar", ttl=5)

    # Lock is acquired, we cannot acquire related paths
    with pytest.raises(LockAlreadyTaken):
        # Child of the locked path
        _test_lock(lock_manager, project_dir / "foo" / "bar" / "spam")

    with pytest.raises(LockAlreadyTaken):
        # Parent of the locked path
        _test_lock(lock_manager, project_dir / "foo")

    # We *can* acquire a lock from an unrelated path
    _test_lock(lock_manager, project_dir / "foo2")

    lock_manager.release("test_project", project_dir / "foo" / "bar")


def test_lock_timeout(lock_manager, upload_tmpdir):
    """
    Test that locks will expire even if they're not released
    """
    project_dir = upload_tmpdir / "projects"

    lock_manager.acquire("test_project", project_dir / "foo", ttl=0.5)

    # Immediately trying to acquire it again will fail
    with pytest.raises(LockAlreadyTaken):
        _test_lock(lock_manager, project_dir / "foo")

    # Waiting 0.5 seconds will make the lock expire
    time.sleep(0.5)

    _test_lock(lock_manager, project_dir / "foo")


def test_lock_response(app, test_auth, test_client, mock_redis):
    """
    Test performing a HTTP request that acquires a lock while a lock
    is already acquired.
    """
    _upload_file(
        test_client, "/v1/files/test_project/foo", test_auth,
        "tests/data/test.txt"
    )

    # Start a metadata generation background job; this will acquire a lock
    response = test_client.post(
        "/v1/metadata/test_project/foo", headers=test_auth
    )

    assert response.status_code == 202

    # Start another job. This will be blocked.
    response = test_client.post(
        "/v1/metadata/test_project/foo", headers=test_auth
    )

    assert response.status_code == 409  # Conflict
    assert response.json["error"] \
        == "The file/directory is currently locked by another task"

    # Flush existing locks
    mock_redis.flushall()