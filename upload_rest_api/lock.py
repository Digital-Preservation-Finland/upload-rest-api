"""Module for handling file storage locks"""

import time
from contextlib import contextmanager

from flask import g
from upload_rest_api.config import CONFIG
from upload_rest_api.database import get_redis_connection
from werkzeug.local import LocalProxy

LOCK_ACQUIRE_LUA = """
-- Simple Redis lock script where we try to acquire a lock for file system path
-- while ensuring no lock is active for the path or any of its sub-directories
local function starts_with(str, start)
   return str:sub(1, #start) == start
end

local project_lock_key = KEYS[1]
local path = ARGV[1]
local current_time = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local new_deadline = current_time + ttl

-- Iterate all locks active for this project
-- HGETALL return value is a [key, value, key, value, ...] list
local result = redis.call('HGETALL', project_lock_key)
for i = 1, #result, 2 do
    local locked_path = result[i]
    local lock_deadline = tonumber(result[i + 1])
    if current_time > lock_deadline then
        -- The lock we iterated has expired. Clean it up.
        redis.call('HDEL', project_lock_key, locked_path)
    end

    local lock_found =
        starts_with(locked_path, path) or starts_with(path, locked_path)
    if lock_found and current_time < lock_deadline then
        -- Lock already active for this directory or subdirectory
        return 0
    end
end

-- We made it this far, the lock is available. Acquire it.
redis.call('HSET', project_lock_key, path, new_deadline)
return 1
"""

# File locks will expire after 12 hours
DEFAULT_LOCK_TTL = 43200

# Each task will attempt to acquire lock for 3 seconds before giving up
DEFAULT_LOCK_TIMEOUT = 3


class LockAlreadyTaken(ValueError):
    """Exception raised when attempt to acquire a lock fails"""


class ProjectLockManager:
    """
    Class for managing project file locks.

    File locks are locks that prevent other filesystem operations or
    background jobs for being performed or launched for a directory/file.
    This includes the directory itself or other directories if they might
    conflict with each other.

    For example, suppose you're performing a background job  for "/spam/eggs"
    and the user tries to create a new file named "/spam/eggs/foo";
    this operation should be prevented as well since it involves a child of
    that directory.

    Some operations are instantenous, while others are background jobs that
    are enqueued and not instantly started. Each lock also has a long
    expiration period to ensure that no permanent deadlocks don't occur in
    case of crashes, and locks are eventually released.
    """
    def __init__(self):
        """Initialize FileLockManager instance."""
        self.redis = get_redis_connection()
        self.lua_acquire = self.redis.register_script(LOCK_ACQUIRE_LUA)

        self.upload_path = CONFIG["UPLOAD_PATH"]
        self.default_lock_ttl = CONFIG.get(
            "UPLOAD_LOCK_TTL", DEFAULT_LOCK_TTL
        )
        self.default_lock_timeout = CONFIG.get(
            "UPLOAD_LOCK_TIMEOUT", DEFAULT_LOCK_TIMEOUT
        )

    @contextmanager
    def lock(self, project, path, timeout=None, ttl=None):
        """
        Context manager to acquire and release a lock
        """
        if ttl is None:
            ttl = self.default_lock_ttl

        if timeout is None:
            timeout = self.default_lock_timeout

        self.acquire(project, path, timeout=timeout, ttl=ttl)
        try:
            yield
        finally:
            self.release(project, path)

    def acquire(self, project, path, timeout=None, ttl=None):
        """
        Try to acquire the lock for a path in the given project.

        :returns: True if lock was acquired
        :raises ValueError: If lock couldn't be acquired in the given time
        """
        if timeout is None:
            timeout = self.default_lock_timeout

        if ttl is None:
            ttl = self.default_lock_ttl

        path = str(path)
        deadline = time.time() + timeout

        if not path.startswith(self.upload_path):
            raise ValueError("Path to lock has to be an absolute project path")

        while time.time() < deadline:
            result = bool(
                self.lua_acquire(
                    keys=[f"upload-rest-api:locks:{project}"],
                    args=[path, time.time(), ttl],
                    client=self.redis
                )
            )

            if result:
                return True

            time.sleep(0.2)

        raise LockAlreadyTaken("File lock could not be acquired")

    def release(self, project, path):
        """
        Release the lock for a path in the given project
        """
        path = str(path)

        if not path.startswith(self.upload_path):
            raise ValueError(
                "Path to release has to be an absolute project path"
            )

        if not bool(self.redis.hdel(f"upload-rest-api:locks:{project}", path)):
            raise ValueError("Lock was already released")


def get_lock_manager():
    """Get lock manager"""
    if "lock_manager" not in g:
        g.lock_manager = ProjectLockManager()

    return g.lock_manager


lock_manager = LocalProxy(get_lock_manager)
