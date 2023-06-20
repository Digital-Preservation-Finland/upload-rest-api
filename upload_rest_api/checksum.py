"""Module for calculating checksums for files"""
import base64
import hashlib
import json
import pickle

import ssl
import rehash

from upload_rest_api.redis import get_redis_connection

HASH_FUNCTION_ALIASES = {
    "md5": "md5",
    "sha1": "sha1",
    "sha2": "sha256",
    "sha256": "sha256"
}

# 4 hours, same as the workspace TTL for flask-tus-io
CHECKSUM_CHECKPOINT_TTL = 4 * 60 * 60

# rehash does not support OpenSSL 3 or newer
REHASH_SUPPORTED = ssl.OPENSSL_VERSION_INFO[0] < 3


def calculate_incr_checksum(algorithm, path, finalize=False):
    """
    Calculate the file checksum incrementally.

    The checksum is calculated until the current end of the file and then
    saved into a checkpoint. This means that the function can be called again
    later when additional data is written for a file.

    :param str algorithm: Cryptographic hash algorithm used to calculate
                          the checksum
    :param path:          Path to the file
    :param bool finalize: Finalize the checksum calculation and delete the
                          checkpoint. Default is False.

    :raises ValueError: If algorithm is not recognized

    :returns: Cryptographic hash of the file based on its current content
    """
    redis_key = f"upload-rest-api:checksum:{algorithm}:{str(path)}"
    redis = get_redis_connection()

    # Try retrieving the current checkpoint if it exists
    checkpoint = redis.get(redis_key)

    if checkpoint:
        data = json.loads(checkpoint)
        offset = data["offset"]
        hash_obj = pickle.loads(base64.b64decode(data["hash_obj"]))
    else:
        offset = 0
        try:
            hash_func = getattr(
                rehash, HASH_FUNCTION_ALIASES[algorithm.lower()]
            )
        except KeyError as exc:
            raise ValueError(
                f"Hash function '{algorithm}' not recognized"
            ) from exc

        hash_obj = hash_func()

    # Read the file until the end
    with path.open("rb") as file_:
        # Continue from where we left off
        file_.seek(offset)

        # Exhaust the file
        while True:
            chunk = file_.read(1024 * 1024)

            if not chunk:
                # File exhausted for now
                break

            hash_obj.update(chunk)

        offset = file_.tell()

    if finalize:
        # Delete the Redis checkpoint
        redis.delete(redis_key)
    else:
        checkpoint = {
            "offset": offset,
            "hash_obj": base64.b64encode(
                pickle.dumps(hash_obj)
            ).decode("utf-8")
        }
        redis.set(
            redis_key, json.dumps(checkpoint), ex=CHECKSUM_CHECKPOINT_TTL
        )

    return hash_obj.hexdigest()


def get_file_checksum(algorithm, path):
    """
    Calculate the file checksum using a given algorithm for a file.

    :param str algorithm: Cryptographic hash algorithm to use to calculate
                          the checksum
    :param path: Path to the file

    :raises ValueError: If algorithm is not recognized

    :returns: Checksum as a hex string
    """
    try:
        hash_func = getattr(hashlib, HASH_FUNCTION_ALIASES[algorithm.lower()])
        hash_obj = hash_func()
    except KeyError as exc:
        raise ValueError(
            f"Hash function '{algorithm}' not recognized"
        ) from exc

    with open(path, "rb") as file_:
        # Read the file in 1 MB chunks
        for chunk in iter(lambda: file_.read(1024 * 1024), b""):
            hash_obj.update(chunk)

    return hash_obj.hexdigest()
