"""Module for accessing user database."""
from redis import Redis

from upload_rest_api.config import CONFIG

# Password vars
PASSWD_LEN = 20
SALT_LEN = 20

# Hashing vars
ITERATIONS = 200000
HASH_ALG = "sha512"


def get_redis_connection():
    """Get Redis connection."""
    password = CONFIG.get("REDIS_PASSWORD", None)
    redis = Redis(
        host=CONFIG["REDIS_HOST"],
        port=CONFIG["REDIS_PORT"],
        db=CONFIG["REDIS_DB"],
        password=password if password else None
    )

    return redis
