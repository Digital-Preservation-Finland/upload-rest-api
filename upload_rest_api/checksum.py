"""Module for calculating checksums for files"""
import hashlib
from typing import Iterable


HASH_FUNCTION_ALIASES = {
    "md5": "md5",
    "sha1": "sha1",
    "sha2": "sha256",
    "sha256": "sha256"
}


def get_file_checksums(algorithms: Iterable[str], path: str) -> dict:
    """
    Calculate the file checksum using given algorithms for a file

    :param algorithms: Cryptographic hash algorithms used to calculate
                       the checksums
    :param path: Path to the file

    :returns: Checksums as a {algorithm: checksum} dict
    """
    algorithms = list(algorithms)

    hash_objs = []

    for algorithm in algorithms:
        try:
            hash_func = getattr(
                hashlib,
                HASH_FUNCTION_ALIASES[algorithm.lower()]
            )
            hash_obj = hash_func()
            hash_objs.append(hash_obj)
        except KeyError as exc:
            raise ValueError(
                f"Hash function '{algorithm}' not recognized"
            ) from exc

    with open(path, "rb") as file_:
        # Read the file in 1 MB chunks
        for chunk in iter(lambda: file_.read(1024 * 1024), b""):
            for hash_obj in hash_objs:
                hash_obj.update(chunk)

    return {
        algorithm: hash_obj.hexdigest()
        for algorithm, hash_obj in zip(algorithms, hash_objs)
    }


def get_file_checksum(algorithm, path):
    """
    Calculate the file checksum using a given algorithm for a file.

    :param str algorithm: Cryptographic hash algorithm to use to calculate
                          the checksum
    :param path: Path to the file

    :raises ValueError: If algorithm is not recognized

    :returns: Checksum as a hex string
    """
    return get_file_checksums([algorithm], path)[algorithm]
