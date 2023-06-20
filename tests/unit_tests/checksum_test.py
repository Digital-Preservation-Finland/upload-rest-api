import pytest

from upload_rest_api.checksum import (calculate_incr_checksum,
                                      get_file_checksum,
                                      REHASH_SUPPORTED)


@pytest.mark.parametrize(
    "algorithm,expected_checksum",
    [
        ("md5", "150b62e4e7d58c70503bd5fc8a26463c"),
        ("sha1", "db69c10bd3151e701d147051c8ee0171183d74b9"),
        (
            "sha256",
            "fa9b19e73084b8c459fd0c4ddc521c252b93ae20eb6068d342495fa3eb209609"
        )
    ]
)
def test_get_file_checksum(algorithm, expected_checksum):
    checksum = get_file_checksum(
        algorithm=algorithm,
        path="tests/data/test.txt"
    )

    assert checksum == expected_checksum


@pytest.mark.parametrize(
    "algorithm,expected_checksum",
    [
        ("md5", "150b62e4e7d58c70503bd5fc8a26463c"),
        ("sha1", "db69c10bd3151e701d147051c8ee0171183d74b9"),
        (
            "sha256",
            "fa9b19e73084b8c459fd0c4ddc521c252b93ae20eb6068d342495fa3eb209609"
        )
    ]
)
def test_calculate_incr_checksum(
        algorithm, expected_checksum, tmpdir, mock_redis):
    """
    Test calculating a file checksum incrementally and ensure the correct
    checksum is calculated at the end
    """

    file_chunks = [
        b"test ",
        b"file ",
        b"for ",
        b"REST ",
        b"file ",
        b"upload\n"
    ]

    temp_file = tmpdir / "upload.txt"

    for chunk in file_chunks:
        with temp_file.open("ab") as file_:
            file_.write(chunk)

        if REHASH_SUPPORTED:
            calculate_incr_checksum(
                algorithm=algorithm,
                path=temp_file
            )

    if REHASH_SUPPORTED:
        # Ensure the Redis checkpoint exists
        assert mock_redis.exists(
            f"upload-rest-api:checksum:{algorithm}:{temp_file}"
        )

        checksum = calculate_incr_checksum(
            algorithm=algorithm, path=temp_file, finalize=True
        )
    else:
        checksum = get_file_checksum(
            algorithm=algorithm, path=temp_file
        )

    assert checksum == expected_checksum

    # Ensure the Redis checkpoint was removed after finalization
    assert not mock_redis.exists(
        f"upload-rest-api:checksum:{algorithm}:{temp_file}"
    )


def test_calculate_incr_checksum_unrecognized():
    """
    Ensure that unknown checksum algorithm raises the expected error
    """
    if not REHASH_SUPPORTED:
        # We can't calculate incremental checksums without rehash.
        return

    with pytest.raises(ValueError) as exc:
        calculate_incr_checksum(
            algorithm="blake3",
            path="tests/data/test.txt"
        )

    assert str(exc.value) == "Hash function 'blake3' not recognized"
