import pytest

from upload_rest_api.checksum import get_file_checksum


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
