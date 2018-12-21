"""Unit tests for metadata generation"""
import upload_rest_api.gen_metadata as md


def test_md5():
    """Test that md5_digest function returns the correct digest"""
    digest1 = "150b62e4e7d58c70503bd5fc8a26463c"
    digest2 = "40c6cadaffe26738f84732d0fdd09ce4"

    assert md.md5_digest("tests/data/test.txt") == digest1
    assert md.md5_digest("tests/data/test.zip") == digest2


def test_mimetype():
    """Test that _get_mimetype() returns correct MIME types"""
    assert md._get_mimetype("tests/data/test.txt") == "text/plain"
    assert md._get_mimetype("tests/data/test.zip") == "application/zip"


def test_gen_metadata():
    """Test that _generate_metadata() produces the correct metadata"""
    metadata = md._generate_metadata(
        "tests/data/test.txt",
        "tests", "data",  
        "test_project", "pid:uuid:storage_id"
    )

    assert len(metadata["identifier"]) == 45
    assert metadata["file_name"] == "test.txt"
    assert metadata["file_format"] == "text/plain"
    assert metadata["file_path"] == "/test_project/test.txt"
    assert metadata["project_identifier"] == "test_project"
    assert "file_uploaded" in metadata 
    assert "file_frozen" in metadata 
    
    checksum = metadata["checksum"]
    assert checksum["algorithm"] == "md5"
    assert checksum["value"] == "150b62e4e7d58c70503bd5fc8a26463c"
    assert "checked" in checksum
    
    assert metadata["file_storage"] == 2
