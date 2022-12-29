from upload_rest_api.models.file_entry import FileEntry


def test_correct_document_structure(files_col):
    """
    Test that saved FileEntry has the same structure as the pre-MongoEngine
    implementation
    """
    file = FileEntry(
        path="/fake/path",
        checksum="6d48b69215369ecd27c1add71746989c",
        identifier="urn:uuid:foo_bar"
    )
    file.save()

    docs = list(files_col.find())

    assert len(docs) == 1
    assert docs[0] == {
        "_id": "/fake/path",
        "checksum": "6d48b69215369ecd27c1add71746989c",
        "identifier": "urn:uuid:foo_bar"
    }
