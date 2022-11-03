from upload_rest_api.models.file import FileEntry


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


def test_files_delete_chunks(test_mongo):
    """Test deleting a large amount of files.

    The deletion queries are split into chunks internally to prevent
    exceeding MongoDB's query size limit.
    """
    # 20,100 files will be added
    for i in range(0, 201):
        test_mongo.upload.files.insert([
            {"_id": f"/path/{(i*100)+j}",
             "checksum": "foobar",
             "identifier": '1'}
            for j in range(0, 100)
        ])

    assert FileEntry.objects.count() == 20100

    # Delete all but the last 3 files entries using `delete`
    paths_to_delete = [f"/path/{i}" for i in range(0, 20097)]
    assert FileEntry.objects.bulk_delete_by_paths(paths_to_delete) == 20097

    # 3 files are left
    assert FileEntry.objects.count() == 3
    assert list(test_mongo.upload.files.find()) == [
        {"_id": "/path/20097", "checksum": "foobar", "identifier": '1'},
        {"_id": "/path/20098", "checksum": "foobar", "identifier": '1'},
        {"_id": "/path/20099", "checksum": "foobar", "identifier": '1'}
    ]
