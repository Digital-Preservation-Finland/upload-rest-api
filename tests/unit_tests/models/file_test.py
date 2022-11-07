from upload_rest_api.models.file import File


def test_correct_document_structure(files_col):
    """
    Test that saved File has the same structure as the pre-MongoEngine
    implementation
    """
    file = File(
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

    assert File.objects.count() == 20100

    # Delete all but the last 3 files entries using `delete`
    paths_to_delete = [f"/path/{i}" for i in range(0, 20097)]
    assert File.objects.bulk_delete_by_paths(paths_to_delete) == 20097

    # 3 files are left
    assert File.objects.count() == 3
    assert list(test_mongo.upload.files.find()) == [
        {"_id": "/path/20097", "checksum": "foobar", "identifier": '1'},
        {"_id": "/path/20098", "checksum": "foobar", "identifier": '1'},
        {"_id": "/path/20099", "checksum": "foobar", "identifier": '1'}
    ]


def test_get_path_checksum_dict(test_mongo):
    """Test getting files as dict of file paths and checksums."""
    files = [
        {"_id": "path_1",
         "checksum": "checksum_1",
         "identifier": "pid:urn:1"},
        {"_id": "path_2",
         "checksum": "checksum_2",
         "identifier": "pid:urn:2"}
    ]
    test_mongo.upload.files.insert_many(files)

    correct_result = {"path_1": "checksum_1", "path_2": "checksum_2"}
    assert File.get_path_checksum_dict() == correct_result
