"""Integration tests using test-metax.

URL endpoints that send requests to Metax are tested. Tests make sure
that the metadata is correctly posted and deleted.

These tests require the METAX_PASSWORD variable to be defined in
/etc/upload_rest_api.conf. If password is not found in configuration
file, the test will prompt password from user.
"""
import getpass
import os
from runpy import run_path

from metax_access import Metax
import pymongo
import pytest
import requests.exceptions

import upload_rest_api.cleanup as clean
import upload_rest_api.database as db
from upload_rest_api.gen_metadata import MetaxClient

URL = "https://metax.fd-test.csc.fi"
USER = "tpas"

if os.path.isfile("/etc/upload_rest_api.conf"):
    PASSWORD = run_path("/etc/upload_rest_api.conf")["METAX_PASSWORD"]
else:
    PASSWORD = getpass.getpass(
        prompt="https://metax.fd-test.csc.fi password for user tpas: "
    )


def _upload_file(client, url, auth, fpath):
    """Send POST request to given URL with file fpath.

    :returns: HTTP response
    """
    with open(fpath, "rb") as test_file:
        response = client.post(
            url,
            input_stream=test_file,
            headers=auth
        )

    return response


@pytest.fixture(autouse=True)
def clean_metax():
    """Clean Metax.

    DELETE all metadata from Metax that might be left from previous
    runs.
    """
    metax_client = MetaxClient(URL, USER, PASSWORD)
    files_dict = metax_client.get_files_dict("test_project")
    file_id_list = [value["id"] for value in files_dict.values()]
    if file_id_list:
        metax_client.client.delete_files(file_id_list)


@pytest.fixture(scope="function", autouse=True)
def integration_mock_setup(mock_config):
    """Configure client to use real Metax test instance."""
    mock_config["METAX_URL"] = URL
    mock_config["METAX_USER"] = USER
    mock_config["METAX_PASSWORD"] = PASSWORD


@pytest.mark.parametrize(
    "dataset", [True, False],
    ids=["File has a dataset", "File has no dataset"]
)
def test_gen_metadata_root(
        app, dataset, test_auth, monkeypatch, background_job_runner
):
    """Test metadata generation for root directory.

    Test that calling /v1/metadata produces correct metadata for all
    files of the project and metadata is removed when the file is
    removed.
    """
    if dataset:
        # Mock file_has_dataset to always return True
        monkeypatch.setattr(
            MetaxClient,
            "file_has_dataset",
            lambda a, b, c: True
        )

    test_client = app.test_client()

    # Upload integration.zip, which is extracted by the server
    response = _upload_file(
        test_client, "/v1/archives/test_project",
        test_auth, "tests/data/integration.zip"
    )
    background_job_runner(test_client, "upload", response)

    # Generate and POST metadata for all the files in test_project
    response = test_client.post(
        "/v1/metadata/test_project/*", headers=test_auth
    )
    # Finish the background job
    response = background_job_runner(test_client, "metadata", response)
    assert response.status_code == 200
    assert response.json['message'] == 'Metadata created: /'
    assert response.json['status'] == 'done'

    # All files should be found in Metax
    for file_path in ["integration/test1/test1.txt",
                      "integration/test2/test2.txt"]:
        response = test_client.get(f"/v1/files/test_project/{file_path}",
                                   headers=test_auth)
        file_identifier = response.json['identifier']
        assert Metax(
            URL,
            USER,
            PASSWORD
        ).get_file(file_identifier)['file_path'] == '/{}'.format(file_path)

    # DELETE single file
    response = test_client.delete(
        "/v1/files/test_project/integration/test1/test1.txt",
        headers=test_auth
    )
    assert response.status_code == 200
    if dataset:
        assert response.json["metax"].startswith(
            "Metadata is part of a dataset"
        )
    else:
        assert response.json["metax"]["deleted_files_count"] == 1

    # Test that test1.txt was removed from Metax but test2.txt is still
    # there
    metax_client = MetaxClient(URL, USER, PASSWORD)
    files_dict = metax_client.get_files_dict("test_project")

    if dataset:
        assert len(files_dict) == 2
    else:
        assert len(files_dict) == 1

    assert "/integration/test2/test2.txt" in files_dict


@pytest.mark.parametrize(
    "dataset", [True, False],
    ids=["File has a dataset", "File has no dataset"]
)
def test_gen_metadata_file(
        app, dataset, test_auth, monkeypatch, background_job_runner):
    """Test metadadata generation for single file.

    Test that generating metadata for a single file works and the
    metadata is removed when project is deleted.
    """
    if dataset:
        # Mock Metax.get_file2dataset_dict to always return a result
        # with an existing dataset for every given file ID
        monkeypatch.setattr(
            Metax,
            "get_file2dataset_dict",
            lambda _, file_ids: {fid: ["fake_dataset"] for fid in file_ids}
        )

    test_client = app.test_client()

    # Upload integration.zip, which is extracted by the server
    response = _upload_file(
        test_client, "/v1/archives/test_project",
        test_auth, "tests/data/integration.zip"
    )
    background_job_runner(test_client, "upload", response)

    # Generate and POST metadata for file test1.txt in test_project
    response = test_client.post(
        "/v1/metadata/test_project/integration/test1/test1.txt",
        headers=test_auth
    )
    response = background_job_runner(test_client, "metadata", response)
    assert response.status_code == 200
    assert response.json['message'] \
        == 'Metadata created: /integration/test1/test1.txt'
    assert response.json['status'] == 'done'

    # Metadata for test1.txt should be found in Metax
    response = test_client.get(
        "/v1/files/test_project/integration/test1/test1.txt",
        headers=test_auth
    )
    file_identifier = response.json['identifier']
    assert Metax(URL, USER, PASSWORD).get_file(file_identifier)['file_path'] \
        == "/integration/test1/test1.txt"

    # DELETE whole project
    response = test_client.delete("/v1/files/test_project", headers=test_auth)
    response = background_job_runner(test_client, "files", response)
    assert response.status_code == 200
    assert response.json["message"] == 'Deleted files and metadata: /'
    assert response.json["status"] == 'done'

    # Test that no test_project files are found in Metax
    metax_client = MetaxClient(URL, USER, PASSWORD)
    files_dict = metax_client.get_files_dict("test_project")

    if dataset:
        assert len(files_dict) == 1
    else:
        assert not files_dict


@pytest.mark.parametrize(
    "accepted_dataset", [True, False],
    ids=[
        "File has a dataset with status 120",
        "File has no dataset with status 120"
    ]
)
def test_delete_metadata(
        app, accepted_dataset, test_auth, background_job_runner):
    """Test metadata deletion.

    Verifies that metadata is 1) deleted for a file belonging to a
    dataset not accepted to preservation and is 2) not deleted when file
    belongs to dataset accepted to preservation.
    """
    test_client = app.test_client()

    # Upload integration.zip, which is extracted by the server
    poll_response = _upload_file(
        test_client, "/v1/archives/test_project",
        test_auth, "tests/data/integration.zip"
    )
    background_job_runner(test_client, "upload", poll_response)

    # Generate and POST metadata for file test1.txt in test_project
    poll_response = test_client.post(
        "/v1/metadata/test_project/integration/test1/test1.txt",
        headers=test_auth
    )
    response = background_job_runner(test_client, "metadata", poll_response)
    assert response.status_code == 200

    # Metadata for test1.txt should be found in Metax
    response = test_client.get(
        "/v1/files/test_project/integration/test1/test1.txt",
        headers=test_auth
    )
    file_identifier = response.json['identifier']
    assert Metax(URL, USER, PASSWORD).get_file(file_identifier)['file_path'] \
        == "/integration/test1/test1.txt"

    # Create dataset
    file_block = _create_dataset_file_block('/integration/test1/test1.txt',
                                            file_identifier)
    dataset_id = _create_dataset_with_file(accepted_dataset,
                                           file_block)['identifier']

    # Delete metadata for file test1.txt in test_project
    poll_response = test_client.delete(
        "/v1/metadata/test_project/integration/test1/test1.txt",
        headers=test_auth
    )
    response = background_job_runner(
        test_client, "metadata", poll_response,
        # If accepted dataset exists, the background job cannot succeed
        expect_success=not accepted_dataset
    )
    assert response.status_code == 200

    if accepted_dataset:
        assert response.json["message"] \
            == "Task failed"
        assert response.json["status"] == 'error'
    else:
        assert response.json["message"] == "1 files deleted"
        assert response.json["status"] == 'done'

    metax_client = MetaxClient(URL, USER, PASSWORD)
    files_dict = metax_client.get_files_dict("test_project")
    if accepted_dataset:
        assert len(files_dict) == 1
    else:
        assert not files_dict

    # DELETE whole project
    poll_response = test_client.delete(
        "/v1/files/test_project", headers=test_auth
    )
    response = background_job_runner(
        test_client, "files", poll_response
    )
    assert response.status_code == 200
    assert response.json["message"] == "Deleted files and metadata: /"
    assert response.json["status"] == "done"

    # Test that no test_project files are found in Metax
    files_dict = metax_client.get_files_dict("test_project")
    if accepted_dataset:
        assert len(files_dict) == 1
    else:
        assert not files_dict

    response = requests.delete("%s/rest/datasets/%s" % (URL, dataset_id),
                               auth=(USER, PASSWORD),
                               verify=False)
    assert response.status_code == 204


@pytest.mark.parametrize(
    "dataset", [True, False],
    ids=["File has a dataset", "File has no dataset"]
)
def test_disk_cleanup(
    app, dataset, test_auth, monkeypatch, background_job_runner, mock_config
):
    """Test file metadata clean up.

    Test that cleanup script removes file metadata from Metax if it
    is not associated with any dataset.
    """
    # Mock configuration
    mock_config["CLEANUP_TIMELIM"] = -1

    if dataset:
        # Mock file_has_dataset to always return True
        monkeypatch.setattr(
            MetaxClient,
            "file_has_dataset",
            lambda a, b, c: True
        )

    test_client = app.test_client()

    # Upload integration.zip, which is extracted by the server
    poll_response = _upload_file(
        test_client, "/v1/archives/test_project",
        test_auth, "tests/data/integration.zip"
    )
    response = background_job_runner(test_client, "upload", poll_response)
    assert response.status_code == 200

    # Generate and POST metadata for all the files in test_project
    poll_response = test_client.post(
        "/v1/metadata/test_project/*", headers=test_auth
    )
    response = background_job_runner(test_client, "metadata", poll_response)
    assert response.status_code == 200

    # Cleanup all files
    clean.clean_disk()

    # Test that no test_project files are found in Metax
    metax_client = MetaxClient(URL, USER, PASSWORD)
    files_dict = metax_client.get_files_dict("test_project")

    if dataset:
        assert len(files_dict) == 2
    else:
        assert not files_dict


def test_mongo_cleanup(
        app, test_auth, monkeypatch, background_job_runner, mock_config
):
    """Test database cleanup.

    Test that cleaning files from mongo deletes all files that haven't
    been posted to Metax.
    """
    # Mock configuration
    mock_config["METAX_PASSWORD"] = PASSWORD
    mock_config["CLEANUP_TIMELIM"] = -1

    test_client = app.test_client()

    # Mock Files mongo connection
    def _mock_init(self, _client):
        host = app.config.get("MONGO_HOST")
        port = app.config.get("MONGO_PORT")
        self.files = pymongo.MongoClient(host, port).upload.files

    monkeypatch.setattr(db.Files, "__init__", _mock_init)

    files_col = db.Database().files

    # ----- Inserting fake files to Mongo and cleaning them
    files_col.insert([
        {"_id": "1", "identifier": "pid:urn:1", "checksum": "checksum_1"},
        {"_id": "2", "identifier": "pid:urn:2", "checksum": "checksum_2"}
    ])
    assert len(files_col.get_all_ids()) == 2

    clean.clean_mongo()

    # File documents should persist, but identifiers should be gone
    assert len(files_col.get_all_files()) == 2
    assert not files_col.get_all_ids()

    # Remove fake files before next test
    files_col.delete(["1", "2"])

    # Upload integration.zip, which is extracted by the server
    poll_response = _upload_file(
        test_client, "/v1/archives/test_project",
        test_auth, "tests/data/integration.zip"
    )
    response = background_job_runner(test_client, "upload", poll_response)
    assert response.status_code == 200

    # Generate and POST metadata for all the files in test_project
    poll_response = test_client.post(
        "/v1/metadata/test_project/*", headers=test_auth
    )
    response = background_job_runner(test_client, "metadata", poll_response)
    assert response.status_code == 200

    # Check that generated identifiers were added to Mongo
    assert len(files_col.get_all_ids()) == 2
    # Check that generated file_paths resolve to actual files
    for file_doc in files_col.get_all_files():
        file_path = file_doc["_id"]
        assert os.path.isfile(file_path)

    # Try to clean file documents that still exist in Metax
    clean.clean_mongo()

    assert len(files_col.get_all_ids()) == 2


def _create_dataset_with_file(accepted_dataset, dataset_file_data_block):
    """Create a dataset into Metax."""
    resp = requests.get(
        "%s/rpc/datasets/get_minimal_dataset_template?type=service" % URL,
        verify=False
    )
    if resp.status_code != 200:
        raise Exception('Error retrieving dataset template from metax: %s'
                        % str(resp.content))
    try:
        dataset = resp.json()
    except Exception as exception:
        raise Exception("Error retrieving dataset template from metax: %s"
                        % str(resp.content)) from exception

    dataset["research_dataset"]['files'] = []
    dataset["research_dataset"]['directories'] = []
    dataset["data_catalog"] = "urn:nbn:fi:att:data-catalog-pas"
    dataset["research_dataset"]["publisher"] = {
        "name": {
            "fi": "School services, ARTS",
            "und": "School services, ARTS"
        },
        "@type": "Organization",
        "homepage": {
            "title": {
                "en": "Publisher website",
                "fi": "Julkaisijan kotisivu"
            },
            "identifier": "http://www.publisher.fi/"
        },
        "identifier": "http://uri.suomi.fi/codelist/fairdata/organization",
    }
    dataset['research_dataset']['title']['en'] = (
        "Upload-rest-api Integration Test Dataset")
    dataset['research_dataset']['issued'] = "1997-02-21"
    dataset['research_dataset']['files'] = [dataset_file_data_block]
    if accepted_dataset:
        dataset["preservation_state"] = 120
    resp = requests.post("%s/rest/datasets" % URL,
                         headers={'Content-Type': 'application/json'},
                         json=dataset,
                         auth=(USER, PASSWORD),
                         verify=False)
    if resp.status_code != 201:
        raise Exception('Metax create dataset fails: ' + str(resp.json()))
    return resp.json()


def _create_dataset_file_block(file_path, file_identifier):
    return {
        "title": "Title for " + file_path,
        "identifier": file_identifier,
        "file_type": {
            "in_scheme": ("http://uri.suomi.fi/codelist/"
                          "fairdata/file_type"),
            "identifier": ("http://uri.suomi.fi/codelist/fairdata/"
                           "file_type/code/image"),
            "pref_label": {
                "en": "Text",
                "fi": "Teksti",
                "und": "Teksti"
            }
        },
        "use_category": {
            "in_scheme": ("http://uri.suomi.fi/codelist/fairdata/"
                          "use_category"),
            "identifier": ("http://uri.suomi.fi/codelist/fairdata/"
                           "use_category/code/source"),
            "pref_label": {
                "en": "Source material",
                "fi": "Lahdeaineisto",
                "und": "Lahdeaineisto"
            }
        }
    }
