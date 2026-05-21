"""Tests for ``upload_rest_api.api.v1.datasets`` module."""
import copy

from metax_access import (
    # TODO: remove preservation states
    DS_STATE_IN_DIGITAL_PRESERVATION,
    DS_STATE_TECHNICAL_METADATA_GENERATED,
    template_data,
)

from tests.utils import update_nested_dict
from upload_rest_api.models.project import Project


def test_get_directory_datasets(test_client, test_auth, requests_mock):
    """Test fetching datasets that contain the directory.

    Creates a directory which contains a file which is added to
    'datasets'. Fetches datasets that contain the directory from
    /v1/datasets API, and checks that response contains expected
    data.

    :param test_client: Flask test client
    :param test_auth: Authentication headers
    :param requests_mock: HTTP request mocker
    """
    # Create two test files in "test" directory
    project_path = Project.get(id="test_project").directory
    (project_path / "test").mkdir()
    (project_path / "test" / "test1.txt").write_text("test1")
    (project_path / "test" / "test2.txt").write_text("test2")

    # Add metadata for files in Metax
    file1 = copy.deepcopy(template_data.FILE)
    file1["id"] = "test-id-1"
    file1["pathname"] = "/test/test1.txt"
    file1["checksum"] = "3e7705498e8be60520841409ebc69bc1"
    file1["pas_process_running"] = True
    file2 = copy.deepcopy(template_data.FILE)
    file2["id"] = "test-id-2"
    file2["pathname"] = "/test/test2.txt"
    file2["checksum"] = "3e7705498e8be60520841409ebc69bc1"
    file2["pas_process_running"] = False
    requests_mock.get(
        "/v3/files?pathname__startswith=/test/",
        json={
            "next": None,
            "results": [file1, file2],
        },
    )

    # Add one of the files to two datasets
    requests_mock.post(
        "/v3/files/datasets?relations=true",
        additional_matcher=(
            lambda req: set(req.json()) == {"test-id-1", "test-id-2"}
        ),
        json={
            "test-id-1": ["urn:uuid:dataset1", "urn:uuid:dataset3"]
        }
    )

    # Add dataset metadata to Metax
    dataset1 = {
        "id": "urn:uuid:dataset1",
        "title": {"en": "Dataset 1"},
        "language": [
            {
                "url": "http://lexvo.org/id/iso639-3/eng",
                "pref_label": {"en": "English"},
            }
        ],
        "fileset": {"csc_project": "bar"},
        "preservation": {
            "state": DS_STATE_IN_DIGITAL_PRESERVATION
        },
    }
    dataset3 = {
        "id": "urn:uuid:dataset3",
        "title": {"en": "Dataset 3"},
        "language": [
            {
                "url": "http://lexvo.org/id/iso639-3/eng",
                "pref_label": {"en": "English"},
            }
        ],
        "fileset": {"csc_project": "bar"},
        "preservation": {
            "state": DS_STATE_TECHNICAL_METADATA_GENERATED
        },
    }
    requests_mock.get(
        "/v3/datasets",
        additional_matcher=lambda req: set(req.qs["id"][0].split(","))
        == {"urn:uuid:dataset1", "urn:uuid:dataset3"},
        json=[
            update_nested_dict(template_data.DATASET, dataset1),
            update_nested_dict(template_data.DATASET, dataset3),
        ],
    )

    # Check that response from API contains expected data
    assert test_client.get(
        "/v1/datasets/test_project/test", headers=test_auth
    ).json == {
        "has_pending_dataset": True,  # TODO: remove this?
        "datasets": [
            {
                "identifier": "urn:uuid:dataset1",
                "languages": [
                    "en",
                ],
                # TODO: remove preservation state
                "preservation_state": DS_STATE_IN_DIGITAL_PRESERVATION,
                "title": {
                    "en": "Dataset 1",
                },
            },
            {
                "identifier": "urn:uuid:dataset3",
                "languages": [
                    "en",
                ],
                # TODO: remove preservation state
                "preservation_state": DS_STATE_TECHNICAL_METADATA_GENERATED,
                "title": {
                    "en": "Dataset 3",
                },
            },
        ],
    }


def test_no_rights(test_auth2, test_client):
    """
    Test that attempting to access dataset details without permission
    results in a 403 Forbidden response
    """
    response = test_client.get(
        "/v1/datasets/test_project/test_file", headers=test_auth2
    )

    assert response.status_code == 403
