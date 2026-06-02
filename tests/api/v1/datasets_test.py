"""Tests for ``upload_rest_api.api.v1.datasets`` module."""
import copy

from metax_access import template_data

from tests.utils import update_nested_dict
from upload_rest_api.models.project import Project


def test_get_directory_datasets(test_client, test_auth, requests_mock):
    """Test fetching datasets that contain a file from the directory.

    Creates a directory which contains a locked file and an unlocked
    file. The files are added to a few different datasets. The pending
    datasets of the directory are feched from /v1/datasets API. The
    response should contain only the pending datasets, and the expected
    metadata of the datasets should be included.

    :param test_client: Flask test client
    :param test_auth: Authentication headers
    :param requests_mock: HTTP request mocker
    """
    # Create two test files in "test" directory. One of the files is
    # locked, one is not.
    project_path = Project.get(id="test_project").directory
    (project_path / "test").mkdir()
    (project_path / "test" / "unlocked_file").touch()
    (project_path / "test" / "locked_file").touch()

    # Add metadata for files in Metax
    locked_file = copy.deepcopy(template_data.FILE)
    locked_file["id"] = "locked-file-id"
    locked_file["pathname"] = "/test/locked_file"
    locked_file["pas_process_running"] = True
    unlocked_file = copy.deepcopy(template_data.FILE)
    unlocked_file["id"] = "unlocked-file-id"
    unlocked_file["pathname"] = "/test/unlocked_file"
    unlocked_file["pas_process_running"] = False
    requests_mock.get(
        "/v3/files?pathname__startswith=/test/",
        json={
            "next": None,
            "results": [locked_file, unlocked_file],
        },
    )

    # Create three datasets.
    # Files should be locked when they are part of pending dataset. Note
    # that pending datasets can currently contain also unlocked files
    # because of this bug: TPASPKT-1191. Pending dataset should be
    # included in the response.
    pending_dataset = {
        "id": "pending-dataset-id",
        "title": {"en": "Pending dataset title"},
        "language": [
            {
                "url": "http://lexvo.org/id/iso639-3/eng",
                "pref_label": {"en": "English"},
            }
        ],
        "fileset": {"csc_project": "bar"},
        "preservation": {
            "pas_package_created": False
        },
    }
    # If preservation process is not not started, or the dataset is
    # reset, files should be unlocked. Dataset that does not contain any
    # locked files should not be included in the response.
    initialized_dataset = {
        "id": "initialized-dataset-id",
        "title": {"en": "Initialized dataset title"},
        "language": [
            {
                "url": "http://lexvo.org/id/iso639-3/eng",
                "pref_label": {"en": "English"},
            }
        ],
        "fileset": {"csc_project": "bar"},
        "preservation": {
            "pas_package_created": False
        },
    }
    # Preserved datasets do not usually contain locked files, but the
    # same files can be added to multiple datasets, so it is possible
    # that some of the preserved files are locked. But preserved
    # datasets should never be listed, because they do not lock the
    # files.
    # TODO: preserved files should probably be in different
    # file-storage: TPASPKT-749
    preserved_dataset = {
        "id": "preserved-dataset-id",
        "title": {"en": "Preserved dataset title"},
        "language": [
            {
                "url": "http://lexvo.org/id/iso639-3/eng",
                "pref_label": {"en": "English"},
            }
        ],
        "fileset": {"csc_project": "bar"},
        "preservation": {
            "pas_package_created": True
        },
    }
    requests_mock.get(
        "/v3/datasets",
        additional_matcher=lambda req: set(req.qs["id"][0].split(","))
        == {
            pending_dataset["id"],
            initialized_dataset["id"],
            preserved_dataset["id"]
        },
        json=[
            update_nested_dict(template_data.DATASET, pending_dataset),
            update_nested_dict(template_data.DATASET, initialized_dataset),
            update_nested_dict(template_data.DATASET, preserved_dataset),
        ],
    )
    requests_mock.post(
        "/v3/files/datasets?relations=true",
        additional_matcher=(
            lambda req: set(req.json())
            == {locked_file["id"], unlocked_file["id"]}
        ),
        json={
            locked_file["id"]: [
                preserved_dataset["id"],
                pending_dataset["id"],
            ],
            unlocked_file["id"]: [
                preserved_dataset["id"],
                initialized_dataset["id"],
            ]
        }
    )

    # Only pending dataset should appear:
    # - preserved_dataset is excluded because pas_package_created=True
    # - initialized_dataset is excluded because it contains only
    #   unlocked files
    assert test_client.get(
        "/v1/datasets/test_project/test", headers=test_auth
    ).json == {
        "has_pending_dataset": True,
        "datasets": [
            {
                "identifier": pending_dataset["id"],
                "title": pending_dataset["title"],
                "languages": ["en"],
            }
        ]
    }


def test_no_pending_datasets(test_client, test_auth, requests_mock):
    """Test fetching datasets of directory without pending files.

    :param test_client: Flask test client
    :param test_auth: Authentication headers
    :param requests_mock: HTTP request mocker
    """
    project_path = Project.get(id="test_project").directory
    (project_path / "test").mkdir()
    (project_path / "test" / "unlocked_file").touch()
    file = copy.deepcopy(template_data.FILE)
    file["pathname"] = "/test/unlocked_file"
    file["pas_process_running"] = False
    requests_mock.get(
        "/v3/files?pathname__startswith=/test/",
        json={"next": None, "results": [file]},
    )

    requests_mock.post(
        "/v3/files/datasets?relations=true",
        additional_matcher=(
            lambda req: set(req.json()) == {file["id"]}
        ),
        json={}
    )

    assert test_client.get(
        "/v1/datasets/test_project/test", headers=test_auth
    ).json == {
        "has_pending_dataset": False,
        "datasets": []
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
