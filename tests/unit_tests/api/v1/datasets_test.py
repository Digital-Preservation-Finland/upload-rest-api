"""Tests for ``upload_rest_api.api.v1.datasets`` module."""
from pathlib import Path

import pytest
from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_IN_DISSEMINATION,
                          DS_STATE_TECHNICAL_METADATA_GENERATED)


def _get_research_dataset(title):
    return {
        "title": {"en": title},
        "language": [
            {
                "identifier": "http://lexvo.org/id/iso639-3/eng",
                "title": {"en": "English"}
            }
        ],
        "files": [
            {
                "details": {
                    "project_identifier": "bar"
                }
            }
        ]
    }


@pytest.mark.parametrize(
    "datasets,has_pending_dataset",
    [
        # Contains a pending dataset
        (
            [
                {
                    "identifier": "urn:uuid:dataset1",
                    "research_dataset": _get_research_dataset("Dataset 1"),
                    "preservation_state":
                        DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
                },
                {
                    "identifier": "urn:uuid:dataset3",
                    "research_dataset": _get_research_dataset("Dataset 3"),
                    "preservation_state":
                        DS_STATE_TECHNICAL_METADATA_GENERATED
                }
            ],
            True
        ),
        # Doesn't contain pending datasets
        (
            [
                {
                    "identifier": "urn:uuid:dataset1",
                    "research_dataset": _get_research_dataset("Dataset 1"),
                    "preservation_state":
                        DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
                },
                {
                    "identifier": "urn:uuid:dataset3",
                    "research_dataset": _get_research_dataset("Dataset 3"),
                    "preservation_state":
                        DS_STATE_IN_DISSEMINATION
                }
            ],
            False
        )
    ]
)
def test_get_file_datasets_directory(
        app, test_client, test_auth, requests_mock,
        test_mongo, datasets, has_pending_dataset):
    upload_path = Path(app.config.get("UPLOAD_PROJECTS_PATH"))

    project_path = upload_path / "test_project"
    (project_path / "test").mkdir(parents=True)
    (project_path / "test" / "test1.txt").write_text("test1")
    (project_path / "test" / "test2.txt").write_text("test2")

    test_mongo.upload.files.insert_many([
        {
            "_id": str(project_path / "test" / "test1.txt"),
            "checksum": "3e7705498e8be60520841409ebc69bc1",
            "identifier": "urn:uuid:test1"
        },
        {
            "_id": str(project_path / "test" / "test2.txt"),
            "checksum": "126a8a51b9d1bbd07fddc65819a542c3",
            "identifier": "urn:uuid:test2"
        }
    ])

    requests_mock.post(
        "https://metax.localdomain/rest/v2/files/datasets?keys=files",
        additional_matcher=(
            lambda req: set(req.json()) == {"urn:uuid:test1", "urn:uuid:test2"}
        ),
        json={
            "urn:uuid:test1": ["urn:uuid:dataset1", "urn:uuid:dataset3"]
        }
    )
    requests_mock.post(
        "https://metax.localdomain/rest/datasets/list?offset=0&limit=1000000"
        "&fields=identifier%2Cpreservation_state%2Cresearch_dataset",
        additional_matcher=(
            lambda req: set(req.json()) == {"urn:uuid:dataset1",
                                            "urn:uuid:dataset3"}
        ),
        json={
            "count": 2, "next": None, "previous": None,
            "results": datasets
        }
    )

    response = test_client.get(
        "/v1/datasets/test_project/test", headers=test_auth
    )
    result = response.json

    assert result["has_pending_dataset"] == has_pending_dataset
    assert len(result["datasets"]) == 2
    dataset_a, dataset_b = result["datasets"]

    assert dataset_a["title"]["en"] == "Dataset 1"
    assert dataset_a["languages"] == ["en"]

    assert dataset_b["title"]["en"] == "Dataset 3"
    assert dataset_b["languages"] == ["en"]


def test_no_rights(test_auth2, test_client):
    """
    Test that attempting to access dataset details without permission
    results in a 403 Forbidden response
    """
    response = test_client.get(
        "/v1/datasets/test_project/test_file", headers=test_auth2
    )

    assert response.status_code == 403
