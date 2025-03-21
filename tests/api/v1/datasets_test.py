"""Tests for ``upload_rest_api.api.v1.datasets`` module."""
from pathlib import Path

import pytest
from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_IN_DISSEMINATION,
                          DS_STATE_TECHNICAL_METADATA_GENERATED)
from tests.metax_data.utils import TEMPLATE_DATASET, update_nested_dict


@pytest.mark.parametrize(
    "datasets,has_pending_dataset",
    [
        # Contains a pending dataset
        (
            [
                {
                    "id": "urn:uuid:dataset1",
                    "title": {"en": "Dataset 1"},
                    "language": [
                        {
                            "url": "http://lexvo.org/id/iso639-3/eng",
                            "title": {"en": "English"},
                        }
                    ],
                    "fileset": {"csc_project": "bar"},
                    "preservation": {
                        "state": DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
                    },
                },
                {
                    "id": "urn:uuid:dataset3",
                    "title": {"en": "Dataset 3"},
                    "language": [
                        {
                            "url": "http://lexvo.org/id/iso639-3/eng",
                            "title": {"en": "English"},
                        }
                    ],
                    "fileset": {"csc_project": "bar"},
                    "preservation": {
                        "state": DS_STATE_TECHNICAL_METADATA_GENERATED
                    },
                },
            ],
            True,
        ),
        # Doesn't contain pending datasets
        (
            [
                {
                    "id": "urn:uuid:dataset1",
                    "title": {"en": "Dataset 1"},
                    "language": [
                        {
                            "url": "http://lexvo.org/id/iso639-3/eng",
                            "title": {"en": "English"},
                        }
                    ],
                    "fileset": {"csc_project": "bar"},
                    "preservation": {
                        "state": DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
                    },
                },
                {
                    "id": "urn:uuid:dataset3",
                    "title": {"en": "Dataset 3"},
                    "language": [
                        {
                            "url": "http://lexvo.org/id/iso639-3/eng",
                            "title": {"en": "English"},
                        }
                    ],
                    "fileset": {"csc_project": "bar"},
                    "preservation": {
                        "state": DS_STATE_IN_DISSEMINATION
                    },
                },
            ],
            False,
        ),
    ],
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
        "/v3/files/datasets?relations=true&include_nulls=True",
        additional_matcher=(
            lambda req: set(req.json()) == {"urn:uuid:test1", "urn:uuid:test2"}
        ),
        json={
            "urn:uuid:test1": ["urn:uuid:dataset1", "urn:uuid:dataset3"]
        }
    )
    requests_mock.get(
        "/v3/datasets/urn:uuid:dataset1?include_nulls=True",
        json = update_nested_dict(TEMPLATE_DATASET, datasets[0])
    )
    requests_mock.get(
        "/v3/datasets/urn:uuid:dataset3?include_nulls=True",
        json = update_nested_dict(TEMPLATE_DATASET, datasets[1])
    )

    response = test_client.get(
        "/v1/datasets/test_project/test", headers=test_auth
    )
    result = response.json
    assert result["has_pending_dataset"] == has_pending_dataset
    assert len(result["datasets"]) == 2
    dataset_a, dataset_b = sorted(result["datasets"], key= lambda ds: ds["title"]["en"])

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
