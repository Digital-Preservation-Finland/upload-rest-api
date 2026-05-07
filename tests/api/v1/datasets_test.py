"""Tests for ``upload_rest_api.api.v1.datasets`` module."""
import copy
from pathlib import Path

import pytest
from metax_access import (
    DS_STATE_IN_DIGITAL_PRESERVATION,
    DS_STATE_TECHNICAL_METADATA_GENERATED,
    template_data,
)

from tests.utils import update_nested_dict


@pytest.mark.parametrize(
    ("datasets", "has_pending_dataset"),
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
                            "pref_label": {"en": "English"},
                        }
                    ],
                    "fileset": {"csc_project": "bar"},
                    "preservation": {
                        "state": DS_STATE_IN_DIGITAL_PRESERVATION
                    },
                },
                {
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
                            "pref_label": {"en": "English"},
                        }
                    ],
                    "fileset": {"csc_project": "bar"},
                    "preservation": {
                        "state": DS_STATE_IN_DIGITAL_PRESERVATION
                    },
                },
                {
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
                        "state": DS_STATE_IN_DIGITAL_PRESERVATION
                    },
                },
            ],
            False,
        ),
    ],
)
def test_get_file_datasets_directory(
        app, test_client, test_auth, requests_mock, datasets,
        has_pending_dataset
):
    upload_path = Path(app.config.get("UPLOAD_PROJECTS_PATH"))

    # Create two test files in "test" directory
    project_path = upload_path / "test_project"
    (project_path / "test").mkdir(parents=True)
    (project_path / "test" / "test1.txt").write_text("test1")
    (project_path / "test" / "test2.txt").write_text("test2")
    # Add metadata for files in Metax
    file1 = copy.deepcopy(template_data.FILE)
    file1["id"] = "test-id-1"
    file1["pathname"] = "/test/test1.txt"
    file1["checksum"] = "3e7705498e8be60520841409ebc69bc1"
    file2 = copy.deepcopy(template_data.FILE)
    file2["id"] = "test-id-2"
    file2["pathname"] = "/test/test2.txt"
    file2["checksum"] = "3e7705498e8be60520841409ebc69bc1"
    requests_mock.get(
        "/v3/files?pathname__startswith=/test/",
        json={
            "next": None,
            "results": [file1, file2],
        },
    )

    requests_mock.post(
        "/v3/files/datasets?relations=true",
        additional_matcher=(
            lambda req: set(req.json()) == {"test-id-1", "test-id-2"}
        ),
        json={
            "test-id-1": ["urn:uuid:dataset1", "urn:uuid:dataset3"]
        }
    )
    requests_mock.get(
        "/v3/datasets",
        additional_matcher=lambda req: set(req.qs["id"][0].split(","))
        == {"urn:uuid:dataset1", "urn:uuid:dataset3"},
        json=[
            update_nested_dict(template_data.DATASET, datasets[0]),
            update_nested_dict(template_data.DATASET, datasets[1]),
        ],
    )

    response = test_client.get(
        "/v1/datasets/test_project/test", headers=test_auth
    )
    result = response.json
    assert result["has_pending_dataset"] == has_pending_dataset
    assert len(result["datasets"]) == 2
    dataset_a, dataset_b = sorted(result["datasets"],
                                  key=lambda ds: ds["title"]["en"])

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
