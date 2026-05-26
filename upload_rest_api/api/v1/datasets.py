"""/datasets/v1 endpoints.

Functionality for retrieving dataset information related to pre-ingest
storage files.
"""
from flask import Blueprint, abort

from upload_rest_api.authentication import current_user
from upload_rest_api.models.resource import get_resource

DATASETS_API_V1 = Blueprint("datasets_v1", __name__, url_prefix="/v1/datasets")

LANGUAGE_IDENTIFIERS = {
    "http://lexvo.org/id/iso639-3/eng": "en",
    "http://lexvo.org/id/iso639-3/fin": "fi",
    "http://lexvo.org/id/iso639-3/swe": "sv"
}


@DATASETS_API_V1.route("/<string:project_id>/<path:fpath>", methods=["GET"])
def get_file_datasets(project_id, fpath):
    """Get the datasets associated with the given file or directory."""
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permissions to access this project")

    resource = get_resource(project_id, fpath)
    datasets = []
    for dataset in resource.get_datasets():
        language_identifiers = [
            LANGUAGE_IDENTIFIERS[language["url"]]
            for language in dataset["language"]
            if LANGUAGE_IDENTIFIERS.get(language["url"])
        ]

        datasets.append(
            {
                "title": dataset["title"],
                "languages": language_identifiers,
                "identifier": dataset["id"],
            }
        )

    return {
        "datasets": datasets,
        "has_pending_dataset": resource.locked
    }
