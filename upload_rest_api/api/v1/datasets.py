"""/datasets/v1 endpoints.

Functionality for retrieving dataset information related to pre-ingest
storage files.
"""
from flask import Blueprint, abort, current_app
from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE,
                          Metax)

from upload_rest_api.authentication import current_user
from upload_rest_api.database import Database, Projects

DATASETS_API_V1 = Blueprint("datasets_v1", __name__, url_prefix="/v1/datasets")


LANGUAGE_IDENTIFIERS = {
    "http://lexvo.org/id/iso639-3/eng": "en",
    "http://lexvo.org/id/iso639-3/fin": "fi",
    "http://lexvo.org/id/iso639-3/swe": "sv"
}


def _get_dataset_results(metax, dataset_ids):
    """Get list of results to return in the JSON response.

    :param metax: metax_access.Metax instance
    :param dataset_ids: List of dataset IDs
    """
    def _dataset_to_result(dataset):
        language_identifiers = [
            LANGUAGE_IDENTIFIERS[language["identifier"]]
            for language in dataset["research_dataset"].get("language", [])
            if LANGUAGE_IDENTIFIERS.get(language["identifier"], None)
        ]

        return {
            "title": dataset["research_dataset"]["title"],
            "languages": language_identifiers,
            "identifier": dataset["identifier"],
            "preservation_state": dataset["preservation_state"],
        }

    if not dataset_ids:
        return []

    count = 0
    total_count = 0
    result = metax.get_datasets_by_ids(
        dataset_ids,
        fields=["identifier", "preservation_state", "research_dataset"]
    )
    datasets = []

    while True:
        total_count = result["count"]

        for dataset in result["results"]:
            datasets.append(_dataset_to_result(dataset))
            count += 1

        if count >= total_count:
            break

        # We haven't retrieved all datasets yet
        result = metax.list_datasets(
            dataset_ids,
            fields=["identifier", "preservation_state", "research_dataset"],
            offset=count
        )

    return datasets


@DATASETS_API_V1.route("/<string:project_id>/<path:fpath>", methods=["GET"])
def get_file_datasets(project_id, fpath):
    """Get the datasets associated with the given file or directory."""
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permissions to access this project")

    db = Database()

    upload_path = Projects.get_upload_path(project_id, fpath)

    file_identifiers = []

    if upload_path.is_file():
        file_identifier = db.files.get_identifier(upload_path)
        if not file_identifier:
            abort(404, "File does not have Metax identifier")

        file_identifiers.append(file_identifier)
    elif upload_path.is_dir():
        file_identifiers = [
            file_["identifier"]
            for file_ in db.files.iter_files_in_dir(upload_path)
            if "identifier" in file_
        ]
    else:
        abort(404, "File or directory not found")

    metax = Metax(
        url=current_app.config.get("METAX_URL"),
        user=current_app.config.get("METAX_USER"),
        password=current_app.config.get("METAX_PASSWORD"),
        verify=current_app.config.get("METAX_SSL_VERIFICATION")
    )

    # Retrieve file -> dataset(s) associations
    file2dataset = metax.get_file2dataset_dict(file_identifiers)
    dataset_ids = set()
    for dataset_ids_ in file2dataset.values():
        dataset_ids |= set(dataset_ids_)
    dataset_ids = list(dataset_ids)

    # Retrieve additional information about datasets
    datasets = _get_dataset_results(metax, dataset_ids)

    # Check if any of the datasets is not accepted.
    # If so, the client should prevent the user from deleting any files.
    has_pending_dataset = any(
        dataset["preservation_state"]
        < DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION
        or dataset["preservation_state"]
        == DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE
        for dataset in datasets
    )

    return {
        "datasets": datasets,
        "has_pending_dataset": has_pending_dataset
    }
