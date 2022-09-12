"""/datasets/v1 endpoints.

Functionality for retrieving dataset information related to pre-ingest
storage files.
"""
from flask import Blueprint, abort
from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_REJECTED_IN_DIGITAL_PRESERVATION_SERVICE)

from upload_rest_api.gen_metadata import MetaxClient
from upload_rest_api.authentication import current_user

DATASETS_API_V1 = Blueprint("datasets_v1", __name__, url_prefix="/v1/datasets")


@DATASETS_API_V1.route("/<string:project_id>/<path:fpath>", methods=["GET"])
def get_file_datasets(project_id, fpath):
    """Get the datasets associated with the given file or directory."""
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permissions to access this project")

    metax = MetaxClient()
    datasets = metax.get_file_datasets(project_id, fpath)

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
