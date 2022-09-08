"""/datasets/v1 endpoints.

Functionality for retrieving dataset information related to pre-ingest
storage files.
"""
from flask import Blueprint, abort

from upload_rest_api.resource import get_resource
from upload_rest_api.authentication import current_user

DATASETS_API_V1 = Blueprint("datasets_v1", __name__, url_prefix="/v1/datasets")


@DATASETS_API_V1.route("/<string:project_id>/<path:fpath>", methods=["GET"])
def get_file_datasets(project_id, fpath):
    """Get the datasets associated with the given file or directory."""
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permissions to access this project")

    resource = get_resource(project_id, fpath)

    return {
        "datasets": resource.datasets(),
        "has_pending_dataset": resource.has_pending_dataset()
    }
