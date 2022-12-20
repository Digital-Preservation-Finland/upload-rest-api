"""/directories/v1 endpoints.

Functionality for creating directories.
"""
from flask import Blueprint, abort, jsonify

from upload_rest_api.authentication import current_user
from upload_rest_api.models.resource import DirectoryResource


DIRECTORIES_API_V1 = Blueprint(
    "directories_v1", __name__, url_prefix="/v1/directories"
)


@DIRECTORIES_API_V1.route(
    "/<string:project_id>/<path:dir_path>", methods=["POST"]
)
def create_directory(project_id, dir_path):
    """Create a directory inside given project."""
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permission to access this project")

    try:
        resource = DirectoryResource.create(
            project_id=project_id, path=dir_path
        )
    except FileExistsError:
        abort(409, "Directory already exists")

    return jsonify({
        "dir_path": str(resource.path),
        "status": "created"
    })
