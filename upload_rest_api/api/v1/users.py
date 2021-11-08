"""REST API for querying user information."""
from flask import Blueprint, jsonify

from upload_rest_api.database import Database
from upload_rest_api.authentication import current_user


USERS_API_V1 = Blueprint("users_v1", __name__, url_prefix="/v1/users")


@USERS_API_V1.route("/projects", methods=["GET"])
def list_user_projects():
    """
    Endpoint for retrieving all projects accessible to the currently
    authenticated user
    """
    database = Database()

    projects = current_user.projects
    projects = [database.projects.get(project_id) for project_id in projects]

    result = {"projects": []}

    for project in projects:
        result["projects"].append({
            "identifier": project["_id"],
            "used_quota": project["used_quota"],
            "quota": project["quota"]
        })

    return jsonify(result)
