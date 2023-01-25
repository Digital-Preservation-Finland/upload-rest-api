"""REST API for querying user information."""
from flask import Blueprint, abort, jsonify, request

from upload_rest_api.authentication import current_user
from upload_rest_api.models import Project, User

USERS_API_V1 = Blueprint("users_v1", __name__, url_prefix="/v1/users")


@USERS_API_V1.route("/projects", methods=["GET"])
def list_user_projects():
    """
    Endpoint for retrieving all projects accessible to the currently
    authenticated user or the specific user.

    If the "username" parameter is determined, the authenticated user
    has to be an administrator to view the details.
    """
    if request.args.get("username", None):
        # If specific username is provided, retrieve the projects for that
        # user, if we have the permission
        username = request.args["username"]

        if not current_user.is_allowed_to_list_projects(username):
            abort(403, "User does not have permission to list projects")

        projects = User.get(username=username).projects
    else:
        # If 'username' is not provided, retrieve projects accessible
        # to the current session
        projects = [Project.get(id=project_id)
                    for project_id in current_user.projects]

    result = {"projects": []}
    for project in projects:
        result["projects"].append({
            "identifier": project.id,
            "used_quota": project.used_quota,
            "quota": project.quota
        })

    return jsonify(result)
