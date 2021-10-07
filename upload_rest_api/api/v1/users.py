"""REST API for querying user information."""
from flask import Blueprint, jsonify, request, abort

from upload_rest_api.database import Database
from upload_rest_api.authentication import current_user


USERS_API_V1 = Blueprint("users_v1", __name__, url_prefix="/v1/users")


@USERS_API_V1.route("/projects", methods=["GET"])
def list_user_projects():
    """
    Endpoint for retrieving all projects accessible to the currently
    authenticated user or the specific user.

    If the "username" parameter is determined, the authenticated user
    has to be an administrator to view the details.
    """
    database = Database()

    if request.args.get("username", None):
        # If specific username is provided, retrieve the projects for that
        # user, if we have the permission
        username = request.args["username"]

        if not current_user.is_allowed_to_list_projects(username):
            abort(403, "User does not have permission to list projects")

        projects = database.user(username).get_projects()
    else:
        # If 'username' is not provided, retrieve projects accessible
        # to the current session
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
