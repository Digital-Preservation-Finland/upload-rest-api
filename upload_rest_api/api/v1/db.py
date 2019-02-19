"""REST api for uploading files into passipservice
"""
from flask import Blueprint, jsonify

import upload_rest_api.authentication as auth
import upload_rest_api.database as db


DB_API_V1 = Blueprint("db_v1", __name__, url_prefix="/db/v1")


@DB_API_V1.route("/<string:username>", methods=["GET"])
def get_user(username):
    """Get user username from the database.

    :returns: HTTP Response
    """
    auth.admin_only()

    user = db.UsersDoc(username)
    response = jsonify(user.get_utf8())
    response.status_code = 200

    return response


@DB_API_V1.route("/<string:username>/<string:project>", methods=["POST"])
def create_user(username, project):
    """Create user username with random password and salt.

    :returns: HTTP Response
    """
    auth.admin_only()

    user = db.UsersDoc(username)
    passwd = user.create(project)
    response = jsonify(
        {
            "username": username,
            "project" : project,
            "password": passwd
        }
    )
    response.status_code = 200

    return response


@DB_API_V1.route("/<string:username>", methods=["DELETE"])
def delete_user(username):
    """Delete user username.

    :returns: HTTP Response
    """
    auth.admin_only()
    db.UsersDoc(username).delete()

    response = jsonify({"username": username, "status": "deleted"})
    response.status_code = 200

    return response
