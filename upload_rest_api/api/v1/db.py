"""REST api for uploading files into passipservice
"""
from flask import Blueprint, jsonify

import upload_rest_api.authentication as auth
import upload_rest_api.database as db
import upload_rest_api.utils as utils


DB_API_V1 = Blueprint("db_v1", __name__, url_prefix="/db/v1")


@DB_API_V1.route("", methods=["GET"])
def get_all_users():
    """Get list of all usernames from the database.

    :returns: HTTP Response
    """
    auth.admin_only()
    users = db.get_all_users()

    response = jsonify(dict(users=users))
    response.status_code = 200

    return response


@DB_API_V1.route("/<string:username>", methods=["GET"])
def get_user(username):
    """Get user username from the database.

    :returns: HTTP Response
    """
    auth.admin_only()
    user = db.UsersDoc(username)

    try:
        response = jsonify(user.get_utf8())
        response.status_code = 200
    except db.UserNotFoundError as error:
        return utils.make_response(404, str(error))

    return response


@DB_API_V1.route("/<string:username>/<string:project>", methods=["POST"])
def create_user(username, project):
    """Create user username with random password and salt.

    :returns: HTTP Response
    """
    auth.admin_only()
    user = db.UsersDoc(username)

    try:
        passwd = user.create(project)
    except db.UserExistsError as error:
        return utils.make_response(409, str(error))

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

    try:
        db.UsersDoc(username).delete()
    except db.UserNotFoundError as error:
        return utils.make_response(404, str(error))

    response = jsonify({"username": username, "status": "deleted"})
    response.status_code = 200

    return response
