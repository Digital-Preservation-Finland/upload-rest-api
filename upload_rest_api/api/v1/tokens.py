"""
REST API for creating and listing tokens.

All API endpoints require admin access, which is only used by
dpres-admin-rest-api.
"""
import datetime

import dateutil.parser
from flask import Blueprint, abort, jsonify, request

from upload_rest_api.authentication import current_user
from upload_rest_api.models.token import Token
from upload_rest_api.models.user import User

TOKEN_API_V1 = Blueprint("tokens_v1", __name__, url_prefix="/v1/tokens")

SESSION_TOKEN_PERIOD = datetime.timedelta(hours=1)


@TOKEN_API_V1.route("/create", methods=["POST"])
def create_token():
    """
    Create token for a given user and projects
    """
    name = request.form.get("name", None)
    if not name:
        abort(400, "'name' is required")

    if len(name) > 1024:
        abort(400, "'name' maximum length is 1024 characters")

    username = request.form.get("username", None)
    if not username:
        abort(400, "'username' is required")

    if not current_user.admin:
        abort(403, "User does not have permission to create tokens")

    projects = request.form.get("projects", None)
    if projects is None:
        abort(400, "'projects' is required")

    projects = projects.split(",")

    expiration_date = request.form.get("expiration_date", None)

    now = datetime.datetime.now(datetime.timezone.utc)

    if expiration_date:
        expiration_date = dateutil.parser.parse(expiration_date)

    if expiration_date and expiration_date < now:
        abort(400, "'expiration_date' has already expired")

    data = Token.create(
        name=name,
        username=username,
        projects=projects,
        expiration_date=expiration_date
    )

    return jsonify({
        "identifier": data["_id"],
        "token": data["token"]
    })


@TOKEN_API_V1.route("/create_session", methods=["POST"])
def create_session_token():
    """
    Create temporary session token for a given user with access to all
    projects
    """
    username = request.form.get("username", None)
    if not username:
        abort(400, "'username' is required")

    if not current_user.admin:
        abort(403, "User does not have permission to create tokens")

    expiration_date = \
        datetime.datetime.now(tz=datetime.timezone.utc) + SESSION_TOKEN_PERIOD

    try:
        user = User.get(username=username)
    except User.DoesNotExist:
        # Create the user automatically if one doesn't exist.
        # Since fddps-frontend tries to create a session token immediately
        # this ensures the user can be managed after they have logged in
        # at least once.
        # Administrator can later create and/or grant any needed projects;
        # by default the user cannot do anything.
        user = User.create(username)

    result = Token.create(
        name=f"{username} session token",
        username=username,
        projects=[project.id for project in user.projects],
        session=True,
        expiration_date=expiration_date
    )

    return jsonify({
        "identifier": result["_id"],
        "token": result["token"]
    })


@TOKEN_API_V1.route("/list", methods=["GET"])
def list_tokens():
    """
    List tokens for the given user
    """
    username = request.args.get("username", current_user.username)

    if not current_user.admin:
        abort(403, "User does not have permission to list tokens")

    # Retrieve all tokens except for session tokens, which are not meant
    # to be visible for the user
    tokens = User.get(username=username).tokens
    token_entries = []

    # Strip token hash from the results and rename '_id' field
    for token in tokens:
        entry = {
            "identifier": token.id,
            "name": token.name,
            "username": token.username,
            "projects": [project.id for project in token.projects],
            "admin": token.admin,
            "session": token.session,
            "expiration_date": token.expiration_date
        }

        if token.expiration_date:
            entry["expiration_date"] = token.expiration_date.isoformat()

        token_entries.append(entry)

    return jsonify({
        "tokens": token_entries
    })


@TOKEN_API_V1.route("/", methods=["DELETE"])
def delete_token():
    """
    Delete token by its ID
    """
    username = request.form.get("username", None)

    if not username:
        abort(400, "'username' not provided")

    if not current_user.admin:
        abort(403, "User does not have permission to delete tokens")

    # Token ID != token. Token ID is used to identify a token so that user
    # can revoke it, even if they no longer have access to it.
    token_id = request.form.get("token_id", None)

    if not token_id:
        abort(400, "'token_id' not provided")

    try:
        Token.get(id=token_id).delete()
        return jsonify({"deleted": True})
    except Token.DoesNotExist:
        abort(404, "Token not found")
