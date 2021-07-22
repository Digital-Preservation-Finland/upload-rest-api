"""
Event handler for the /files_tus/v1 endpoint.
"""
from flask import Blueprint, abort, request, safe_join
from upload_rest_api import database, upload
from upload_rest_api.upload import save_file_into_db
from upload_rest_api.database import Database, Uploads

import flask_tus_io

FILES_TUS_API_V1 = Blueprint(
    "files_tus_v1", __name__, url_prefix="/v1/files_tus"
)


def register_blueprint(app):
    """
    Register the `flask_tus_io` application under the `/v1/files_tus` URL
    prefix
    """
    flask_tus_io.register_blueprints(
        app,
        url_prefixes=["/v1/files_tus"],
        event_handler=tus_event_handler
    )
    app.register_blueprint(FILES_TUS_API_V1)


def _upload_started(workspace, resource):
    """
    Callback function called when a new upload is started
    """
    try:
        db = database.Database()
        user = db.user(request.authorization.username)
        uploads = db.uploads

        upload_length = resource.upload_length
        fpath = resource.upload_metadata["file_path"]

        upload_path = safe_join("", fpath)
        file_path = user.project_directory / upload_path

        allocated_quota = uploads.get_user_allocated_quota(user)
        remaining_quota = (
            user.get_quota()  # User's total quota
            - user.get_used_quota()  # Finished and saved uploads
            - allocated_quota  # Disk space allocated for unfinished uploads
            - upload_length  # Disk space that will be allocated for this upload
        )

        if remaining_quota < 0:
            # Remaining user quota too low to allow this upload
            abort(413, "Remaining user quota too low")

        # Validate the user's quota and content type is not exceeded again.
        upload.validate_upload(
            user=user,
            content_length=resource.upload_length,
            content_type="application/octet-stream"
        )

        # Check if the file exists: either an upload has been initiated with the
        # same path, or a file already exists at the final location
        file_exists = (
            file_path.exists()
            or db.uploads.uploads.find_one({"file_path": str(file_path)})
        )

        if file_exists:
            abort(
                409,  # 409 CONFLICT
                "File already exists"
            )

        # Quota is sufficient, create a new Upload entry
        uploads.create(
            user=user,
            file_path=str(file_path),
            resource=resource
        )
    except Exception:
        # Remove the workspace to prevent filling up disk space with bogus
        # requests
        workspace.remove()
        raise


def _upload_completed(workspace, resource):
    """
    Callback function called when an upload is finished
    """
    db = database.Database()
    uploads = db.uploads
    user = db.user(request.authorization.username)

    fpath = resource.upload_metadata["file_path"]

    upload_path = safe_join("", fpath)
    file_path = user.project_directory / upload_path

    try:
        # Validate the user's quota and content type again
        upload.validate_upload(
            user=user,
            content_length=resource.upload_length,
            content_type="application/octet-stream"
        )

        # Upload passed validation, move it to the actual file storage
        resource.upload_file_path.rename(file_path)
    finally:
        # Delete the tus-specific workspace regardless of the outcome.
        workspace.remove()
        uploads.delete_one(resource.identifier)

    # Use `save_file_into_db` to handle the rest using the same code path
    # as `/v1/files` API
    save_file_into_db(
        file_path=file_path,
        database=db,
        user=user
    )


def tus_event_handler(event_type, workspace, resource):
    """
    Call the corresponding callback function if available for the given
    event type.

    :param event_type: Event type
    :param workspace: tus workspace
    :param resource: tus resource
    """
    callbacks = {
        "upload-started": _upload_started,
        "upload-completed": _upload_completed,
    }

    if event_type in callbacks:
        callbacks[event_type](workspace=workspace, resource=resource)
