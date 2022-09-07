"""Event handler for the /files_tus/v1 endpoint."""
import flask_tus_io
import werkzeug
from flask import Blueprint, abort

from upload_rest_api.authentication import current_user
from upload_rest_api.checksum import (HASH_FUNCTION_ALIASES,
                                      calculate_incr_checksum)
from upload_rest_api.database import Database, Projects
from upload_rest_api.upload import Upload
from upload_rest_api.utils import parse_relative_user_path

FILES_TUS_API_V1 = Blueprint(
    "files_tus_v1", __name__, url_prefix="/v1/files_tus"
)


def register_blueprint(app):
    """
    Register the `flask_tus_io` application under the `/v1/files_tus`
    URL prefix
    """
    flask_tus_io.register_blueprints(
        app,
        url_prefixes=["/v1/files_tus"],
        event_handler=tus_event_handler
    )
    app.register_blueprint(FILES_TUS_API_V1)


def _delete_workspace(workspace):
    """Delete workspace and remove the corresponding database entry."""
    uploads = Database().uploads

    resource = workspace.get_resource()
    workspace.remove()
    uploads.delete_one(resource.identifier)


def _upload_started(workspace, resource):
    """Callback function called when a new upload is started."""
    try:
        db = Database()
        uploads = db.uploads

        upload_length = resource.upload_length
        project_id = resource.upload_metadata["project_id"]
        fpath = resource.upload_metadata["upload_path"]
        upload_type = resource.upload_metadata["type"]

        if upload_type not in ("file", "archive"):
            abort(400, f"Unknown upload type '{upload_type}'")

        if not current_user.is_allowed_to_access_project(project_id):
            abort(403)

        try:
            upload_path = parse_relative_user_path(fpath)
        except ValueError:
            abort(404)

        project_dir = Projects.get_project_directory(project_id)
        file_path = project_dir / upload_path

        project = db.projects.get(project_id)

        allocated_quota = uploads.get_project_allocated_quota(project_id)
        remaining_quota = (
            project["quota"]  # User's total quota
            - project["used_quota"]  # Finished and saved uploads
            - allocated_quota  # Disk space allocated for unfinished uploads
            # Disk space that will be allocated for this upload
            - upload_length
        )

        if remaining_quota < 0:
            # Remaining user quota too low to allow this upload
            abort(413, "Remaining user quota too low")

        # check if the file/dirctory exists:
        # either an upload has been initiated with
        # the same path, or a file already exists at the final location
        upload_exists = (
            file_path.exists()
            or db.uploads.uploads.find_one({"upload_path": str(file_path)})
        )

        if upload_exists:
            if upload_type == "file":
                error = "File already exists"
            elif upload_type == "archive":
                error = "Target directory already exists"

            abort(
                409,  # 409 CONFLICT
                error
            )

        # Quota is sufficient, create a new Upload entry
        uploads.create(
            project_id=project_id,
            upload_path=str(file_path),
            resource=resource
        )
    except Exception:
        # Remove the workspace to prevent filling up disk space with
        # bogus requests
        _delete_workspace(workspace)
        raise


def _store_files(workspace, resource, upload_type):
    """Start the extraction job for an uploaded archive."""
    project_id = resource.upload_metadata["project_id"]
    fpath = resource.upload_metadata["upload_path"]
    checksum = calculate_incr_checksum(algorithm='md5',
                                       path=resource.upload_file_path)

    try:
        upload_path = parse_relative_user_path(fpath)
    except ValueError:
        abort(404)

    try:
        upload = Upload(project_id, upload_path, upload_type=upload_type)
        upload.add_source(resource.upload_file_path,
                          resource.upload_length,
                          checksum,
                          verify=False)

    finally:
        # Delete the tus-specific workspace regardless of the
        # outcome.
        _delete_workspace(workspace)

    if upload_type == 'archive':
        upload.validate_archive()
        upload.enqueue_store_task()

    else:
        upload.store_files()


def _get_checksum_tuple(checksum):
    """
    Return a (algorithm, checksum) tuple from a "checksum" tus metadata value
    """
    # The 'checksum' tus field has the syntax
    # '<algorithm>:<hex_checksum>'.
    try:
        algorithm, expected_checksum = checksum.split(":")
    except ValueError:
        abort(400, "Checksum does not follow '<alg>:<checksum>' syntax")

    if algorithm.lower() not in HASH_FUNCTION_ALIASES:
        abort(400, f"Unrecognized hash algorithm '{algorithm.lower()}'")

    return algorithm, expected_checksum


def _chunk_upload_completed(workspace, resource):
    """
    Process the received chunk, calculating both the MD5 checksum and
    the optional user-provided algorithm incrementally
    """
    try:
        # Always calculate the MD5 checksum since that's what we'll
        # save into our database
        calculate_incr_checksum(
            algorithm="md5",
            path=resource.upload_file_path
        )

        checksum = resource.metadata.get("checksum", None)

        if not checksum or checksum.lower() == "md5":
            return

        algorithm, _ = _get_checksum_tuple(checksum)

        # Calculate the checksum up to the current end; the function
        # will save the current progress and resume where it left off
        # later.
        calculate_incr_checksum(
            algorithm=algorithm,
            path=resource.upload_file_path
        )
    except Exception:
        _delete_workspace(workspace)
        raise


def _check_upload_integrity(resource, workspace, checksum):
    """
    Check the integrity of an upload by comparing the user provided
    checksum against the calculated checksum
    """
    try:
        algorithm, expected_checksum = _get_checksum_tuple(checksum)

        calculated_checksum = calculate_incr_checksum(
            algorithm=algorithm,
            path=resource.upload_file_path,
            # Don't delete the progress if it's a MD5 checksum, as we'll
            # later use it for the checksum in our database
            finalize=bool(checksum != "md5")
        )

        if calculated_checksum != expected_checksum:
            abort(400, "Upload checksum mismatch")
    except Exception:
        _delete_workspace(workspace)
        raise


def _upload_completed(workspace, resource):
    """Callback function called when an upload is finished."""
    upload_type = resource.upload_metadata["type"]

    checksum = resource.upload_metadata.get("checksum", None)

    if checksum:
        _check_upload_integrity(
            resource=resource, workspace=workspace, checksum=checksum
        )

    if upload_type not in ['file', 'archive']:
        raise werkzeug.exceptions.BadRequest(
            f"Unknown upload type '{upload_type}'"
        )

    _store_files(workspace, resource, upload_type)


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
        "chunk-upload-completed": _chunk_upload_completed,
    }

    if event_type in callbacks:
        callbacks[event_type](workspace=workspace, resource=resource)
