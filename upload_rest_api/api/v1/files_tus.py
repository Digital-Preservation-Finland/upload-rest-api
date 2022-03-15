"""
Event handler for the /files_tus/v1 endpoint.
"""
import os
import uuid
from pathlib import Path

import flask_tus_io
import werkzeug
from flask import Blueprint, abort, current_app, safe_join

from upload_rest_api import database, upload
from upload_rest_api.authentication import current_user
from upload_rest_api.database import Database, Projects
from upload_rest_api.jobs.utils import METADATA_QUEUE, enqueue_background_job
from upload_rest_api.lock import lock_manager
from upload_rest_api.upload import extract_archive, save_file_into_db
from upload_rest_api.utils import parse_relative_user_path

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
        db = Database()
        uploads = db.uploads

        upload_length = resource.upload_length
        project_id = resource.upload_metadata["project_id"]
        fpath = resource.upload_metadata["file_path"]

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

        # Validate the user's quota and content type is not exceeded again.
        upload.validate_upload(
            project_id=project_id,
            content_length=resource.upload_length,
            content_type="application/octet-stream"
        )

        # check if the file exists: either an upload has been initiated with
        # the same path, or a file already exists at the final location
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
            project_id=project_id,
            file_path=str(file_path),
            resource=resource
        )
    except Exception:
        # Remove the workspace to prevent filling up disk space with bogus
        # requests
        workspace.remove()
        raise


def _save_file(workspace, resource):
    """
    Save file to the project directory
    """
    db = database.Database()
    uploads = db.uploads

    project_id = resource.upload_metadata["project_id"]
    fpath = resource.upload_metadata["file_path"]
    create_metadata = \
        resource.upload_metadata.get("create_metadata", "") == "true"

    try:
        upload_path = parse_relative_user_path(fpath)
    except ValueError:
        abort(404)

    project_dir = Projects.get_project_directory(project_id)
    file_path = project_dir / upload_path

    lock_manager.acquire(project_id, file_path)

    try:
        try:
            # Validate the user's quota and content type again
            upload.validate_upload(
                project_id=project_id,
                content_length=resource.upload_length,
                content_type="application/octet-stream"
            )

            # Ensure the parent directories exist
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Upload passed validation, move it to the actual file storage
            resource.upload_file_path.rename(file_path)
            # g+w required for siptools-research
            os.chmod(file_path, 0o664)
        finally:
            # Delete the tus-specific workspace regardless of the outcome.
            workspace.remove()
            uploads.delete_one(resource.identifier)

        # Use `save_file_into_db` to handle the rest using the same code path
        # as `/v1/files` API
        save_file_into_db(
            file_path=file_path,
            database=db,
            project_id=project_id
        )

        if create_metadata:
            # If enabled, enqueue background job to create Metax metadata
            storage_id = current_app.config.get("STORAGE_ID")
            enqueue_background_job(
                task_func="upload_rest_api.jobs.metadata.post_metadata",
                queue_name=METADATA_QUEUE,
                project_id=project_id,
                job_kwargs={
                    "path": fpath,
                    "project_id": project_id,
                    "storage_id": storage_id
                }
            )
        else:
            # Don't hold the lock since we're not generating metadata
            lock_manager.release(project_id, file_path)
    except Exception:
        lock_manager.release(project_id, file_path)
        raise


def _extract_archive(workspace, resource):
    """
    Start the extraction job for an uploaded archive
    """
    db = database.Database()
    uploads = db.uploads

    project_id = resource.upload_metadata["project_id"]
    fpath = resource.upload_metadata["file_path"]
    extract_dir_name = resource.upload_metadata["extract_dir_name"]
    create_metadata = \
        resource.upload_metadata.get("create_metadata", "") == "true"

    # 'fpath' contains the archive file name as the last path component.
    # We will replace it with the actual directory that will contain the
    # extracted files.
    project_dir = Projects.get_project_directory(project_id)
    rel_upload_path = safe_join("", os.path.split(fpath)[0], extract_dir_name)
    upload_path = project_dir / rel_upload_path

    lock_manager.acquire(project_id, upload_path)

    try:
        try:
            # Validate the user's quota and content type again
            upload.validate_upload(
                project_id=project_id,
                content_length=resource.upload_length,
                content_type="application/octet-stream"
            )

            if upload_path.is_dir() and not upload_path.samefile(project_dir):
                raise werkzeug.exceptions.Conflict(
                    f"Directory '{rel_upload_path}' already exists"
                )

            # Move the archive to a temporary path to begin the extraction
            tmp_path = Path(current_app.config.get("UPLOAD_TMP_PATH"))
            fpath = tmp_path / str(uuid.uuid4())
            fpath.parent.mkdir(exist_ok=True)
            resource.upload_file_path.rename(fpath)
        finally:
            # Delete the tus-specific workspace regardless of the outcome.
            workspace.remove()
            uploads.delete_one(resource.identifier)

        extract_archive(
            database=db,
            project_id=project_id,
            fpath=fpath,
            upload_path=rel_upload_path,
            create_metadata=create_metadata
        )
    except Exception:
        lock_manager.release(project_id, upload_path)
        raise


def _upload_completed(workspace, resource):
    """
    Callback function called when an upload is finished
    """
    upload_type = resource.upload_metadata["type"]

    if upload_type == "file":
        _save_file(workspace, resource)
    elif upload_type == "archive":
        _extract_archive(workspace, resource)
    else:
        raise werkzeug.exceptions.BadRequest(
            f"Unknown upload type '{upload_type}'"
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
