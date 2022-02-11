"""
Event handler for the /files_tus/v1 endpoint.
"""
import os

import flask_tus_io
from flask import Blueprint, abort, current_app, safe_join

from upload_rest_api import database, upload
from upload_rest_api.authentication import current_user
from upload_rest_api.database import Database, Projects
from upload_rest_api.jobs.utils import METADATA_QUEUE, enqueue_background_job
from upload_rest_api.lock import lock_manager
from upload_rest_api.upload import save_file_into_db

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

        upload_path = safe_join("", fpath)
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


def _upload_completed(workspace, resource):
    """
    Callback function called when an upload is finished
    """
    db = database.Database()
    uploads = db.uploads

    project_id = resource.upload_metadata["project_id"]
    fpath = resource.upload_metadata["file_path"]
    create_metadata = \
        resource.upload_metadata.get("create_metadata", "") == "true"

    upload_path = safe_join("", fpath)
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
