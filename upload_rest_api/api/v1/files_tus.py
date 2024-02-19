"""Event handler for the /files_tus/v1 endpoint."""
import flask_tus_io
import werkzeug
from flask import Blueprint, abort

from upload_rest_api.authentication import current_user
from upload_rest_api.checksum import HASH_FUNCTION_ALIASES, get_file_checksums
from upload_rest_api.config import CONFIG
from upload_rest_api.jobs import UPLOAD_QUEUE, enqueue_background_job
from upload_rest_api.lock import lock_manager
from upload_rest_api.models.resource import Directory, File
from upload_rest_api.models.upload import Upload

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


def _release_lock(workspace):
    """Release file storage lock."""
    resource = workspace.get_resource()
    try:
        upload = Upload.get(id=resource.identifier)
        lock_manager.release(upload.project.id, upload.storage_path)
    except Upload.DoesNotExist:
        return


def _delete_workspace(workspace):
    """Delete the workspace."""
    workspace.remove()


def _upload_started(workspace, resource):
    """Callback function called when a new upload is started."""
    try:
        upload_type = resource.upload_metadata["type"]

        if upload_type == 'file':
            target_class = File
        elif upload_type == 'archive':
            target_class = Directory
        else:
            abort(400, f"Unknown upload type '{upload_type}'")

        if not current_user.is_allowed_to_access_project(
            resource.upload_metadata['project_id']
        ):
            abort(403)

        target = target_class(
            resource.upload_metadata['project_id'],
            resource.upload_metadata['upload_path']
        )
        Upload.create(target,
                      size=resource.upload_length,
                      identifier=resource.identifier,
                      is_tus_upload=True)
    except Exception:
        # Remove the workspace to prevent filling up disk space with
        # bogus requests
        _release_lock(workspace)
        _delete_workspace(workspace)
        raise


def _store_files(workspace, resource, upload_type, calculated_algorithm=None,
                 calculated_checksum=None):
    """Start the extraction job for an uploaded archive."""
    upload = Upload.get(id=resource.identifier)

    # Enqueue background job for storing archive, or store single file
    # right away. Source file verification can be skipped, because
    # it was verified before this method.
    if upload_type == 'archive':
        try:
            enqueue_background_job(
                task_func="upload_rest_api.jobs.upload.store_files",
                task_id=upload.id,
                queue_name=UPLOAD_QUEUE,
                project_id=upload.project.id,
                job_kwargs={
                    "identifier": upload.id,
                    "verify_source": False
                }
            )
        except Exception:
            # If we couldn't enqueue background job, release the lock
            _release_lock(workspace)
            raise
    else:
        upload.store_files(verify_source=False)


def _get_checksum_tuple(checksum):
    """
    Return a (algorithm, checksum) tuple from a "checksum" tus metadata value
    """
    if checksum is None:
        return None, None

    # The 'checksum' tus field has the syntax
    # '<algorithm>:<hex_checksum>'.
    try:
        algorithm, expected_checksum = checksum.split(":")
    except ValueError:
        abort(400, "Checksum does not follow '<alg>:<checksum>' syntax")

    if algorithm.lower() not in HASH_FUNCTION_ALIASES:
        abort(400, f"Unrecognized hash algorithm '{algorithm.lower()}'")

    return algorithm, expected_checksum


def _calculate_upload_checksum(resource, workspace, checksum):
    """
    Calculate the MD5 checksum and save it. If user also provided their own
    checksum, check the integrity of an upload by comparing the user provided
    checksum against the calculated checksum.
    """
    algorithms = set(["md5"])
    try:
        upload = Upload.get(id=resource.identifier)

        source_algorithm, expected_checksum = _get_checksum_tuple(checksum)
        if source_algorithm:
            algorithms.add(source_algorithm.lower())

        calculated_checksums = get_file_checksums(
            algorithms=algorithms,
            path=resource.upload_file_path
        )

        checksum_correct = (
            not source_algorithm
            or calculated_checksums[source_algorithm] == expected_checksum
        )
        if not checksum_correct:
            # User provided checksum but it didn't match what we calculated
            abort(400, "Upload checksum mismatch")

        # Save the MD5 checksum which we always calculate
        upload.add_source(
            resource.upload_file_path, checksum=calculated_checksums["md5"]
        )
    except Exception:
        _release_lock(workspace)
        _delete_workspace(workspace)
        raise


def _upload_completed(workspace, resource):
    """Callback function called when an upload is finished."""
    upload_type = resource.upload_metadata["type"]

    checksum = resource.upload_metadata.get("checksum", None)

    if upload_type not in ['file', 'archive']:
        raise werkzeug.exceptions.BadRequest(
            f"Unknown upload type '{upload_type}'"
        )

    if resource.bytes_uploaded >= CONFIG["UPLOAD_ASYNC_THRESHOLD_BYTES"]:
        # Perform checksum calculation asynchronously
        try:
            source_checksum_algorithm, source_checksum = _get_checksum_tuple(
                checksum
            )
            enqueue_background_job(
                task_func=(
                    "upload_rest_api.jobs.upload.calculate_upload_checksum"
                ),
                task_id=resource.identifier,
                queue_name=UPLOAD_QUEUE,
                project_id=resource.upload_metadata["project_id"],
                job_kwargs={
                    "identifier": resource.identifier,
                    "path": workspace.path,
                    "source_checksum_algorithm": source_checksum_algorithm,
                    "source_checksum": source_checksum,
                }
            )
        except Exception:
            # If we couldn't enqueue background job, release the lock
            _release_lock(workspace)
            raise
    else:
        # Perform checksum calculation synchronously
        _calculate_upload_checksum(
            resource=resource, workspace=workspace, checksum=checksum
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
    }

    if event_type in callbacks:
        callbacks[event_type](workspace=workspace, resource=resource)
