"""Upload module background jobs."""
from flask_tus_io.workspace import Workspace

from upload_rest_api.checksum import get_file_checksums
from upload_rest_api.jobs.utils import ClientError, api_background_job
from upload_rest_api.lock import ProjectLockManager
from upload_rest_api.models.upload import Upload, UploadError, UploadType


@api_background_job
def calculate_upload_checksum(
        identifier: str, path: str,
        source_checksum_algorithm=None, source_checksum=None,
        task=None):
    """Calculate upload checksum. If source checksum was provided, also
    verify it. Afterwards, complete the upload as with `store_files`.

    This background job is only used if the upload is 1 GB or bigger; this is
    done to ensure that user facing HTTP requests complete quickly and do not
    hog the WSGI app server.

    :param identifier: Identifier of upload
    :param path: Path to the tus upload workspace
    :param source_checksum_algorithm: Optional source checksum algorithm
                                      provided by uploader
    :param source_checksum: Optional source checksum provided by uploader
    """
    workspace = Workspace(path)
    resource = workspace.get_resource()

    upload = Upload.get(id=identifier)

    lock_manager = ProjectLockManager()

    algorithms = set(["md5"])

    if source_checksum_algorithm is not None:
        source_checksum_algorithm = source_checksum_algorithm.lower()
        algorithms.add(source_checksum_algorithm)

    try:
        task.set_fields(message="Calculating checksum")

        checksums = get_file_checksums(algorithms, resource.upload_file_path)
        md5_checksum = checksums["md5"]

        checksum_correct = (
            not source_checksum_algorithm
            or checksums[source_checksum_algorithm] == source_checksum
        )

        if not checksum_correct:
            # User provided checksum but it didn't match
            raise ClientError("Upload checksum mismatch")
    except Exception:
        workspace.remove()
        lock_manager.release(upload.project.id, upload.storage_path)
        raise

    # Finalize the tus upload and remove the tus workspace;
    # the rest of the upload
    # (extraction, moving into pre-ingest file storage,
    # Metax metadata generation) will be handled by the Upload model
    upload.add_source(resource.upload_file_path, checksum=md5_checksum)

    workspace.remove()

    return _store_files(
        identifier=identifier, verify_source=False, task=task
    )


@api_background_job
def store_files(identifier, verify_source, task):
    """Store files.

    Create metadata for uploaded files and move them to storage.

    :param str identifier: identifier of upload
    :param str task: Task instance
    """
    return _store_files(
        identifier=identifier, verify_source=verify_source, task=task
    )


def _store_files(identifier, verify_source, task):
    """Store files.

    Create metadata for uploaded files and move them to storage.

    :param str identifier: identifier of upload
    :param str task: Task instance
    """
    upload = Upload.get(id=identifier)

    if upload.type_ == UploadType.ARCHIVE:
        message = "Extracting archive"
    else:
        message = f"Creating metadata /{upload.path}"

    task.set_fields(message=message)

    try:
        upload.store_files(verify_source)
    except UploadError as error:
        raise ClientError(str(error), error.files) from error

    return f"{upload.type_.value} uploaded to {upload.path}"
