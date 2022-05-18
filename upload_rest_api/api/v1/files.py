"""/files/v1 endpoints.

Functionality for uploading, querying and deleting files from the
server.
"""
import os
import secrets
import shutil

import metax_access
from flask import Blueprint, abort, current_app, jsonify, request, url_for

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
import upload_rest_api.upload as up
from upload_rest_api import utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
from upload_rest_api.authentication import current_user
from upload_rest_api.jobs.utils import FILES_QUEUE, enqueue_background_job
from upload_rest_api.lock import lock_manager

FILES_API_V1 = Blueprint("files_v1", __name__, url_prefix="/v1/files")


def _get_dir_tree(project_id, fpath):
    """Return with dir tree from fpath as a dict."""
    file_dict = {}
    for dirpath, _, files in os.walk(fpath):
        path = db.Projects.get_return_path(project_id, dirpath)
        file_dict[path] = files

    if "." in file_dict:
        file_dict["/"] = file_dict.pop(".")

    return file_dict


@FILES_API_V1.route("/<string:project_id>/<path:fpath>", methods=["POST"])
def upload_file(project_id, fpath):
    """Save the uploaded file at <UPLOAD_PROJECTS_PATH>/project/fpath.

    :returns: HTTP Response
    """
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permission to access this project")

    database = db.Database()
    try:
        rel_upload_path = utils.parse_relative_user_path(fpath)
    except ValueError:
        abort(404)

    up.validate_upload(
        project_id, request.content_length, request.content_type
    )

    md5 = up.save_file(database,
                       project_id,
                       request.stream,
                       request.args.get('md5', None),
                       rel_upload_path)

    return jsonify(
        {
            'file_path': f"/{rel_upload_path}",
            'md5': md5,
            'status': 'created'
        }
    )


@FILES_API_V1.route(
    "/<string:project_id>/", defaults={'fpath': ""}, methods=["GET"],
    strict_slashes=False
)
@FILES_API_V1.route(
    "/<string:project_id>/<path:fpath>", methods=["GET"]
)
def get_path(project_id, fpath):
    """Get filepath, name and checksum.

    :returns: HTTP Response
    """
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permission to access this project")

    database = db.Database()

    upload_path = db.Projects.get_upload_path(project_id, fpath)
    return_path = db.Projects.get_return_path(project_id, upload_path)

    if request.args.get("all", None) == "true" and fpath.strip("/") == "":
        # Retrieve entire directory listing if 'all' URL parameter is set
        # and fpath is set to '' or '/'
        fpath = db.Projects.get_project_directory(project_id)

        if not os.path.exists(fpath):
            abort(404, "No files found")

        response = jsonify(_get_dir_tree(project_id, fpath))
    elif os.path.isfile(upload_path):
        file_doc = database.files.get(str(upload_path))
        response = {
            "file_path": return_path,
            "identifier": file_doc.get("identifier", None),
            "md5": file_doc["checksum"],
            "timestamp": md.iso8601_timestamp(upload_path)
        }

    elif os.path.isdir(upload_path):
        metax = metax_access.Metax(
            url=current_app.config.get("METAX_URL"),
            user=current_app.config.get("METAX_USER"),
            password=current_app.config.get("METAX_PASSWORD"),
            verify=current_app.config.get("METAX_SSL_VERIFICATION")
        )
        try:
            identifier = metax.get_project_directory(
                project_id, return_path
            )['identifier']
        except metax_access.DirectoryNotAvailableError:
            identifier = None

        # Create a list of directories and files to avoid scanning the
        # directory twice
        entries = list(os.scandir(upload_path))

        response = {
            'identifier': identifier,
            'directories': [entry.name for entry in entries if entry.is_dir()],
            'files':  [entry.name for entry in entries if entry.is_file()]
        }

    else:
        abort(404, "File not found")

    return response


@FILES_API_V1.route(
    "/<string:project_id>", defaults={'fpath': ""}, methods=["DELETE"],
    strict_slashes=False
)
@FILES_API_V1.route("/<string:project_id>/<path:fpath>", methods=["DELETE"])
def delete_path(project_id, fpath):
    """Delete fpath under project.

    If fpath resolves to a directory, the whole directory is recursively
    removed.

    :returns: HTTP Response
    """
    if not current_user.is_allowed_to_access_project(project_id):
        abort(403, "No permission to access this project")

    root_upload_path = current_app.config.get("UPLOAD_PROJECTS_PATH")
    database = db.Database()
    upload_path = db.Projects.get_upload_path(project_id, fpath)
    project_dir = database.projects.get_project_directory(project_id)

    if os.path.isfile(upload_path):
        with lock_manager.lock(project_id, upload_path):
            # Remove metadata from Metax
            try:
                response = md.MetaxClient().delete_file_metadata(
                    project_id, upload_path, root_upload_path
                )
            except md.MetaxClientError as exception:
                response = str(exception)

            # Remove checksum and identifier from mongo
            database.files.delete_one(os.path.abspath(upload_path))
            os.remove(upload_path)

    elif upload_path.exists() \
            and upload_path.samefile(project_dir) \
            and not any(upload_path.iterdir()):
        # Trying to delete empty project directory
        abort(404, "No files found")

    elif os.path.isdir(upload_path):
        is_project_dir = upload_path.samefile(project_dir)

        # Create a random ID for the directory that will contain the files
        # and directories to delete. This is used to prevent potential race
        # conditions where the user creates and deletes a directory/file while
        # the previous directory/file is still being deleted.
        # TODO: This pattern could be implemented in a more generic manner and
        # for other purposes besides deletion. In short:
        #
        # 1. Create temporary directory with unique ID with the same structure
        #    as the actual project directory§
        # 2. Perform required operations (deletion, extraction) in the
        #    temporary directory
        # 3. Move the complete directory to the actual project directory
        #    atomically
        # 4. Delete the temporary directory
        trash_id = secrets.token_hex(8)

        trash_root = database.projects.get_trash_root(
            project_id=project_id,
            trash_id=trash_id
        )
        trash_path = database.projects.get_trash_path(
            project_id=project_id,
            trash_id=trash_id,
            file_path=fpath
        )
        # Acquire a lock *and* keep it alive even after this HTTP request.
        # It will be released by the 'delete_files' background job once it
        # finishes.
        lock_manager.acquire(project_id, upload_path)

        try:
            try:
                trash_path.parent.mkdir(exist_ok=True, parents=True)
                upload_path.rename(trash_path)
            except FileNotFoundError:
                # The directory to remove does not exist anymore;
                # other request managed to start deletion first.
                shutil.rmtree(trash_path.parent)
                abort(404, "No files found")

            if is_project_dir:
                # If we're deleting the entire project directory, create an
                # empty directory before proceeding with deletion
                project_dir.mkdir(exist_ok=True)

            # Remove all file metadata of files under fpath from Metax
            task_id = enqueue_background_job(
                task_func="upload_rest_api.jobs.files.delete_files",
                queue_name=FILES_QUEUE,
                project_id=project_id,
                job_kwargs={
                    "fpath": upload_path,
                    "trash_path": trash_path,
                    "trash_root": trash_root,
                    "project_id": project_id,
                }
            )
        except Exception:
            # If we couldn't enqueue background job, release the lock
            lock_manager.release(project_id, upload_path)
            raise

        polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
        response = jsonify({
            "file_path": db.Projects.get_return_path(project_id, upload_path),
            "message": "Deleting metadata",
            "polling_url": polling_url,
            "status": "pending"
        })
        location = url_for(TASK_STATUS_API_V1.name + ".task_status",
                           task_id=task_id)
        response.headers[b'Location'] = location
        response.status_code = 202
        return response

    else:
        abort(404, "File not found")

    database.projects.update_used_quota(project_id, root_upload_path)

    response = jsonify({
        "file_path": db.Projects.get_return_path(project_id, upload_path),
        "message": "deleted",
        "metax": response
    })
    response.status_code = 200

    return response
