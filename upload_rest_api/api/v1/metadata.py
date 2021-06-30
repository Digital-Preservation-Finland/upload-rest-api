"""REST api for uploading files into passipservice."""
import os

from flask import Blueprint, jsonify, request, current_app, url_for

import upload_rest_api.database as db
import upload_rest_api.utils as utils
from upload_rest_api.api.v1.tasks import TASK_STATUS_API_V1
from upload_rest_api.jobs.utils import enqueue_background_job, METADATA_QUEUE


METADATA_API_V1 = Blueprint("metadata_v1", __name__, url_prefix="/v1/metadata")


@METADATA_API_V1.route("/<path:fpath>", methods=["POST"])
def post_metadata(fpath):
    """POST file metadata to Metax.

    A background task is launched to run the job. The ``Location``
    header and the body of the response contain the URL to be used for
    polling the status of the task. Status code is set to HTTP
    202(Accepted).

    :returns: HTTP Response
    """
    username = request.authorization.username
    user = db.Database().user(username)
    file_path = utils.get_upload_path(user, fpath)

    storage_id = current_app.config.get("STORAGE_ID")
    task_id = enqueue_background_job(
        task_func="upload_rest_api.jobs.metadata.post_metadata",
        queue_name=METADATA_QUEUE,
        username=username,
        job_kwargs={
            "path": fpath,
            "username": username,
            "storage_id": storage_id
        }
    )

    polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
    ret_path = utils.get_return_path(user, file_path)
    response = jsonify({
        "file_path": ret_path,
        "message": "Creating metadata",
        "polling_url": polling_url,
        "status": "pending"
    })
    location = url_for(TASK_STATUS_API_V1.name + ".task_status",
                       task_id=task_id)
    response.headers[b'Location'] = location
    response.status_code = 202

    return response


@METADATA_API_V1.route("/<path:fpath>", methods=["DELETE"])
def delete_metadata(fpath):
    """Delete fpath metadata under user's project.

    A background task is launched to run the job. If fpath resolves to a
    directory metadata is recursively removed all the files under the
    directory. The ``Location`` header and the body of the response
    contain the URL to be used for polling the status of the task.
    Status code is set to HTTP 202(Accepted).

    :returns: HTTP Response
    """
    username = request.authorization.username
    user = db.Database().user(username)
    file_path = utils.get_upload_path(user, fpath)

    task_id = enqueue_background_job(
        task_func="upload_rest_api.jobs.metadata.delete_metadata",
        queue_name=METADATA_QUEUE,
        username=username,
        job_kwargs={
            "fpath": fpath,
            "username": username
        }
    )

    polling_url = utils.get_polling_url(TASK_STATUS_API_V1.name, task_id)
    ret_path = utils.get_return_path(user, file_path)
    response = jsonify({
        "file_path": ret_path,
        "message": "Deleting metadata",
        "polling_url": polling_url,
        "status": "pending"
    })
    location = url_for(TASK_STATUS_API_V1.name + ".task_status",
                       task_id=task_id)
    response.headers[b'Location'] = location
    response.status_code = 202

    return response
