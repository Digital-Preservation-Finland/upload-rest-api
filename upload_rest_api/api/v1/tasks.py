"""REST api for querying upload status."""
from urllib.parse import urlparse, urlunparse
from flask import Blueprint, jsonify, url_for, request

import upload_rest_api.database as db

TASK_STATUS_API_V1 = Blueprint("tasks_v1", __name__,
                               url_prefix="/v1/tasks")


def get_polling_url(task_id):
    """Create url used to poll the status of asynchronous request.

    :param task_id: task identifier
    """
    path = url_for(TASK_STATUS_API_V1.name + ".task_status", task_id=task_id)
    parsed_url = urlparse(request.url)
    return urlunparse([parsed_url[0], parsed_url[1], path, "", "", ""])


def _create_gone_response():
    """Creates a response telling that task has completed and status
    information is not available anymore.
    """
    response = jsonify({"code": 404, "status": "Not found"})
    response.status_code = 404
    return response


@TASK_STATUS_API_V1.route("/<task_id>", methods=["GET"])
def task_status(task_id):
    """Endpoint for querying the upload task status.

    When task is not in pending state it will be removed automatically
    in GET. Further queries will return 404.
    """
    tasks = db.Database().tasks
    task = tasks.get(task_id)
    if task is None:
        response = _create_gone_response()

    else:
        content = {'status': task["status"]}
        if 'message' in task:
            content["message"] = task["message"]
        if 'errors' in task:
            content['errors'] = task['errors']
        response = jsonify(content)

        if task["status"] != "pending":
            tasks.delete_one(task_id)

    return response


@TASK_STATUS_API_V1.route("/<task_id>", methods=["DELETE"])
def task_delete(task_id):
    """Endpoint for deleting the upload task entry from mongo DB.

    Further queries will return 404.
    """
    tasks = db.Database().tasks
    task = tasks.get(task_id)
    if task is None:
        return _create_gone_response()

    tasks.delete_one(task_id)
    response = jsonify({"message": "deleted"})
    response.status_code = 200

    return response
