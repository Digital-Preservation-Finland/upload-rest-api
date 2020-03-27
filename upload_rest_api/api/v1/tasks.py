"""REST api for querying upload status
"""
from __future__ import unicode_literals
import json

from flask import Blueprint, jsonify

import upload_rest_api.database as db

TASK_STATUS_API_V1 = Blueprint("tasks_v1", __name__,
                               url_prefix="/v1/tasks")


def _create_gone_response():
    """Creates a response telling that task has completed and status
    information is not available anymore"""
    response = jsonify({"code": 404, "status": "Not found"})
    response.status_code = 404
    return response


@TASK_STATUS_API_V1.route("/<task_id>", methods=["GET"])
def task_status(task_id):
    """Endpoint for querying the upload task status. When task is not in
    pending state it will be removed automatically in GET. Further queries
    will return 404.
    """
    task = db.AsyncTaskCol().get(task_id)
    if task is None:
        return _create_gone_response()
    if "message" in task:
        try:
            json_object = json.loads(task["message"])
            json_object["status"] = task["status"]
            response = jsonify(json_object)
        except ValueError:
            response = jsonify({'status': task["status"],
                                "message": task["message"]})
    else:
        response = jsonify({'status': task["status"]})
    if task["status"] != "pending":
        db.AsyncTaskCol().delete_one(task_id)
    response.status_code = 200
    return response


@TASK_STATUS_API_V1.route("/<task_id>", methods=["DELETE"])
def task_delete(task_id):
    """Endpoint for deleting the upload task entry from mongo DB. Further
    queries will return 404.
    """
    task = db.AsyncTaskCol().get(task_id)
    if task is None:
        return _create_gone_response()
    db.AsyncTaskCol().delete_one(task_id)
    response = jsonify({"message": "deleted"})
    response.status_code = 200
    return response
