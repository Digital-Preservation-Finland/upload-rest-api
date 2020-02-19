"""REST api for querying upload status
"""
from __future__ import unicode_literals
import json

from flask import Blueprint, request, jsonify

import upload_rest_api.database as db

TASK_STATUS_API_V1 = Blueprint("tasks_v1", __name__,
                               url_prefix="/v1/tasks")


@TASK_STATUS_API_V1.route("/<task_id>", methods=["GET", "DELETE"])
def task_status(task_id):
    """Endpoint for querying the upload task status. Delete for deleting
    the upload task entry from mongo DB. When task is not in pending state
    it will be removed automatically in GET. Further queries will return 410.
    """
    task = db.AsyncTaskCol().get(task_id)
    if task is None:
        response = jsonify({"code": 410, "status": "Gone"})
        response.status_code = 410
        return response
    if request.method == "GET":
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
    elif request.method == "DELETE":
        db.AsyncTaskCol().delete_one(task_id)
        response = jsonify({"message": "deleted"})
    response.status_code = 200
    return response
