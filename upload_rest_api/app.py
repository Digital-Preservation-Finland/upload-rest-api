"""REST api for uploading files into passipservice
"""
import os

from shutil import rmtree
from flask import Flask, abort, safe_join, request, jsonify
from werkzeug.utils import secure_filename

import upload as up
from authentication import authenticate


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
    app = Flask(__name__)

    app.config["UPLOAD_PATH"] = "/home/vagrant/test/rest"
    app.config["API_PATH"] = "/api/upload/v1"

    # Authenticate all requests
    app.before_request(authenticate)

    @app.route(
        "%s/<string:project>/<path:fpath>" % app.config.get("API_PATH"),
        methods=["POST"]
    )
    def upload_file(project, fpath):
        """Save file uploaded as multipart/form-data at
        /var/spool/uploads/organization/project/fpath

        :returns: HTTP Response
        """
        _file = request.files["file"]

        upload_path = app.config.get("UPLOAD_PATH")
        fpath, fname = os.path.split(fpath)
        fname = secure_filename(fname)
        project = secure_filename(project)

        fpath = safe_join(upload_path, project, fpath)

        # Create directory if it does not exist
        if not os.path.exists(fpath):
            os.makedirs(fpath, 0o700)

        fpath = safe_join(fpath, fname)

        return up.save_file(_file, fpath, upload_path)


    @app.route(
        "%s/<string:project>/<path:fpath>" % app.config.get("API_PATH"),
        methods=["GET"]
    )
    def get_file(project, fpath):
        """Get filepath, name and checksum.

        :returns: HTTP Response
        """
        fpath, fname = os.path.split(fpath)

        upload_path = app.config.get("UPLOAD_PATH")
        fname = secure_filename(fname)
        project = secure_filename(project)
        fpath = safe_join(upload_path, project, fpath, fname)

        if not os.path.isfile(fpath):
            abort(404)

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(upload_path):]

        return jsonify({"file_path": return_path, "md5": up.md5_digest(fpath)})


    @app.route(
        "%s/<string:project>/<path:fpath>" % app.config.get("API_PATH"),
        methods=["DELETE"]
    )
    def delete_file(project, fpath):
        """Get filepath, name and checksum.

        :returns: HTTP Response
        """
        fpath, fname = os.path.split(fpath)

        upload_path = app.config.get("UPLOAD_PATH")
        fname = secure_filename(fname)
        project = secure_filename(project)
        fpath = safe_join(upload_path, project, fpath, fname)

        if os.path.isfile(fpath):
            os.remove(fpath)
        else:
            abort(404)

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(upload_path):]

        return jsonify({"file_path": return_path, "status": "deleted"})


    @app.route(
        "%s/<string:project>" % app.config.get("API_PATH"),
        methods=["GET"]
    )
    def get_files(project):
        """Get all files under a project

        :return: HTTP Response
        """
        upload_path = app.config.get("UPLOAD_PATH")
        fpath = safe_join(upload_path, secure_filename(project))

        if not os.path.exists(fpath):
            abort(404)

        file_dict = {}
        for root, dirs, files in os.walk(fpath):
            file_dict[root[len(upload_path):]] = files

        response = jsonify(file_dict)
        response.status_code = 200

        return response


    @app.route(
        "%s/<string:project>" % app.config.get("API_PATH"),
        methods=["DELETE"]
    )
    def delete_files(project):
        """Delete all files under a project

        :returns: HTTPS Response
        """
        upload_path = app.config.get("UPLOAD_PATH")
        fpath = safe_join(upload_path, secure_filename(project))

        if not os.path.exists(fpath):
            abort(404)

        rmtree(fpath)

        response = jsonify({"fpath": fpath[len(upload_path):], "status": "deleted"})
        response.status_code = 200

        return response


    @app.errorhandler(401)
    def page_not_found(error):
        """JSON response handler for the 401 - Unauthorized errors

        :returns: HTTP Response
        """
        response = jsonify({"code": 401, "error": str(error)})
        response.status_code = 401

        return response


    @app.errorhandler(404)
    def page_not_found(error):
        """JSON response handler for the 404 - Not found errors

        :returns: HTTP Response
        """
        response = jsonify({"code": 404, "error": str(error)})
        response.status_code = 404

        return response


    @app.errorhandler(405)
    def method_not_allowed(error):
        """JSON response handler for the 405 - Method not allowed errors

        :returns: HTTP Response
        """
        response = jsonify({"code": 405, "error": str(error)})
        response.status_code = 405

        return response


    if __name__ == "__main__":
        app.run(debug=True)
    else:
        return app


if __name__ == "__main__":
    create_app()
