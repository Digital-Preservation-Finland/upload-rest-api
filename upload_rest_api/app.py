"""REST api for uploading files into passipservice
"""
import os
from shutil import rmtree

from flask import Flask, abort, safe_join, request, jsonify
from werkzeug.utils import secure_filename

import upload_rest_api.upload as up
import upload_rest_api.authentication as auth
import upload_rest_api.database as db


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
    app = Flask(__name__)

    app.config["UPLOAD_PATH"] = "/home/vagrant/test/rest"
    app.config["UPLOAD_API_PATH"] = "/api/upload/v1"
    app.config["DB_API_PATH"] = "/api/db/v1"

    app.config["MONGO_HOST"] = "localhost"
    app.config["MONGO_PORT"] = 27017

    # Authenticate all requests
    app.before_request(auth.authenticate)

    @app.route(
        "%s/<path:fpath>" % app.config.get("UPLOAD_API_PATH"),
        methods=["POST"]
    )
    def upload_file(fpath):
        """Save file uploaded as multipart/form-data at
        /var/spool/uploads/user/fpath

        :returns: HTTP Response
        """
        _file = request.files["file"]
        username = request.authorization.username

        upload_path = app.config.get("UPLOAD_PATH")
        fpath, fname = os.path.split(fpath)
        fname = secure_filename(fname)
        user = secure_filename(username)

        fpath = safe_join(upload_path, user, fpath)

        # Create directory if it does not exist
        if not os.path.exists(fpath):
            os.makedirs(fpath, 0o700)

        fpath = safe_join(fpath, fname)

        return up.save_file(_file, fpath, upload_path)


    @app.route(
        "%s/<path:fpath>" % app.config.get("UPLOAD_API_PATH"),
        methods=["GET"]
    )
    def get_file(fpath):
        """Get filepath, name and checksum.

        :returns: HTTP Response
        """
        fpath, fname = os.path.split(fpath)
        username = request.authorization.username

        upload_path = app.config.get("UPLOAD_PATH")
        fname = secure_filename(fname)
        user = secure_filename(username)
        fpath = safe_join(upload_path, user, fpath, fname)

        if not os.path.isfile(fpath):
            abort(404)

        # Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(upload_path):]

        return jsonify({"file_path": return_path, "md5": up.md5_digest(fpath)})


    @app.route(
        "%s/<path:fpath>" % app.config.get("UPLOAD_API_PATH"),
        methods=["DELETE"]
    )
    def delete_file(fpath):
        """Get filepath, name and checksum.

        :returns: HTTP Response
        """
        fpath, fname = os.path.split(fpath)
        username = request.authorization.username

        upload_path = app.config.get("UPLOAD_PATH")
        fname = secure_filename(fname)
        user = secure_filename(username)
        fpath = safe_join(upload_path, user, fpath, fname)

        if os.path.isfile(fpath):
            os.remove(fpath)
        else:
            abort(404)

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(upload_path):]

        return jsonify({"file_path": return_path, "status": "deleted"})


    @app.route(
        "%s" % app.config.get("UPLOAD_API_PATH"),
        methods=["GET"]
    )
    def get_files():
        """Get all files of the user

        :return: HTTP Response
        """
        username = request.authorization.username
        upload_path = app.config.get("UPLOAD_PATH")
        fpath = safe_join(upload_path, secure_filename(username))

        if not os.path.exists(fpath):
            abort(404)

        file_dict = {}
        for root, dirs, files in os.walk(fpath):
            file_dict[root[len(upload_path):]] = files

        response = jsonify(file_dict)
        response.status_code = 200

        return response


    @app.route(
        "%s" % app.config.get("UPLOAD_API_PATH"),
        methods=["DELETE"]
    )
    def delete_files():
        """Delete all files of a user

        :returns: HTTP Response
        """
        username = request.authorization.username
        upload_path = app.config.get("UPLOAD_PATH")
        fpath = safe_join(upload_path, secure_filename(username))

        if not os.path.exists(fpath):
            abort(404)

        rmtree(fpath)

        response = jsonify({"fpath": fpath[len(upload_path):], "status": "deleted"})
        response.status_code = 200

        return response


    @app.route(
        "%s/<string:username>" % app.config.get("DB_API_PATH"),
        methods=["GET"]
    )
    def get_user(username):
        """Get user username from the database.

        :returns: HTTP Response
        """
        auth.admin_only()

        user = db.User(username)
        response = jsonify(user.get_utf8())
        response.status_code = 200

        return response


    @app.route(
        "%s/<string:username>" % app.config.get("DB_API_PATH"),
        methods=["POST"]
    )
    def create_user(username):
        """Create user username with random password and salt.

        :returns: HTTP Response
        """
        auth.admin_only()

        user = db.User(username)
        passwd = user.create()

        response = jsonify({"username": username, "password": passwd})
        response.status_code = 200

        return response


    @app.route(
        "%s/<string:username>" % app.config.get("DB_API_PATH"),
        methods=["DELETE"]
    )
    def delete_user(username):
        """Delete user username.

        :returns: HTTP Response
        """
        auth.admin_only()
        db.User(username).delete()

        response = jsonify({"username": username, "status": "deleted"})
        response.status_code = 200

        return response


    @app.errorhandler(401)
    def unauthorized_error(error):
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
