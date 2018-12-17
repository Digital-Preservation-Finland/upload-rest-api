"""REST api for uploading files into passipservice
"""
import os
from shutil import rmtree
import werkzeug

from flask import Flask, abort, safe_join, request, jsonify
from werkzeug.utils import secure_filename

import upload_rest_api.upload as up
import upload_rest_api.authentication as auth
import upload_rest_api.database as db
from upload_rest_api.dir_cleanup import readable_timestamp


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

    app.config["MAX_CONTENT_LENGTH"] = 50 * 1024**3

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
        if up.request_exceeds_quota(request):
            abort(413)
        elif request.content_length > app.config.get("MAX_CONTENT_LENGTH"):
            abort(413)

        username = request.authorization.username

        upload_path = app.config.get("UPLOAD_PATH")
        fpath, fname = os.path.split(fpath)
        fname = secure_filename(fname)
        username = secure_filename(username)

        fpath = safe_join(upload_path, username, fpath)

        # Create directory if it does not exist
        if not os.path.exists(fpath):
            os.makedirs(fpath, 0o700)

        fpath = safe_join(fpath, fname)

        response = up.save_file(request, fpath, upload_path)
        db.update_used_quota(request)

        return response


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

        return jsonify({
            "file_path": return_path,
            "md5": up.md5_digest(fpath),
            "timestamp": readable_timestamp(fpath)
        })


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
            db.update_used_quota(request)
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
        db.update_used_quota(request)

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


    @app.errorhandler(werkzeug.exceptions.HTTPException)
    def http_error(error):
        """General response handler for all werkzeug HTTPExceptions

        :returns: HTTP Response
        """
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code

        return response


    if __name__ == "__main__":
        app.run()
    else:
        return app


if __name__ == "__main__":
    create_app()
