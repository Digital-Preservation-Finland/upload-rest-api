"""REST api for uploading files into passipservice
"""
import os
import json
from shutil import rmtree
from ConfigParser import ConfigParser

from flask import Flask, abort, safe_join, request, jsonify
import werkzeug
from werkzeug.utils import secure_filename

import upload_rest_api.upload as up
import upload_rest_api.authentication as auth
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as gen_metadata


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
    app = Flask(__name__)

    # API paths
    app.config["UPLOAD_PATH"] = "/home/vagrant/test/rest"
    app.config["UPLOAD_API_PATH"] = "/api/upload/v1"
    app.config["DB_API_PATH"] = "/api/db/v1"
    app.config["METADATA_API_PATH"] = "/api/gen_metadata/v1"

    # Mongo params
    app.config["MONGO_HOST"] = "localhost"
    app.config["MONGO_PORT"] = 27017

    # Storage params
    app.config["STORAGE_ID"] = "urn:uuid:f843c26d-b5f7-4c66-91e7-2e75f5377636"
    app.config["MAX_CONTENT_LENGTH"] = 50 * 1024**3

    # Metax params
    conf = ConfigParser()
    conf.read("/etc/siptools_research.conf")

    app.config["METAX_USER"] = conf.get(
        "siptools_research", "metax_user"
    )
    app.config["METAX_URL"] = conf.get(
        "siptools_research", "metax_url"
    )
    app.config["METAX_PASSWORD"] = conf.get(
        "siptools_research", "metax_password"
    )

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
        if request.content_length > app.config.get("MAX_CONTENT_LENGTH"):
            abort(413, "Max single file size exceeded")
        elif up.request_exceeds_quota():
            abort(413, "Personal quota exceeded")

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

        response = up.save_file(fpath, upload_path)
        db.update_used_quota()

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
            abort(404, "File not found")

        # Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(upload_path):]

        return jsonify({
            "file_path": return_path,
            "md5": up.md5_digest(fpath),
            "timestamp": gen_metadata.iso8601_timestamp(fpath)
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
            db.update_used_quota()
        else:
            abort(404, "File not found")

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
            abort(404, "No files found")

        file_dict = {}
        for root, _, files in os.walk(fpath):
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
            abort(404, "No files found")

        rmtree(fpath)
        db.update_used_quota()

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
        "%s/<string:username>/<string:project>" % app.config.get("DB_API_PATH"),
        methods=["POST"]
    )
    def create_user(username, project):
        """Create user username with random password and salt.

        :returns: HTTP Response
        """
        auth.admin_only()

        user = db.User(username)
        passwd = user.create(project)
        response = jsonify(
            {
                "username": username,
                "project" : project,
                "password": passwd
            }
        )
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


    @app.route(
        "%s/<path:fpath>" % app.config.get("METADATA_API_PATH"),
        methods=["POST"]
    )
    def post_metadata(fpath):
        """Delete user username.

        :returns: HTTP Response
        """
        fpath, fname = os.path.split(fpath)
        username = request.authorization.username

        upload_path = app.config.get("UPLOAD_PATH")
        fname = secure_filename(fname)
        user = secure_filename(username)
        fpath = safe_join(upload_path, user, fpath, fname)

        if not os.path.isfile(fpath):
            abort(404, "File not found")

        return jsonify(gen_metadata.post_metadata(fpath))


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
