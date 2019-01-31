"""REST api for uploading files into passipservice
"""
import os
from shutil import rmtree

from flask import Flask, abort, safe_join, request, jsonify
import werkzeug
from werkzeug.utils import secure_filename
from werkzeug.exceptions import NotFound

import upload_rest_api.upload as up
import upload_rest_api.authentication as auth
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md


def configure_app(app):
    """Read config from /etc/upload_rest_api.conf"""
    app.config.from_pyfile("/etc/upload_rest_api.conf")


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
    app = Flask(__name__)

    # Configure app
    configure_app(app)

    # Authenticate all requests
    app.before_request(auth.authenticate)


    def _get_upload_path(fpath):
        """Get upload path for current request"""
        username = request.authorization.username
        user = db.UsersDoc(username)
        project = user.get_project()

        upload_path = app.config.get("UPLOAD_PATH")
        fpath, fname = os.path.split(fpath)
        fname = secure_filename(fname)
        project = secure_filename(project)

        joined_path = safe_join(upload_path, project)
        joined_path = safe_join(joined_path, fpath)

        return joined_path, fname


    @app.route(
        "%s/<path:fpath>" % app.config.get("UPLOAD_API_PATH"),
        methods=["POST"]
    )
    def upload_file(fpath):
        """Save the uploaded file at /var/spool/uploads/project/fpath

        :returns: HTTP Response
        """
        # Update used_quota also at the start of the function
        # since multiple users might by using the same project
        db.update_used_quota()

        if request.content_length > app.config.get("MAX_CONTENT_LENGTH"):
            abort(413, "Max single file size exceeded")
        elif up.request_exceeds_quota():
            abort(413, "Quota exceeded")

        fpath, fname = _get_upload_path(fpath)

        # Create directory if it does not exist
        if not os.path.exists(fpath):
            os.makedirs(fpath, 0o700)

        fpath = safe_join(fpath, fname)
        response = up.save_file(fpath, app.config.get("UPLOAD_PATH"))
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
        fpath, fname = _get_upload_path(fpath)
        fpath = safe_join(fpath, fname)

        if not os.path.isfile(fpath):
            abort(404, "File not found")

        # Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(app.config.get("UPLOAD_PATH")):]
        return_path = os.path.normpath(return_path)

        return jsonify({
            "file_path": return_path,
            "md5": md.md5_digest(fpath),
            "timestamp": md.iso8601_timestamp(fpath)
        })


    @app.route(
        "%s/<path:fpath>" % app.config.get("UPLOAD_API_PATH"),
        methods=["DELETE"]
    )
    def delete_file(fpath):
        """Get filepath, name and checksum.

        :returns: HTTP Response
        """
        username = request.authorization.username
        project = db.UsersDoc(username).get_project()
        fpath, fname = _get_upload_path(fpath)
        fpath = safe_join(fpath, fname)

        if os.path.isfile(fpath):
            os.remove(fpath)
            db.update_used_quota()
        else:
            abort(404, "File not found")

        # Remove metadata from Metax
        metax_response = md.MetaxClient().delete_file_metadata(project, fpath)

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(app.config.get("UPLOAD_PATH")):]
        return_path = os.path.normpath(return_path)

        return jsonify({
            "file_path": return_path,
            "status": "deleted",
            "metax": metax_response
        })


    @app.route(
        "%s" % app.config.get("UPLOAD_API_PATH"),
        methods=["GET"]
    )
    def get_files():
        """Get all files of the user

        :return: HTTP Response
        """
        username = request.authorization.username
        project = db.UsersDoc(username).get_project()
        upload_path = app.config.get("UPLOAD_PATH")
        fpath = safe_join(upload_path, secure_filename(project))

        if not os.path.exists(fpath):
            abort(404, "No files found")

        file_dict = {}
        for dirpath, _, files in os.walk(fpath):
            file_dict[dirpath[len(upload_path):]] = files

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
        project = db.UsersDoc(username).get_project()
        upload_path = app.config.get("UPLOAD_PATH")
        fpath = safe_join(upload_path, secure_filename(project))

        if not os.path.exists(fpath):
            abort(404, "No files found")

        # Remove metadata from Metax
        metax_response = md.MetaxClient().delete_all_metadata(project, fpath)

        # Remove project directory and update used_quota
        rmtree(fpath)
        db.update_used_quota()

        response = jsonify({
            "fpath": fpath[len(upload_path):],
            "status": "deleted",
            "metax": metax_response
        })
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

        user = db.UsersDoc(username)
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

        user = db.UsersDoc(username)
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
        db.UsersDoc(username).delete()

        response = jsonify({"username": username, "status": "deleted"})
        response.status_code = 200

        return response


    @app.route(
        "%s/<path:fpath>" % app.config.get("METADATA_API_PATH"),
        methods=["POST"]
    )
    def post_metadata(fpath):
        """POST file metadata to Metax

        :returns: HTTP Response
        """
        fpath, fname = _get_upload_path(fpath)
        fpath = safe_join(fpath, fname)

        if os.path.isdir(fpath):
            # POST metadata of all files under dir fpath
            fpaths = []
            for dirpath, _, files in os.walk(fpath):
                for fname in files:
                    fpaths.append(os.path.join(dirpath, fname))

        elif os.path.isfile(fpath):
            fpaths = [fpath]

        else:
            abort(404, "File not found")

        metax_client = md.MetaxClient()
        response = metax_client.post_metadata(fpaths)

        # Add created identifiers to Mongo
        if "success" in response and len(response["success"]) > 0:
            created_md = response["success"]
            db.FilesCol().store_identifiers(created_md)

        return jsonify(response)


    @app.errorhandler(401)
    def http_error_401(error):
        """Response handler for status code 401"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(404)
    def http_error_404(error):
        """Response handler for status code 404"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(405)
    def http_error_405(error):
        """Response handler for status code 405"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(409)
    def http_error_409(error):
        """Response handler for status code 409"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(413)
    def http_error_413(error):
        """Response handler for status code 413"""
        response = jsonify({"code": error.code, "error": str(error)})
        response.status_code = error.code
        return response


    @app.errorhandler(500)
    def http_error_500(error):
        """Response handler for status code 500"""
        response = jsonify({"code": "500", "error": "Internal server error"})
        response.status_code = 500
        return response


    return app


if __name__ == "__main__":
    create_app().run()
