"""REST api for uploading files into passipservice
"""
import os
import hashlib

from shutil import rmtree
from flask import Flask, abort, safe_join, request, jsonify
from werkzeug.utils import secure_filename


FILE_PATH = "/home/vagrant/test/rest"
API_PATH = "/api/upload/v1"


def _md5_digest(fname):
    """Return md5 digest of file fname
    """
    md5_hash = hashlib.md5()

    with open(fname, "rb") as _file:
        # read the file in 1MB chunks
        for chunk in iter(lambda: _file.read(1024 * 1024), b''):
            md5_hash.update(chunk)

    return md5_hash.hexdigest()


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
    app = Flask(__name__)

    @app.route(
        "%s/<string:project>/<path:fpath>" % API_PATH,
        methods=["POST"]
    )
    def upload_file(project, fpath):
        """Save file uploaded as multipart/form-data at
        /var/spool/uploads/organization/project/fpath

        :returns: HTTP Response
        """
        _file = request.files["file"]

        fpath, fname = os.path.split(fpath)
        fname = secure_filename(fname)
        project = secure_filename(project)
        fpath = safe_join(FILE_PATH, project, fpath)

        # Create directory if it does not exist
        if not os.path.exists(fpath):
            os.makedirs(fpath, 0o700)

        fpath = safe_join(fpath, fname)

        # Write the file if it does not exist already
        if not os.path.exists(fpath):
            _file.save(fpath)
            status = "file created"
        else:
            status = "file already exists"

        # Do not accept symlinks
        if os.path.islink(fpath):
            os.unlink(fpath)
            status = "file not created. symlinks are not supported"

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(FILE_PATH):]

        response = jsonify(
            {
                "file_path": return_path,
                "md5": _md5_digest(fpath),
                "status": status
            }
        )
        response.status_code = 200

        return response


    @app.route(
        "%s/<string:project>/<path:fpath>" % API_PATH,
        methods=["GET"]
    )
    def get_file(project, fpath):
        """Get filepath, name and checksum.

        :returns: HTTP Response
        """
        fpath, fname = os.path.split(fpath)

        fname = secure_filename(fname)
        project = secure_filename(project)
        fpath = safe_join(FILE_PATH, project, fpath, fname)

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(FILE_PATH):]

        if os.path.isfile(fpath):
            return jsonify(
                {
                    "file_path": return_path,
                    "md5": _md5_digest(fpath)
                }
            )
        else:
            abort(404)


    @app.route(
        "%s/<string:project>/<path:fpath>" % API_PATH,
        methods=["DELETE"]
    )
    def delete_file(project, fpath):
        """Get filepath, name and checksum.

        :returns: HTTP Response
        """
        fpath, fname = os.path.split(fpath)

        fname = secure_filename(fname)
        project = secure_filename(project)
        fpath = safe_join(FILE_PATH, project, fpath, fname)

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(FILE_PATH):]

        if os.path.isfile(fpath):
            os.remove(fpath)
            return jsonify(
                {
                    "file_path": return_path,
                    "status": "deleted"
                }
            )
        else:
            abort(404)


    @app.route(
        "%s/<string:project>" % API_PATH,
        methods=["GET"]
    )
    def get_files(project):
        """Get all files under a project

        :return: HTTP Response
        """
        fpath = safe_join(FILE_PATH, secure_filename(project))

        if not os.path.exists(fpath):
            abort(404)

        file_dict = {}
        for root, dirs, files in os.walk(fpath):
            file_dict[root[len(FILE_PATH):]] = files

        response = jsonify(file_dict)
        response.status_code = 200

        return response


    @app.route(
        "%s/<string:project>" % API_PATH,
        methods=["DELETE"]
    )
    def delete_files(project):
        """Delete all files under a project

        :returns: HTTPS Response
        """
        fpath = safe_join(FILE_PATH, secure_filename(project))

        if not os.path.exists(fpath):
            abort(404)

        rmtree(fpath)

        response = jsonify({"fpath": fpath[len(FILE_PATH):], "status": "deleted"})
        response.status_code = 200

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
    def page_not_found(error):
        """JSON response handler for the 404 - Not found errors

        :returns: HTTP Response
        """
        response = jsonify({"code": 405, "error": str(error)})
        response.status_code = 405

        return response


    if __name__ == "__main__":
        app.run(debug=True)


if __name__ == "__main__":
    create_app()
