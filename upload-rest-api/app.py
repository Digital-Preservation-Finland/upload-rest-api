"""REST api for uploading files into passipservice
"""
from flask import Flask, abort, safe_join, request, jsonify
from shutil import rmtree
import hashlib
import os
from werkzeug.utils import secure_filename


BASE_PATH = "/home/vagrant/test/rest"


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
        "/upload/v1/<string:project>/<path:fpath>/<string:fname>",
        methods=["POST"]
    )
    def upload_file(project, fpath, fname):
        """Save file uploaded as multipart/form-data at
        /var/spool/uploads/organization/project/fpath

        :returns: HTTP Response
        """
        upload_dir = safe_join(BASE_PATH, secure_filename(project))
        _file = request.files["file"]
        fpath = safe_join(upload_dir, fpath)
        fname = secure_filename(fname)

        # Create directory if it does not exist
        if not os.path.exists(fpath):
            os.makedirs(fpath)

        # Write the file if it does not exist already
        if not os.path.exists("%s/%s" % (fpath, fname)):
            _file.save("%s/%s" % (fpath, fname))
            written = "true"
        else:
            written = "false"

        if os.path.islink("%s/%s" % (fpath, fname)):
            os.unlink("%s/%s" % (fpath, fname))
            written = "false"

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(BASE_PATH):]

        response = jsonify(
            {
                "written": written,
                "file_path": return_path,
                "file_name": fname,
                "md5": _md5_digest("%s/%s" % (fpath, fname))
            }
        )
        response.status_code = 200

        return response


    @app.route(
        "/upload/v1/<string:project>/<path:fpath>/<string:fname>",
        methods=["GET"]
    )
    def get_file(project, fpath, fname):
        """Get filepath, name and checksum.

        :returns: HTTP Response
        """
        upload_dir = safe_join(BASE_PATH, secure_filename(project))
        fpath = safe_join(upload_dir, fpath)
        fname = secure_filename(fname)
        _file = "%s/%s" % (fpath, fname)

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(BASE_PATH):]

        if os.path.exists(_file):
            return jsonify(
                {
                    "file_path": return_path,
                    "file_name": fname,
                    "md5": _md5_digest(_file)
                }
            )
        else:
            abort(404)

    @app.route(
        "/upload/v1/<string:project>/<path:fpath>/<string:fname>",
        methods=["DELETE"]
    )
    def delete_file(project, fpath, fname):
        """Get filepath, name and checksum.

        :returns: HTTP Response
        """
        upload_dir = safe_join(BASE_PATH, secure_filename(project))
        fpath = safe_join(upload_dir, fpath)
        fname = secure_filename(fname)
        _file = "%s/%s" % (fpath, fname)

        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(BASE_PATH):]

        if os.path.exists(_file):
            return jsonify(
                {
                    "file_path": return_path,
                    "file_name": fname,
                    "md5": _md5_digest(_file)
                }
            )
        else:
            abort(404)


    @app.route(
        "/upload/v1/<string:project>",
        methods=["GET"]
    )
    def get_files(project):
        """Get all files under a project

        :return: HTTP Response
        """
        fpath = safe_join(BASE_PATH, secure_filename(project))
        
        if not os.path.exists(fpath):
            abort(404)

        file_dict = {}
        for root, dirs, files in os.walk(fpath):
            file_dict[root[len(BASE_PATH):]] = files

        response = jsonify(file_dict)
        response.status_code = 200
        return response


    @app.route(
        "/upload/v1/<string:project>",
        methods=["DELETE"]
    )
    def delete_files(project):
        """Delete all files under a project

        :returns: HTTPS Response
        """
        fpath = safe_join(BASE_PATH, secure_filename(project))

        if not os.path.exists(fpath):
            abort(404)

        rmtree(fpath)

        response = jsonify({"fpath": fpath[len(BASE_PATH):], "deleted": "true"})
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


    if __name__ == "__main__":
        app.run(debug=True)

if __name__ == "__main__":
    create_app()
