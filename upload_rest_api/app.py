"""REST api for uploading files into passipservice
"""
import os
import hashlib
import zipfile

from shutil import rmtree
from flask import Flask, abort, safe_join, request, jsonify
from werkzeug.utils import secure_filename


def _md5_digest(fpath):
    """Return md5 digest of file fpath

    :param fpath: path to file to be hashed
    :returns: digest as a string
    """
    md5_hash = hashlib.md5()

    with open(fpath, "rb") as _file:
        # read the file in 1MB chunks
        for chunk in iter(lambda: _file.read(1024 * 1024), b''):
            md5_hash.update(chunk)

    return md5_hash.hexdigest()


def _rm_symlinks(fpath):
    """Unlink all symlinks below fpath

    :param fpath: Path to directory under which all symlinks are unlinked
    :returns: None
    """
    for root, dirs, files in os.walk(fpath):
        for _file in files:
            if os.path.islink("%s/%s" % (root, _file)):
                os.unlink("%s/%s" % (root, _file))


def create_app():
    """Configure and return a Flask application instance.

    :returns: Instance of flask.Flask()
    """
    app = Flask(__name__)

    app.config["UPLOAD_PATH"] = "/home/vagrant/test/rest"
    app.config["API_PATH"] = "/api/upload/v1"

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
            md5 = "none"
        else:
            md5 = _md5_digest(fpath)

        # If zip file was uploaded extract all files
        if zipfile.is_zipfile(fpath):

            # Extract
            with zipfile.ZipFile(fpath) as zipf:
                fpath, fname = os.path.split(fpath)
                zipf.extractall(fpath)

            # Remove zip archive
            os.remove("%s/%s" % (fpath, fname))

            # Remove possible symlinks
            _rm_symlinks(fpath)

            status = "zip uploaded and extracted"


        #Show user the relative path from /var/spool/uploads/
        return_path = fpath[len(upload_path):]

        response = jsonify(
            {
                "file_path": return_path,
                "md5": md5,
                "status": status
            }
        )
        response.status_code = 200

        return response


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

        return jsonify({"file_path": return_path, "md5": _md5_digest(fpath)})


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
