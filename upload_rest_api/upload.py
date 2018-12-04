"""Module for handling the file uploads"""
import os
import hashlib
import zipfile
from shutil import rmtree

from flask import jsonify


def md5_digest(fpath):
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
        for fname in files:
            if os.path.islink("%s/%s" % (root, fname)):
                os.unlink("%s/%s" % (root, fname))


def save_file(_file, fpath, upload_path):
    """Save file _file on disk at fpath. Extract zip files
    and check that no symlinks are created.

    :param _file: The uploaded file request.files["file"]
    :param fpath: Path where to save the file
    :param upload_path: Base bath not shown to the user
    :returns: HTTP Response
    """
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
        md5 = md5_digest(fpath)

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
