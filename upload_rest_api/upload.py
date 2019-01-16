"""Module for handling the file uploads"""
import os
import zipfile

from flask import jsonify, abort, request

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as gen_metadata


def request_exceeds_quota():
    """Check whether the request exceeds users quota

    :returns: True if the request exceeds user's quota else False
    """
    username = request.authorization.username
    user = db.UsersDoc(username)
    quota = user.get_quota() - user.get_used_quota()

    return quota - request.content_length < 0


def _zipfile_exceeds_quota(zipf, username):
    """Check whether extracting the zipfile exceeds users quota

    :returns: True if the zipfile exceeds user's quota else False
    """
    user = db.UsersDoc(username)
    quota = user.get_quota() - user.get_used_quota()
    uncompressed_size = sum(zinfo.file_size for zinfo in zipf.filelist)

    return quota - uncompressed_size < 0


def _rm_symlinks(fpath):
    """Unlink all symlinks below fpath

    :param fpath: Path to directory under which all symlinks are unlinked
    :returns: None
    """
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            if os.path.islink(_file):
                os.unlink(_file)


def _save_stream(fpath, chunk_size=1024*1024):
    """Save the file into fpath by reading the stream in chunks
    of chunk_size bytes.
    """
    with open(fpath, "wb") as f_out:
        while True:
            chunk = request.stream.read(chunk_size)
            if chunk == '':
                break
            f_out.write(chunk)


def save_file(fpath, upload_path):
    """Save the posted file on disk at fpath by reading
    the upload stream in 1MB chunks. Extract zip files
    and check that no symlinks are created.

    :param fpath: Path where to save the file
    :param upload_path: Base bath not shown to the user
    :returns: HTTP Response
    """
    username = request.authorization.username

    # Write the file if it does not exist already
    if not os.path.exists(fpath):
        _save_stream(fpath)
        status = "created"
    else:
        abort(409, "File already exists")

    # Do not accept symlinks
    if os.path.islink(fpath):
        os.unlink(fpath)
        abort(415, "Symlinks are not supported")

    md5 = gen_metadata.md5_digest(fpath)

    # If zip file was uploaded extract all files
    if zipfile.is_zipfile(fpath):
        with zipfile.ZipFile(fpath) as zipf:
            fpath, fname = os.path.split(fpath)

            # Check the uncompressed size
            if _zipfile_exceeds_quota(zipf, username):
                # Remove zip archive and abort
                os.remove("%s/%s" % (fpath, fname))
                abort(413, "Quota exceeded")

            zipf.extractall(fpath)

        # Remove zip archive and all created symlinks
        os.remove("%s/%s" % (fpath, fname))
        _rm_symlinks(fpath)

        status = "zip uploaded and extracted"

    #Show user the relative path from /var/spool/uploads/
    return_path = fpath[len(upload_path):]

    response = jsonify({
        "file_path": return_path,
        "md5": md5,
        "status": status
    })
    response.status_code = 200

    return response
