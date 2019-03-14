"""Module for handling the file uploads"""
import os
import zipfile

from flask import jsonify, request

import upload_rest_api.database as db
import upload_rest_api.gen_metadata as gen_metadata
import upload_rest_api.utils as utils


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


def _zipfile_overwrites(fpath, namelist):
    """Check if writing files specified in namelist overwrites anything."""
    for name in namelist:
        if os.path.exists(os.path.join(fpath, name)):
            return True

    return False


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

    os.chmod(fpath, 0o664)


class OverwriteError(Exception):
    """Exception for trying to overwrite a existing file"""
    pass


class SymlinkError(Exception):
    """Exception for trying to create a symlink"""
    pass


class QuotaError(Exception):
    """Exception for exceeding to quota"""
    pass


def save_file(fpath):
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
        raise OverwriteError("File already exists")

    # Do not accept symlinks
    if os.path.islink(fpath):
        os.unlink(fpath)
        raise SymlinkError("Symlinks are not supported")

    md5 = gen_metadata.md5_digest(fpath)

    # If zip file was uploaded extract all files
    if zipfile.is_zipfile(fpath):
        with zipfile.ZipFile(fpath) as zipf:
            dir_path = os.path.split(fpath)[0]

            # Check the uncompressed size
            if _zipfile_exceeds_quota(zipf, username):
                # Remove zip archive and raise an exception
                os.remove(fpath)
                raise QuotaError("Quota exceeded")

            # Check that extracting the zipfile will not overwrite anything
            if _zipfile_overwrites(dir_path, zipf.namelist()):
                # Remove zip archive and raise an exception
                os.remove(fpath)
                raise OverwriteError(
                    "Zip extraction error: "
                    "overwriting existing files not allowed"
                )

            zipf.extractall(dir_path)

        # Remove zip archive and all created symlinks
        os.remove(fpath)
        _rm_symlinks(dir_path)

        status = "zip uploaded and extracted"

    response = jsonify({
        "file_path": utils.get_return_path(fpath),
        "md5": md5,
        "status": status
    })
    response.status_code = 200

    return response
