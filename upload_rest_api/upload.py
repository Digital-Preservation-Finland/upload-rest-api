"""Module for handling the file uploads"""
from __future__ import unicode_literals

import os
import tarfile
import zipfile

from flask import jsonify, request

from archive_helpers.extract import extract
from archive_helpers.extract import (
    MemberNameError, MemberOverwriteError, MemberTypeError
)

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


def _archive_exceeds_quota(archive_path, username):
    """Check whether extracting the archive exceeds users quota.

    :returns: True if the archive exceeds user's quota else False
    """
    user = db.UsersDoc(username)
    quota = user.get_quota() - user.get_used_quota()

    if tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as archive:
            size = sum(memb.size for memb in archive)
    else:
        with zipfile.ZipFile(archive_path) as archive:
            size = sum(memb.file_size for memb in archive.filelist)

    return quota - size < 0


def _process_extracted_files(fpath):
    """Unlink all symlinks below fpath and change the mode of all other
    regular files to 0o664.

    :param fpath: Path to the directory to be processed
    :returns: None
    """
    for dirpath, _, files in os.walk(fpath):
        for fname in files:
            _file = os.path.join(dirpath, fname)
            if os.path.islink(_file):
                os.unlink(_file)
            elif os.path.isfile(_file):
                os.chmod(_file, 0o664)


def _get_archive_checksums(archive, extract_path):
    """Calculate md5 checksums of all archive members and return a list of
    dicts::

        {
            "_id": filpath,
            "checksum": md5 digest
        }

    :param archive: Path to the extracted archive
    :param extract_path: Path to the dir where the archive was extracted
    :returns: A list of checksum dicts
    """
    if tarfile.is_tarfile(archive):
        with tarfile.open(archive) as tarf:
            files = [member.name for member in tarf]
    else:
        with zipfile.ZipFile(archive) as zipf:
            files = [member.filename for member in zipf.infolist()]

    checksums = []
    for _file in files:
        fpath = os.path.abspath(os.path.join(extract_path, _file))
        if os.path.isfile(fpath):
            checksums.append({
                "_id": fpath,
                "checksum": gen_metadata.md5_digest(fpath)
            })

    return checksums


def _save_stream(fpath, chunk_size=1024*1024):
    """Save the file into fpath by reading the stream in chunks
    of chunk_size bytes.
    """
    with open(fpath, "wb") as f_out:
        while True:
            chunk = request.stream.read(chunk_size)
            if chunk == b'':
                break
            f_out.write(chunk)

    os.chmod(fpath, 0o664)


class OverwriteError(Exception):
    """Exception for trying to overwrite a existing file"""
    pass


class QuotaError(Exception):
    """Exception for exceeding to quota"""
    pass


def save_file(fpath, extract_archives=False):
    """Save the posted file on disk at fpath by reading
    the upload stream in 1MB chunks. Extract zip files
    and check that no symlinks are created.

    :param fpath: Path where to save the file
    :param extract_archives: Defines wheter or not tar and zip archives are
                             extracted
    :returns: HTTP Response
    """
    username = request.authorization.username

    # Write the file if it does not exist already
    if not os.path.exists(fpath):
        _save_stream(fpath)
        message = "created"
    else:
        raise OverwriteError("File already exists")

    md5 = gen_metadata.md5_digest(fpath)

    # If zip or tar file was uploaded, extract all files
    is_archive = zipfile.is_zipfile(fpath) or tarfile.is_tarfile(fpath)
    if is_archive and extract_archives:
        dir_path = os.path.split(fpath)[0]

        # Check the uncompressed size
        if _archive_exceeds_quota(fpath, username):
            # Remove the archive and raise an exception
            os.remove(fpath)
            raise QuotaError("Quota exceeded")

        try:
            extract(fpath, dir_path)
        except (MemberNameError, MemberTypeError, MemberOverwriteError):
            # Remove the archive and raise the exception
            os.remove(fpath)
            raise

        # Add checksums of the extracted files to mongo
        db.ChecksumsCol().insert(_get_archive_checksums(fpath, dir_path))

        # Remove archive and all created symlinks
        os.remove(fpath)
        _process_extracted_files(dir_path)

        message = "archive uploaded and extracted"

    else:
        # Add file checksum to mongo
        db.ChecksumsCol().insert_one(os.path.abspath(fpath), md5)

    response = jsonify({
        "file_path": utils.get_return_path(fpath),
        "md5": md5,
        "message": message
    })
    response.status_code = 200

    return response
