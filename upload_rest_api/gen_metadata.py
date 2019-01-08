"""Module for generating basic file metadata and posting it to Metax"""
import os
import hashlib
from datetime import datetime
from uuid import uuid4

import magic
from flask import current_app, request

import metax_access
import upload_rest_api.database as db


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


def _get_mimetype(fpath):
    """Returns the MIME type of file fpath"""
    _magic = magic.open(magic.MAGIC_MIME_TYPE)
    _magic.load()
    mimetype = _magic.file(fpath)
    _magic.close()

    return mimetype


def iso8601_timestamp(fpath):
    """Returns last access time in ISO 8601 format"""
    timestamp = datetime.utcfromtimestamp(os.stat(fpath).st_atime)
    return str(timestamp.replace(microsecond=0).isoformat())


def _timestamp_now():
    """Returns current time in ISO 8601 format"""
    timestamp = datetime.utcnow()
    return str(timestamp.replace(microsecond=0).isoformat())


def _generate_metadata(fpath, upload_path, project, storage_id):
    """Generate metadata in json format"""
    timestamp = iso8601_timestamp(fpath)
    file_path = "/%s%s" % (project, fpath[len(upload_path+project)+1:])
    file_path = os.path.abspath(file_path)

    metadata = {
        "identifier" : uuid4().urn,
        "file_name" : os.path.split(fpath)[1],
        "file_format" : _get_mimetype(fpath),
        "byte_size" : os.stat(fpath).st_size,
        "file_path" : file_path,
        "project_identifier" : project,
        "file_uploaded" : timestamp,
        "file_frozen" : timestamp,
        "checksum" : {
            "algorithm" : "md5",
            "value" : md5_digest(fpath),
            "checked" : _timestamp_now()
        },
        "file_storage" : storage_id
    }

    return metadata


def post_metadata(fpaths):
    """generate and POST metadata to Metax

    :param fpaths: List of files for which to generate the metadata
    :returns: HTTP response given by Metax
    """
    app = current_app

    # Metax vars
    metax_url = app.config.get("METAX_URL")
    metax_user = app.config.get("METAX_USER")
    metax_password = app.config.get("METAX_PASSWORD")
    metax_client = metax_access.Metax(metax_url, metax_user, metax_password)

    # _generate_metadata() vars
    upload_path = app.config.get("UPLOAD_PATH")
    user = request.authorization.username
    project = db.User(user).get_project()
    storage_id = app.config.get("STORAGE_ID")

    metadata = []
    for fpath in fpaths:
        metadata.append(_generate_metadata(
            fpath, upload_path,
            project, storage_id
        ))

    return metax_client.post_file(metadata).json()
