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
    timestamp = datetime.fromtimestamp(os.stat(fpath).st_atime)
    return str(timestamp.replace(microsecond=0).isoformat())


def _timestamp_now():
    """Returns current time in ISO 8601 format"""
    timestamp = datetime.now()
    return str(timestamp.replace(microsecond=0).isoformat())


def _generate_metadata(fpath, upload_path, project, storage_id):
    """Generate metadata in json format"""
    timestamp = iso8601_timestamp(fpath)

    metadata = {
        "identifier" : uuid4().urn,
        "file_name" : os.path.split(fpath)[1],
        "file_format" : _get_mimetype(fpath),
        "file_path" : "/%s%s" % (project, fpath[len(upload_path):]),
        "project_identifier" : project,
        "file_uploaded" : timestamp,
        "file_frozen" : timestamp,
        "checksum" : {
            "algorithm" : "md5",
            "value" : md5_digest(fpath),
            "checked" : _timestamp_now()
        },
        "file_storage" : {
            "identifier" : storage_id
        }
    }

    return metadata


def post_metadata():
    """generate and POST metadata to Metax"""
    pass


if __name__ == "__main__":
    import json
    print json.dumps(
        _generate_metadata(
             "/home/vagrant/test/rest/admin/asd",
             "/home/vagrant/test/rest", "admin_project",
             "urn:uuid:f843c26d-b5f7-4c66-91e7-2e75f5377636"
        )
    )
