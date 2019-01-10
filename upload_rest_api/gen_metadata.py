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


def get_metax_path(fpath, upload_path):
    """Returns file_path that is stored in Metax"""
    file_path = "/%s" % (fpath[len(upload_path)+1:])
    file_path = os.path.abspath(file_path)

    return file_path


def _generate_metadata(fpath, upload_path, project, storage_id):
    """Generate metadata in json format"""
    timestamp = iso8601_timestamp(fpath)
    file_path = get_metax_path(fpath, upload_path)

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


class MetaxClient(object):
    """Class for handling Metax metadata"""

    def __init__(self, url=None, user=None, password=None):
        """Init MetaxClient instances"""

        # If any of the params is not provided read them from app.config
        if url is None or user is None or password is None:
            app = current_app
            url = app.config.get("METAX_URL")
            user = app.config.get("METAX_USER")
            password = app.config.get("METAX_PASSWORD")

        self.client = metax_access.Metax(url, user, password)

    def get_files_dict(self, project):
        """Returns dict {fpath: id} of all the files of a given project"""
        return self.client.get_files_dict(project)

    def post_metadata(self, fpaths):
        """generate and POST metadata to Metax

        :param fpaths: List of files for which to generate the metadata
        :returns: HTTP response returned by Metax
        """
        app = current_app

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

        return self.client.post_file(metadata).json()

    def delete_metadata(self, project, fpaths):
        """DELETE metadata from Metax

        :param project: Project identifier
        :param fpaths: List of file_paths to remove
        :returns: HTTP response returned by Metax
        """
        files_dict = self.client.get_files_dict(project)

        # Generate the list of ids to remove from Metax
        file_id_list = []
        for fpath in fpaths:
            if fpath in files_dict:
                file_id_list.append(files_dict[fpath])

        return self.client.delete_files(file_id_list)

    def file_has_dataset(self, fpath, files_dict):
        """Check if file belongs to any dataset"""
        if fpath not in files_dict:
            return False

        file_id = files_dict[fpath]
        datasets = self.client.get_file_datasets(file_id)

        return len(datasets) != 0
