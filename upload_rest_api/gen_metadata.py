"""Module for generating basic file metadata and posting it to Metax"""
from __future__ import unicode_literals

import hashlib
import os
from datetime import datetime
from uuid import uuid4

import requests.exceptions
import six
import magic
from flask import current_app, request

from metax_access import (Metax, DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_IN_DIGITAL_PRESERVATION)
import upload_rest_api.database as db

PAS_FILE_STORAGE_ID = "urn:nbn:fi:att:file-storage-pas"


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

    return six.text_type(mimetype)


def iso8601_timestamp(fpath):
    """Returns last access time in ISO 8601 format"""
    timestamp = datetime.utcfromtimestamp(os.stat(fpath).st_atime)
    return "{}+00:00".format(timestamp.replace(microsecond=0).isoformat())


def _timestamp_now():
    """Returns current time in ISO 8601 format"""
    timestamp = datetime.utcnow()
    return "{}+00:00".format(timestamp.replace(microsecond=0).isoformat())


def get_metax_path(fpath, upload_path):
    """Returns file_path that is stored in Metax"""
    file_path = "/%s" % (fpath[len(upload_path)+1:])
    file_path = os.path.abspath(file_path)
    project = file_path.split("/")[1]

    return file_path[len(project)+1:]


def _generate_metadata(fpath, upload_path, project, storage_id, checksums):
    """Generate metadata in json format"""
    timestamp = iso8601_timestamp(fpath)
    file_path = get_metax_path(fpath, upload_path)

    metadata = {
        "identifier": six.text_type(uuid4().urn),
        "file_name": six.text_type(os.path.split(fpath)[1]),
        "file_format": _get_mimetype(fpath),
        "byte_size": os.stat(fpath).st_size,
        "file_path": file_path,
        "project_identifier": project,
        "file_uploaded": timestamp,
        "file_modified": timestamp,
        "file_frozen": timestamp,
        "checksum": {
            "algorithm": "md5",
            "value": checksums.get_checksum(os.path.abspath(fpath)),
            "checked": _timestamp_now()
        },
        "file_storage": storage_id
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

        self.client = Metax(url, user, password)
        # dataset_id => preservation_state dict
        self.dataset_cache = {}

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
        project = db.UsersDoc(user).get_project()
        storage_id = app.config.get("STORAGE_ID")

        checksums = db.ChecksumsCol()
        metadata = []
        for fpath in fpaths:
            metadata.append(_generate_metadata(
                fpath, upload_path,
                project, storage_id, checksums
            ))

        try:
            return self.client.post_file(metadata), 200
        except requests.exceptions.HTTPError as exception:
            response = exception.response
            return response.json(), response.status_code

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
                file_id_list.append(files_dict[fpath]["id"])

        try:
            return self.client.delete_files(file_id_list), 200
        except requests.exceptions.HTTPError as exception:
            response = exception.response
            return response.json(), response.status_code

    def delete_file_metadata(self, project, fpath, force=False):
        """Delete file metadata from Metax if file is not associated with
        any dataset. If force parameter is True metadata is deleted if the
        file belongs to a dataset not accepted to preservation.
        """
        self.dataset_cache.clear()
        upload_path = current_app.config.get("UPLOAD_PATH")
        files_dict = self.client.get_files_dict(project)
        metax_path = get_metax_path(fpath, upload_path)

        if metax_path not in files_dict:
            response = "Metadata not found in Metax"
            status_code = 404
        elif files_dict[metax_path]["storage_identifier"] != \
                PAS_FILE_STORAGE_ID:
            response = "Incorrect file storage. Metadata not removed"
            status_code = 400
        elif not force and self.file_has_dataset(metax_path, files_dict):
            response = "Metadata is part of a dataset. Metadata not removed"
        elif force and self.file_has_accepted_dataset(metax_path, files_dict):
            response = ("Metadata is part of an accepted dataset. Metadata not"
                        " removed")
            status_code = 400
        else:
            file_id = six.text_type(files_dict[metax_path]["id"])
            response = self.client.delete_file(file_id)
            status_code = 200

        return response, status_code

    def delete_all_metadata(self, project, fpath, force=False):
        """Delete all file metadata from Metax found under dir fpath, which
        is not associated with any dataset and is stored in PAS file storage.
        If force parameter is True metadata is deleted if file belongs to a
        dataset not accepted to preservation.
        """
        self.dataset_cache.clear()
        upload_path = current_app.config.get("UPLOAD_PATH")
        files_dict = self.client.get_files_dict(project)
        file_id_list = []
        status_code = 200
        # Iterate through all files under dir fpath
        for dirpath, _, files in os.walk(fpath):
            for _file in files:
                fpath = os.path.join(dirpath, _file)
                metax_path = get_metax_path(fpath, upload_path)
                if metax_path not in files_dict:
                    continue
                storage_id = files_dict[metax_path]["storage_identifier"]
                if storage_id != PAS_FILE_STORAGE_ID:
                    continue
                # Append file id to file_id_list if file is not associated
                # with any dataset and file metadata is in Metax
                if force:
                    no_dataset = not self.file_has_accepted_dataset(metax_path,
                                                                    files_dict)
                else:
                    no_dataset = not self.file_has_dataset(metax_path,
                                                           files_dict)
                if no_dataset:
                    file_id_list.append(files_dict[metax_path]["id"])
                else:
                    status_code = 400

        if not file_id_list:
            return {"deleted_files_count": 0}, status_code
        # Remove file metadata from Metax and return the response
        response = self.client.delete_files(file_id_list)
        return response, 200

    def get_all_ids(self, project_list):
        """Get a set of all identifiers of files in any of the projects in
        project_list.
        """
        id_set = set()

        # Iterate all projects
        for project in project_list:
            # Find all indentifiers in one project
            files_dict = self.get_files_dict(project)
            project_id_set = {
                _file["identifier"] for _file in files_dict.values()
            }

            # Add the identifiers to id_set
            id_set |= project_id_set

        return id_set

    def file_has_dataset(self, metax_path, files_dict):
        """Check if file belongs to any dataset"""
        if metax_path not in files_dict:
            return False

        file_id = files_dict[metax_path]["id"]
        datasets = self.client.get_file_datasets(file_id)
        return len(datasets) != 0

    def file_has_accepted_dataset(self, metax_path, files_dict):
        """Check if file belongs to dataset accepted to preservation"""
        if metax_path in files_dict:
            file_id = files_dict[metax_path]["id"]
            dataset_ids = self.client.get_file_datasets(file_id)
            for dataset_id in dataset_ids:
                if dataset_id not in self.dataset_cache:
                    dataset = self.client.query_datasets(
                        {'preferred_identifier': dataset_id}
                    )
                    self.dataset_cache[dataset_id] = \
                        dataset['preservation_state']
                dataset_state = self.dataset_cache[dataset_id]
                if (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION <=
                        dataset_state <=
                        DS_STATE_IN_DIGITAL_PRESERVATION):
                    return True
        return False
