"""Module for generating basic file metadata and posting it to Metax."""
from __future__ import unicode_literals

import hashlib
import os
from datetime import datetime
from uuid import uuid4

import six

import magic
from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_IN_DIGITAL_PRESERVATION,
                          Metax)
import upload_rest_api.database as db
from upload_rest_api.config import CONFIG

PAS_FILE_STORAGE_ID = "urn:nbn:fi:att:file-storage-pas"


def md5_digest(fpath):
    """Return md5 digest of file fpath.

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
    """Return the MIME type of file fpath."""
    _magic = magic.open(magic.MAGIC_MIME_TYPE)
    _magic.load()
    mimetype = _magic.file(fpath)
    _magic.close()

    return six.text_type(mimetype)


def iso8601_timestamp(fpath):
    """Return last access time in ISO 8601 format."""
    timestamp = datetime.utcfromtimestamp(os.stat(fpath).st_atime)
    return "{}+00:00".format(timestamp.replace(microsecond=0).isoformat())


def _timestamp_now():
    """Return current time in ISO 8601 format."""
    timestamp = datetime.utcnow()
    return "{}+00:00".format(timestamp.replace(microsecond=0).isoformat())


def get_metax_path(fpath, root_upload_path):
    """Return file_path that is stored in Metax."""
    file_path = "/%s" % (fpath[len(root_upload_path)+1:])
    file_path = os.path.abspath(file_path)
    project = file_path.split("/")[1]

    return file_path[len(project)+1:]


def _generate_metadata(fpath, root_upload_path, project, storage_id,
                       checksums):
    """Generate metadata in json format."""
    timestamp = iso8601_timestamp(fpath)
    file_path = get_metax_path(fpath, root_upload_path)

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
            "algorithm": "MD5",
            "value": checksums[os.path.abspath(fpath)],
            "checked": _timestamp_now()
        },
        "file_storage": storage_id
    }

    return metadata


def _strip_metax_response(metax_response):
    """Collect only the necessary fields from the metax response."""
    response = {"success": [], "failed": []}

    if "failed" in metax_response:
        response["failed"] = metax_response["failed"]

    if "success" in metax_response and metax_response["success"]:
        for file_md in metax_response["success"]:
            identifier = file_md["object"]["identifier"]
            file_path = file_md["object"]["file_path"]
            parent_dir = file_md["object"]["parent_directory"]["identifier"]
            checksum = file_md["object"]["checksum"]["value"]

            metadata = {
                "object": {
                    "identifier": identifier,
                    "file_path": file_path,
                    "parent_directory": {"identifier": parent_dir},
                    "checksum": {"value": checksum}
                }
            }

            response["success"].append(metadata)

    return response


class MetaxClientError(Exception):
    """Generic error raised by MetaxClient."""


class MetaxClient(object):
    """Class for handling Metax metadata."""

    def __init__(self, url=None, user=None, password=None, verify=None):
        """Init MetaxClient instances."""
        # If any of the params is not provided read them from app.config
        if url is None or user is None or password is None:
            url = CONFIG.get("METAX_URL")
            user = CONFIG.get("METAX_USER")
            password = CONFIG.get("METAX_PASSWORD")

        if verify is None:
            verify = CONFIG.get("METAX_SSL_VERIFICATION", True)

        self.client = Metax(url, user, password, verify=verify)
        # dataset_id => preservation_state dict
        self.dataset_cache = {}

    def get_files_dict(self, project):
        """Return dict {fpath: id} of all the files of a given project.
        """
        return self.client.get_files_dict(project)

    def post_metadata(self, fpaths, root_upload_path, username, storage_id):
        """Generate file metadata and POST it to Metax in 5k chunks.

        :param fpaths: List of files for which to generate the metadata
        :param root_upload_path: root upload directory
        :param username: current user
        :param storage_id: pas storage identifier in Metax
        :returns: Stripped HTTP response returned by Metax.
                  Success list contains succesfully generated file
                  metadata in format:
                  [
                      {
                          "object": {
                              "identifier": identifier,
                              "file_path": file_path,
                              "checksum": {"value": checksum},
                              "parent_directory": {
                                  "identifier": identifier
                              }
                          }
                      },
                      .
                      .
                      .
                  ]
        """
        database = db.Database()
        project = database.user(username).get_project()
        checksums = database.checksums.get_checksums()
        metadata = []
        responses = []

        i = 0
        for fpath in fpaths:
            metadata.append(_generate_metadata(
                fpath, root_upload_path,
                project, storage_id, checksums
            ))

            # POST metadata to Metax every 5k steps
            i += 1
            if i % 5000 == 0:
                response = self.client.post_file(metadata)
                responses.append(_strip_metax_response(response))
                # Add created identifiers to Mongo
                if "success" in response and response["success"]:
                    database.store_identifiers(
                        response["success"], root_upload_path, username
                    )

                metadata = []

        # POST remaining metadata
        if metadata:
            response = self.client.post_file(metadata)
            responses.append(_strip_metax_response(response))
            # Add created identifiers to Mongo
            if "success" in response and response["success"]:
                database.store_identifiers(
                    response["success"], root_upload_path, username
                )

        # Merge all responses into one response
        response = {"success": [], "failed": []}
        for metax_response in responses:
            if "success" in metax_response:
                response["success"].extend(metax_response["success"])
            if "failed" in metax_response:
                response["failed"].extend(metax_response["failed"])

        return response

    def delete_metadata(self, project, fpaths):
        """DELETE metadata from Metax.

        :param project: Project identifier
        :param fpaths: List of file_paths to remove
        :returns: HTTP response returned by Metax
        """
        files_dict = self.client.get_files_dict(project)

        # Retrieve "file -> dataset" association map
        file_ids = [
            file_["identifier"] for file_ in six.itervalues(files_dict)
        ]
        file2datasets = {}
        if file_ids:
            file2datasets = self.client.get_file2dataset_dict(file_ids)

        # Delete metadata if file exists in fpaths AND it doesn't have
        # any datasets
        file_ids_to_delete = []
        for metax_path, file_ in six.iteritems(files_dict):
            path_exists = metax_path in fpaths
            dataset_exists = file2datasets.get(file_["identifier"], None)

            if path_exists and not dataset_exists:
                file_ids_to_delete.append(file_["identifier"])

        if not file_ids_to_delete:
            return {"deleted_files_count": 0}

        return self.client.delete_files(file_ids_to_delete)

    def delete_file_metadata(self, project, fpath, root_upload_path=None,
                             force=False):
        """Delete file metadata from Metax if file is not associated
        with any dataset.

        If force parameter is True metadata is deleted if the file
        belongs to a dataset not accepted to preservation.
        """
        self.dataset_cache.clear()
        files_dict = self.client.get_files_dict(project)
        metax_path = get_metax_path(fpath, root_upload_path)

        if metax_path not in files_dict:
            raise MetaxClientError("Metadata not found in Metax")

        file_metadata = files_dict[metax_path]
        if file_metadata["storage_identifier"] != PAS_FILE_STORAGE_ID:
            raise MetaxClientError("Incorrect file storage")
        if not force and self.file_has_dataset(metax_path, files_dict):
            raise MetaxClientError("Metadata is part of a dataset")
        if self.file_has_accepted_dataset(metax_path, files_dict):
            raise MetaxClientError(
                "Metadata is part of an accepted dataset"
            )

        file_id = six.text_type(file_metadata["id"])
        return self.client.delete_file(file_id)

    def delete_all_metadata(self, project, fpath, root_upload_path,
                            force=False):
        """Delete all file metadata from Metax found under dir fpath,
        which is not associated with any dataset and is stored in PAS
        file storage.

        If force parameter is True metadata is deleted if file belongs
        to a dataset not accepted to preservation.
        """
        self.dataset_cache.clear()
        files_dict = self.client.get_files_dict(project)
        files_to_delete = {}

        # Iterate through all files under dir fpath
        for dirpath, _, files in os.walk(fpath):
            for _file in files:
                fpath = os.path.join(dirpath, _file)
                metax_path = get_metax_path(fpath, root_upload_path)
                if metax_path not in files_dict:
                    continue
                storage_id = files_dict[metax_path]["storage_identifier"]
                if storage_id != PAS_FILE_STORAGE_ID:
                    continue

                files_to_delete[metax_path] = files_dict[metax_path]

        if force:
            # Delete metadata for files which don't belong to accepted
            # datasets
            # FIXME: Deleting all file metadata when 'force' is in use
            # is inefficient at the moment due to each check requiring
            # an API call.
            file_ids_to_delete = [
                file_["identifier"] for metax_path, file_
                in six.iteritems(files_to_delete)
                if not self.file_has_accepted_dataset(metax_path, files_dict)
            ]
        else:
            # Delete metadata for files that don't belong to datasets
            file_ids = [
                file_["identifier"] for file_
                in six.itervalues(files_to_delete)
            ]
            # Retrieve related datasets in a single bulk operation
            file2datasets = {}
            if file_ids:
                file2datasets = self.client.get_file2dataset_dict(file_ids)

            file_ids_to_delete = [
                file_["identifier"] for metax_path, file_
                in six.iteritems(files_to_delete)
                if not file2datasets.get(file_["identifier"], None)
            ]

        if not file_ids_to_delete:
            return {"deleted_files_count": 0}

        return self.client.delete_files(file_ids_to_delete)

    def get_all_ids(self, project_list):
        """Get a set of all identifiers of files in any of the projects
        in project_list.
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
        """Check if file belongs to any dataset."""
        if metax_path not in files_dict:
            return False

        file_id = files_dict[metax_path]["id"]
        datasets = self.client.get_file_datasets(file_id)
        return len(datasets) != 0

    def file_has_accepted_dataset(self, metax_path, files_dict):
        """Check if file belongs to dataset accepted to preservation."""
        if metax_path in files_dict:
            file_id = files_dict[metax_path]["id"]
            dataset_ids = self.client.get_file_datasets(file_id)
            for dataset_id in dataset_ids:
                if dataset_id not in self.dataset_cache:
                    dataset = self.client.get_dataset(dataset_id)
                    self.dataset_cache[dataset_id] = \
                        dataset['preservation_state']
                dataset_state = self.dataset_cache[dataset_id]
                if (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION <=
                        dataset_state <=
                        DS_STATE_IN_DIGITAL_PRESERVATION):
                    return True
        return False
