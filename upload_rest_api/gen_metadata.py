"""Module for managing metadata in Metax."""
import os
import pathlib

from metax_access import (DS_STATE_ACCEPTED_TO_DIGITAL_PRESERVATION,
                          DS_STATE_IN_DIGITAL_PRESERVATION, Metax)

from upload_rest_api.config import CONFIG
from upload_rest_api.models.file_entry import FileEntry
from upload_rest_api.models.project import Project

LANGUAGE_IDENTIFIERS = {
    "http://lexvo.org/id/iso639-3/eng": "en",
    "http://lexvo.org/id/iso639-3/fin": "fi",
    "http://lexvo.org/id/iso639-3/swe": "sv"
}


def get_metax_path(fpath, root_upload_path):
    """Return file_path that is stored in Metax."""
    file_path = "/%s" % fpath.relative_to(root_upload_path)
    file_path = os.path.abspath(file_path)
    project = file_path.split("/")[1]

    return file_path[len(project)+1:]


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


class MetaxClient:
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

    def post_metadata(self, metadata_dicts):
        """Post multiple file metadata dictionaries to Metax.

        :param metadata_dicts: List of file metadata dictionaries
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
        metadata = []
        responses = []

        # Post file metadata to Metax, 5000 files at time. Larger amount
        # would cause performance issues.
        i = 0
        for metadata_dict in metadata_dicts:
            metadata.append(metadata_dict)

            i += 1
            if i % 5000 == 0:
                response = self.client.post_file(metadata)
                responses.append(_strip_metax_response(response))
                metadata = []

        # POST remaining metadata
        if metadata:
            response = self.client.post_file(metadata)
            responses.append(_strip_metax_response(response))

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
            file_["identifier"] for file_ in files_dict.values()
        ]
        file2datasets = {}
        if file_ids:
            file2datasets = self.client.get_file2dataset_dict(file_ids)

        # Delete metadata if file exists in fpaths AND it doesn't have
        # any datasets
        file_ids_to_delete = []
        for metax_path, file_ in files_dict.items():
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

        :param project: Project identifier
        :param fpath: Absolute file path
        :param root_upload_path: Root of the projects directory
        :param bool force: Whether to force deletion of a file belonging to a
                           pending dataset not yet in preservation
        """
        self.dataset_cache.clear()
        files_dict = self.client.get_files_dict(project)
        metax_path = get_metax_path(fpath, root_upload_path)

        if metax_path not in files_dict:
            raise MetaxClientError("Metadata not found in Metax")

        file_metadata = files_dict[metax_path]
        if file_metadata["storage_identifier"] != CONFIG["STORAGE_ID"]:
            raise MetaxClientError("Incorrect file storage")
        if not force and self.file_has_dataset(metax_path, files_dict):
            raise MetaxClientError("Metadata is part of a dataset")
        if self.file_has_accepted_dataset(metax_path, files_dict):
            raise MetaxClientError(
                "Metadata is part of an accepted dataset"
            )

        file_id = str(file_metadata["identifier"])

        self.client.delete_file(file_id)
        return {'deleted_files_count': 1}

    def delete_all_metadata(self, project, fpath, root_upload_path,
                            force=False):
        """Delete all file metadata from Metax found under dir fpath,
        which is not associated with any dataset and is stored in PAS
        file storage.

        If force parameter is True metadata is deleted if file belongs
        to a dataset not accepted to preservation.

        :param str project: Project name
        :param fpath: Absolute path to the directory to delete from Metax
        :param root_upload_path: Absolute path to the directory containing
                                 the project directory and the rest of `fpath`.
        :param bool force: Force deletion of metadata if file belongs to
                           dataset. Default is False.
        """
        self.dataset_cache.clear()
        files_dict = self.client.get_files_dict(project)
        files_to_delete = {}

        # Iterate through all files under dir fpath
        for dirpath, _, files in os.walk(fpath):
            for _file in files:
                fpath = os.path.join(dirpath, _file)
                metax_path = get_metax_path(pathlib.Path(fpath),
                                            root_upload_path)
                if metax_path not in files_dict:
                    continue
                storage_id = files_dict[metax_path]["storage_identifier"]
                if storage_id != CONFIG["STORAGE_ID"]:
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
                in files_to_delete.items()
                if not self.file_has_accepted_dataset(metax_path, files_dict)
            ]
        else:
            # Delete metadata for files that don't belong to datasets
            file_ids = [
                file_["identifier"] for file_
                in files_to_delete.values()
            ]
            # Retrieve related datasets in a single bulk operation
            file2datasets = {}
            if file_ids:
                file2datasets = self.client.get_file2dataset_dict(file_ids)

            file_ids_to_delete = [
                file_["identifier"] for metax_path, file_
                in files_to_delete.items()
                if not file2datasets.get(file_["identifier"], None)
            ]

        if not file_ids_to_delete:
            return {"deleted_files_count": 0}

        return self.client.delete_files(file_ids_to_delete)

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

    def get_datasets(self, dataset_ids):
        """Get list of datasets with given IDs.

        :param dataset_ids: List of dataset IDs
        """
        def _dataset_to_result(dataset):
            language_identifiers = [
                LANGUAGE_IDENTIFIERS[language["identifier"]]
                for language in dataset["research_dataset"].get("language", [])
                if LANGUAGE_IDENTIFIERS.get(language["identifier"], None)
            ]

            return {
                "title": dataset["research_dataset"]["title"],
                "languages": language_identifiers,
                "identifier": dataset["identifier"],
                "preservation_state": dataset["preservation_state"]
            }

        if not dataset_ids:
            return []

        count = 0
        total_count = 0
        result = self.client.get_datasets_by_ids(
            dataset_ids,
            fields=["identifier", "preservation_state", "research_dataset"]
        )
        datasets = []

        while True:
            total_count = result["count"]

            for dataset in result["results"]:
                datasets.append(_dataset_to_result(dataset))
                count += 1

            if count >= total_count:
                break

            # We haven't retrieved all datasets yet
            result = self.client.list_datasets(
                dataset_ids,
                fields=[
                    "identifier", "preservation_state", "research_dataset"
                ],
                offset=count
            )

        return datasets

    def get_file_datasets(self, project, path):
        """Get the datasets associated with the given file or directory.

        :param str project: Project identifier
        :param str path: Relative project path
        :raises ValueError: If file or directory does not exist
        """
        upload_path = Project.get(id=project).get_upload_path(path)

        file_identifiers = []

        if upload_path.is_file():
            file_identifier = (
                FileEntry.objects.only("identifier")
                         .get(path=str(upload_path))
                         .identifier
            )
            file_identifiers.append(file_identifier)
        elif upload_path.is_dir():
            file_identifiers = [
                file_["identifier"]
                for file_ in FileEntry.objects.in_dir(upload_path)
                if "identifier" in file_
            ]

        if not file_identifiers:
            return []

        # Retrieve file -> dataset(s) associations
        file2dataset = self.client.get_file2dataset_dict(file_identifiers)
        dataset_ids = set()
        for dataset_ids_ in file2dataset.values():
            dataset_ids |= set(dataset_ids_)
        dataset_ids = list(dataset_ids)

        # Retrieve additional information about datasets
        return self.get_datasets(dataset_ids)
