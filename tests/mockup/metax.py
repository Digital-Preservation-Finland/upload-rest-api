"""Metax mockup"""
from __future__ import unicode_literals

import os

from flask import current_app

import upload_rest_api.gen_metadata as md


def _get_metax_path(fpath):
    """Return file_path as stored in Metax"""
    upload_path = current_app.config.get("UPLOAD_PATH")
    return md.get_metax_path(fpath, upload_path)


class MockMetax(object):
    """Class for mocking MetaxClient"""

    def delete_file_metadata(self, project, fpath):
        """Return the file user is trying to remove from Metax"""
        return _get_metax_path(fpath)

    def delete_all_metadata(self, project, fpath):
        """Return a list of files user is trying to remove from Metax"""
        file_list = []

        for dirpath, _, files in os.walk(fpath):
            for _file in files:
                fpath = os.path.join(dirpath, _file)
                file_list.append(_get_metax_path(fpath))

        return file_list
