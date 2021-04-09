Pre-ingest filestorage
======================
Temporary filestorage service for datasets to be packaged by packaging service. Provides REST API for file uploads and technical metadata generation.

Usage
-----
Development
^^^^^^^^^^^
Copy configuration file `include/etc/upload_rest_api.conf` to /etc/. Edit
MongoDB, Redis, and Metax configuration according to your system. Ensure that
filestorage service app has read&write permissions to directories configured in
configuration file.

Create virtual environment and install requirements::

    virtualenv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements_dev.txt

Start local development/test server::

    python upload_rest_api/app.py

Start rq workers that read jobs from queues: `files`, `metadata`, and `upload`::

    rq worker files metadata upload

Create a user using CLI. The command returns username and password::

    python upload_rest_api create <user> <project>

POST file::

    curl -X POST -T <path-to-file> -u <user:password> localhost:5000/v1/files/<path-to-file-on-server>

GET file::

    curl -u <user>:<password> localhost:5000/v1/files/<path-to-file-on-server>

DELETE file::

    curl -X DELETE -u <user>:<password> localhost:5000/v1/files/<path-to-file-on-server>

POST file metadata to Metax::

    curl -X POST -u <user>:<password> localhost:5000/v1/metadata/<path-to-file-or-dir>

If the given path resolves to a directory, all files inside the directory and its
subdirectories are posted to Metax. POST metadata of all uploaded files to Metax::

    curl -X POST -u <user>:<password> localhost:5000/v1/metadata/*

DELETE file metadata from Metax::

    curl -X DELETE -u <user>:<password> localhost:5000/v1/metadata/<path-to-file-or-dir>

If the given path resolves to a directory, all files inside the directory and its
subdirectories are deleted from Metax. Delete metadata of all uploaded files from Metax::

    curl -X DELETE -u <user>:<password> localhost:5000/v1/metadata/*

Copyright
---------
Copyright (C) 2018 CSC - IT Center for Science Ltd.

This program is free software: you can redistribute it and/or modify it under the terms
of the GNU Lesser General Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with
this program.  If not, see <https://www.gnu.org/licenses/>.
