upload-rest-api
===============
REST API for file uploads to passipservice.csc.fi.

Usage
-----
Development
^^^^^^^^^^^
Create virtual environment and install requirements::

    virtualenv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements_dev.txt

Start local development/test server::

    python upload_rest_api/app.py

POST file::

    curl -X POST -T path/to/file.txt -u user:passwd localhost:5000/api/upload/v1/path/on/server/file.txt

GET file::

    curl -u user:passwd localhost:5000/api/upload/v1/path/on/server/file.txt

DELETE file::

    curl -X DELETE -u user:passwd localhost:5000/api/upload/v1/path/on/server/file.txt

admin user can manage user database through the db API::

    curl -X GET/POST/DELETE -u admin:passwd localhost:5000/api/db/v1/user

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
