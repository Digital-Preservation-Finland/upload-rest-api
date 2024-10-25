Pre-ingest filestorage
======================
Temporary filestorage service for datasets to be packaged by packaging service. Provides REST API for file uploads.

Requirements
------------

Installation and usage requires Python 3.9 or newer.
The software is tested with Python 3.9 on AlmaLinux 9 release.

Installation using RPM packages (preferred)
-------------------------------------------

Installation on Linux distributions is done by using the RPM Package Manager.
See how to `configure the PAS-jakelu RPM repositories`_ to setup necessary software sources.

.. _configure the PAS-jakelu RPM repositories: https://www.digitalpreservation.fi/user_guide/installation_of_tools 

After the repository has been added, the package can be installed by running the following command::

    sudo dnf install python3-upload-rest-api

Usage
-----

Copy configuration file `include/etc/upload_rest_api.conf` to /etc/. Edit
MongoDB, Redis, and Metax configuration according to your system. Ensure that
filestorage service app has read&write permissions to directories configured in
configuration file.

Start local development/test server::

    python upload_rest_api/app.py

Start rq workers that read jobs from queues: `files`, and `upload`::

    rq worker -c upload_rest_api.rq_config --queue-class "upload_rest_api.jobs.BackgroundJobQueue" files upload

Create a user and project using CLI::

    upload-rest-api users create <username>
    upload-rest-api projects create --quota <quota> <project>
    upload-rest-api users project-rights grant <user> <project>

POST file::

    curl -X POST -T <path-to-file> -u <user:password> localhost:5000/v1/files/<project>/<path-to-file-on-server>

GET file::

    curl -u <user>:<password> localhost:5000/v1/files/<project>/<path-to-file-on-server>

DELETE file::

    curl -X DELETE -u <user>:<password> localhost:5000/v1/files/<project>/<path-to-file-on-server>


Installation using Python Virtualenv for development purposes
-------------------------------------------------------------

Install MongoDB::

    sudo bash -c "cat > /etc/yum.repos.d/mongodb-org.repo" << EOL
    [mongodb-org-6.0]
    name=MongoDB Repository
    baseurl=https://repo.mongodb.org/yum/redhat/\$releasever/mongodb-org/6.0/x86_64/
    gpgcheck=1
    enabled=1
    gpgkey=https://www.mongodb.org/static/pgp/server-6.0.asc
    EOL
    sudo dnf install mongodb-org

To avoid MongoDB connection errors caused by system resource limits, ensure
that user limits are high enough (see https://www.mongodb.com/docs/manual/reference/ulimit/
for more information). For example::

    ulimit -n 64000  # increase limit of open file descriptors for current session
    echo "vagrant soft nofile 64000" | sudo tee -a /etc/security/limits.conf  # make the change permanent

Create a virtual environment::

    python3 -m venv venv

Run the following to activate the virtual environment::

    source venv/bin/activate

Install the required software with commands::

    pip install --upgrade pip==20.2.4 setuptools
    pip install -r requirements_dev.txt
    pip install .

To deactivate the virtual environment, run ``deactivate``.
To reactivate it, run the ``source`` command above.

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
