passipservice filestorage API
=============================

filestorage API can be used to upload files directly to passipservice server
and generate the basic file metadata. This document is a hands-on tutorial,
which demonstrates how to use the interface.

Installation
------------

Using the interface only requires you to able to make HTTP requests and as such
doesn't need any special software. In this tutorial command-line tool :code:`curl`
is used to make the HTTP requests, which is pre-installed on most Linux
distributions. Optionally, :code:`jq` can also be installed, which parses the json
responses sent by the server and makes them more human-readable. Check that
curl and jq are installed by running the following command in terminal::

    sudo yum install curl jq

If you choose not to install jq, ignore all pipes to jq for the remainder of
this tutorial i.e. strip "| jq" from all the commands. Finally, let's make
some fake data that we want to upload to the filestorage::

    mkdir -p data/test1 data/test2
    echo "This is test file 1" > data/test1/file_1.txt
    echo "This is test file 2" > data/test1/file_2.txt
    for i in {00..99}; do echo $i > data/test2/$i.txt; done

Usage
-----

filestorage API can be accessed at
:code:`https://passipservice.csc.fi/filestorage/api/`. Check your connection
to the API by sending a GET request to the root of API::

    curl https://passipservice.csc.fi/filestorage/api/ -u username:password | jq

If the server returns :code:`401: Unauthorized` the provided credentials
:code:`username:password` were mistyped or the user does not exist. Server
should return :code:`404: Not found`, since no functionality is defined for the
root of the filestorage API.

POST files
~~~~~~~~~~

Let's upload files :code:`data/test1/file_?.txt`
to passipservice. This can be done by sending a POST request to
:code:`/filestorage/api/v1/files/path/to/the/file`, where
:code:`/path/to/the/file` is the path to the file on the server relative to
your project directory. It is later used as the file_path attribute in the file
metadata in Metax and thus can be used to define the directory structure of
the files in the dataset. Now, let's upload the two files with commands::

    curl https://passipservice.csc.fi/filestorage/api/v1/files/data/test1/file_1.txt -X POST -T data/test1/file_1.txt -u username:password | jq
    curl https://passipservice.csc.fi/filestorage/api/v1/files/data/test1/file_2.txt -X POST -T data/test1/file_2.txt -u username:password | jq

Here, flags :code:`-X` and :code:`-T` define request method and the actual data
sent respectively. Without any flags provided, :code:`curl` sends a GET request
by default. The aforementioned commands should return, file_path, md5 checksum
and status. Checksums of the sent files should always be checked to make sure
the files were not corrupted during the transfer. Checksums returned by the
server should always match the local checksums, which can be calculated with
command md5sum::

    md5sum data/test1/file_?.txt

Directory :code:`data/test2/` contains 100 files so uploading them individually
doesn't make sense. Writing a shell script that uploads each of them seperately
would work, but even that would accumulate latency and make uploading multiple
small files really slow. Thus, it's best to make a zip archive, and upload it.
The archive is extracted by the server automatically. zip :code:`data/test2/`
directory with command::

    zip -r test2.zip data/test2/

Upload the zip archive to the server::

    curl https://passipservice.csc.fi/filestorage/api/v1/files/test2.zip -X POST -T test2.zip -u username:password | jq

Again, it is recommended to check that the checksums match with command::

    md5sum test2.zip

GET files
~~~~~~~~~

Now that all the test files have been uploaded to the server let's check some
of them. All directories and filenames can be requested by sending a GET
request to :code:`/filestorage/api/v1/files`::

    curl https://passipservice.csc.fi/filestorage/api/v1/files -u username:password | jq

To get more info request an individual file with e.g.

::

    curl https://passipservice.csc.fi/filestorage/api/v1/files/data/test1/file_1.txt -u username:password | jq

POST file metadata
~~~~~~~~~~~~~~~~~~

Finally, you need to POST file metadata to Metax to be able the access
the files in Qvain. This can be done be sending a POST request to
:code:`/filestorage/api/v1/metadata/path/to/file/or/dir`. If the path
resolves to a directory, all metadata is generated and posted to Metax
recursively for all the files in that directory and all the subdirectories.
If the path resolves to a file, metadata is generated for only that file.
Metadata can be generated for all files with command::

    curl https://passipservice.csc.fi/filestorage/api/v1/metadata/* -X POST -u username:password | jq

Server returns `failed` and `success` lists. Success list contains all the
generated metadata that was successfully posted to Metax. Failed list
contains all the metadata that couldn't be posted to Metax and the
corresponding error codes.

DELETE files
~~~~~~~~~~~~

Files that were uploaded to sipservice can also be deleted. This deletes
the files from passipservice and file metadata from Metax, if it is not
associated with any dataset. Delete can be requested for the whole project
or a single file similar to the GET shown earlier. Following command deletes
all the files::

    curl https://passipservice.csc.fi/filestorage/api/v1/files -X DELETE -u username:password | jq

Files can be deleted from passipservice after the dataset has been accepted
for digital preservation. All the files will automatically be cleaned after
30 days based on the timestamp returned by
:code:`GET /filestorage/api/v1/files/path/to/the/file`.

Summary
~~~~~~~

Basic workflow for uploading the files and generating the metadata is as
follows:

    - Make a zip archive of the files: :code:`zip -r files.zip directory/`
    - Send the zip archive to passipservice:
      :code:`/filestorage/api/v1/files/path/to/the/dir -X POST -T files.zip`
    - Make sure the checksums match: :code:`md5sum files.zip`
    - Generate file metadata for all the files:
      :code:`/filestorage/api/v1/metadata/* -X POST`
