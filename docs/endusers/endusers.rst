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
    for i in {000..100}; do echo $i > data/test2/$i.txt; done

Usage
-----

filestorage API can be accessed at
:code:`https://passipservice.csc.fi/filestorage/api/`. Check your connection
to the API by sending a GET request to the root of API::

    curl https://passipservice.csc.fi/filestorage/api/ -u username:password | jq

If the server returns :code:`401: Unauthorized` the provided credentials 
:code:`username:password` were mistyped or the user does not exist. Server
should return :code:`404: Not found`, since no functionality is defined for the
root of the filestorage API. Let's upload files :code:`data/test1/file_?.txt`
to passipservice. This can be done by sending a POST request to 
:code:`/filestorage/api/files/v1/path/to/the/file`, where
:code:`/path/to/the/file` is the relative path to the file from your project
directory on the server. The project will be automatically prepended to the
path and thus does not need to be provided when sending requests to the API.
The two files can be uploaded with commands::

    curl https://passipservice.csc.fi/filestorage/api/files/v1/data/test1/file_1.txt -X POST -T data/test1/file_1.txt -u username:password | jq
    curl https://passipservice.csc.fi/filestorage/api/files/v1/data/test1/file_2.txt -X POST -T data/test1/file_2.txt -u username:password | jq

Here, flags :code:`-X` and :code:`-T` define request method and the actual data
sent respectively. Without any flags provided :code:`curl` sends a GET request
by default.
