"""Commandline interface for upload_rest_api package"""
from __future__ import print_function

import argparse
import json

import upload_rest_api.database as db
from upload_rest_api.cleanup import clean_disk, clean_mongo


def _parse_args():
    """Parse command line arguments.

    :returns: Parsed arguments
    """
    # Parse commandline arguments
    parser = argparse.ArgumentParser(description="upload_rest_api CLI")

    # Add the alternative commands
    subparsers = parser.add_subparsers(title='Available commands')
    _setup_cleanup_args(subparsers)
    _setup_create_args(subparsers)
    _setup_delete_args(subparsers)
    _setup_modify_args(subparsers)

    # Parse arguments and return the arguments
    return parser.parse_args()


def _setup_cleanup_args(subparsers):
    """Define cleanup subparser and its arguments."""
    parser = subparsers.add_parser(
        'cleanup', help='Clean files from disk or identfiers from mongo.'
    )
    parser.set_defaults(func=_cleanup)
    parser.add_argument('location', help="mongo or disk")


def _setup_create_args(subparsers):
    """Define create subparser and its arguments."""
    parser = subparsers.add_parser(
        'create', help='Create new user'
    )
    parser.set_defaults(func=_create)
    parser.add_argument('username')
    parser.add_argument('project')


def _setup_delete_args(subparsers):
    """Define delete subparser and its arguments."""
    parser = subparsers.add_parser(
        'delete', help='Delete an existing user'
    )
    parser.set_defaults(func=_delete)
    parser.add_argument('username')


def _setup_modify_args(subparsers):
    """Define modify subparser and its arguments."""
    parser = subparsers.add_parser(
        'modify', help='Modify an existing user'
    )
    parser.set_defaults(func=_modify)
    parser.add_argument('username')
    parser.add_argument('--quota', type=int)
    parser.add_argument('--project')


def _cleanup(args):
    """Generate technical metadata for the dataset"""
    if args.location == "disk":
        deleted_count = clean_disk()
    elif args.location == "mongo":
        deleted_count = clean_mongo()
    else:
        raise ValueError("Unsupported location: %s" % args.location)

    print("Cleaned %d files" % deleted_count)


def _create(args):
    """Create a new user"""
    user = db.UsersDoc(args.username)
    passwd = user.create(args.project)
    print("%s:%s" % (args.username, passwd))


def _delete(args):
    """Delete an existing user"""
    db.UsersDoc(args.username).delete()
    print("Deleted")


def _modify(args):
    """Modify an existing user"""
    user = db.UsersDoc(args.username)
    if args.quota:
        user.set_quota(args.quota)
    if args.project:
        user.set_project(args.project)

    user = user.get()
    print(json.dumps(
        {
            "_id": user["_id"],
            "quota": user["quota"],
            "project": user["project"]
        },
        indent=4
    ))


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    # Parse arguments and call function defined by chosen subparser.
    args = _parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
