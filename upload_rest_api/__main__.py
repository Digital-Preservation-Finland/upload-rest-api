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
    _setup_cleanup_files_args(subparsers)
    _setup_cleanup_mongo_args(subparsers)
    _setup_get_args(subparsers)
    _setup_create_args(subparsers)
    _setup_delete_args(subparsers)
    _setup_modify_args(subparsers)

    # Parse arguments and return the arguments
    return parser.parse_args()


def _setup_cleanup_files_args(subparsers):
    """Define cleanup-files subparser and its arguments."""
    parser = subparsers.add_parser(
        'cleanup-files', help='Clean files from disk'
    )
    parser.set_defaults(func=_cleanup_files)


def _setup_cleanup_mongo_args(subparsers):
    """Define cleanup-mongo subparser and its arguments."""
    parser = subparsers.add_parser(
        'cleanup-mongo', help='Clean identfiers from mongo'
    )
    parser.set_defaults(func=_cleanup_mongo)


def _setup_get_args(subparsers):
    """Define get subparser and its arguments."""
    parser = subparsers.add_parser(
        'get', help='Get mongo documents'
    )
    parser.set_defaults(func=_get)
    parser.add_argument('--user', help="Get one user")
    parser.add_argument(
        '--users', action="store_true", default=False,
        help="Get all users"
    )
    parser.add_argument(
        '--identifier',
        help="Get path based on Metax identifier"
    )
    parser.add_argument(
        '--identifiers', action="store_true", default=False,
        help="Get all Metax identifiers"
    )
    parser.add_argument('--checksum', help="Get one checksum")
    parser.add_argument(
        '--checksums', action="store_true", default=False,
        help="Get all checksums"
    )


def _setup_create_args(subparsers):
    """Define create subparser and its arguments."""
    parser = subparsers.add_parser(
        'create', help='Create a new user'
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
    parser.add_argument('--quota', type=int, help="Change user's quota")
    parser.add_argument('--project', help="Change user's project")
    parser.add_argument(
        '--password', action="store_true", default=False,
        help="Generate new password"
    )


def _cleanup_files(args):
    """Clean files from the disk"""
    deleted_count = clean_disk()
    print("Cleaned %d files" % deleted_count)


def _cleanup_mongo(args):
    """Clean identifiers from the mongo"""
    deleted_count = clean_mongo()
    print("Cleaned %d identifiers" % deleted_count)


def _get_users(args):
    """Get users from mongo"""
    database = db.Database()

    if args.users:
        users = database.get_all_users()
        for user in users:
            print(user)
    elif args.user:
        try:
            user = database.user(args.user).get()
        except db.UserNotFoundError:
            print("User not found")
            return

        response = {
            "_id": user["_id"],
            "quota": user["quota"],
            "used_quota": user["used_quota"],
            "project": user["project"]
        }
        print(json.dumps(response, indent=4))


def _get_checksums(args):
    """Get checksums from mongo"""
    database = db.Database()

    if args.checksums:
        checksums = database.checksums.get_checksums()
        for checksum in checksums:
            print(json.dumps(checksum, indent=4))
    elif args.checksum:
        checksum = database.checksums.get_checksum(args.checksum)
        if checksum:
            print(checksum)
        else:
            print("Checksum not found")


def _get_identifiers(args):
    """Get identifiers from mongo"""
    database = db.Database()

    if args.identifiers:
        identifiers = database.files.get_all_ids()
        for identifier in identifiers:
            print(identifier)
    elif args.identifier:
        path = database.files.get_path(args.identifier)
        if path:
            print(path)
        else:
            print("Identifier not found")


def _get(args):
    """Get mongo documents"""
    _get_users(args)
    _get_checksums(args)
    _get_identifiers(args)


def _create(args):
    """Create a new user"""
    user = db.Database().user(args.username)
    passwd = user.create(args.project)
    print("%s:%s" % (args.username, passwd))


def _delete(args):
    """Delete an existing user"""
    db.Database().user(args.username).delete()
    print("Deleted")


def _modify(args):
    """Modify an existing user"""
    user = db.Database().user(args.username)
    if args.quota:
        user.set_quota(args.quota)
    if args.project:
        user.set_project(args.project)
    if args.password:
        passwd = user.change_password()

    user = user.get()
    response = {
        "_id": user["_id"],
        "quota": user["quota"],
        "project": user["project"]
    }
    if args.password:
        response["password"] = passwd

    print(json.dumps(response, indent=4))


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    # Parse arguments and call function defined by chosen subparser.
    args = _parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
