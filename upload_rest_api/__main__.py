"""Commandline interface for upload_rest_api package."""
import os
import argparse
import json

import upload_rest_api.config
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
from upload_rest_api.cleanup import clean_disk, clean_mongo


def _parse_args():
    """Parse command line arguments.

    :returns: Parsed arguments
    """
    # Parse commandline arguments
    parser = argparse.ArgumentParser(description="upload_rest_api CLI")

    # Add the alternative commands
    subparsers = parser.add_subparsers(title='Available commands')
    _setup_cleanup_tokens_args(subparsers)
    _setup_cleanup_files_args(subparsers)
    _setup_cleanup_mongo_args(subparsers)
    _setup_generate_metadata_args(subparsers)
    _setup_get_args(subparsers)
    _setup_create_args(subparsers)
    _setup_delete_args(subparsers)
    _setup_modify_args(subparsers)

    # Parse arguments and return the arguments
    return parser.parse_args()


def _setup_cleanup_tokens_args(subparsers):
    """Define cleanup-tokens subparser and its arguments."""
    parser = subparsers.add_parser(
        'cleanup-tokens', help='Clean expired session tokens from database'
    )
    parser.set_defaults(func=_cleanup_tokens)


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


def _setup_generate_metadata_args(subparsers):
    """Define generate-metadata subparser and its arguments."""
    parser = subparsers.add_parser(
        'generate-metadata', help='Generate file metadata'
    )
    parser.set_defaults(func=_generate_metadata)
    parser.add_argument('user')
    parser.add_argument(
        '-o', '--output', default="identifiers.txt", help="Output filepath"
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


def _cleanup_tokens(_args):
    """Clean expired session tokens from the database"""
    database = db.Database()

    deleted_count = database.tokens.clean_session_tokens()
    print(f"Cleaned {deleted_count} expired token(s)")


def _cleanup_files(_args):
    """Clean files from the disk."""
    deleted_count = clean_disk()
    print("Cleaned %d files" % deleted_count)


def _cleanup_mongo(_args):
    """Clean identifiers from the mongo."""
    deleted_count = clean_mongo()
    print("Cleaned %d identifiers" % deleted_count)


def _get_users(args):
    """Get users from mongo."""
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
    """Get checksums from mongo."""
    database = db.Database()

    if args.checksums:
        checksums = database.checksums.get_checksums()
        print(json.dumps(checksums, indent=4))
    elif args.checksum:
        checksum = database.checksums.get_checksum(args.checksum)
        if checksum:
            print(checksum)
        else:
            print("Checksum not found")


def _get_identifiers(args):
    """Get identifiers from mongo."""
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
    """Get mongo documents."""
    _get_users(args)
    _get_checksums(args)
    _get_identifiers(args)


def _generate_metadata(args):
    """Generate metadata for the specified project."""
    if os.path.exists(args.output):
        raise ValueError("Output file exists")

    conf = upload_rest_api.config.CONFIG
    username = args.user
    project = db.Database().user(username).get_project()
    project_path = os.path.join(conf["UPLOAD_PATH"], project)
    metax_client = md.MetaxClient(conf["METAX_URL"],
                                  conf["METAX_USER"],
                                  conf["METAX_PASSWORD"],
                                  conf["METAX_SSL_VERIFICATION"])

    fpaths = []
    for dirpath, _, files in os.walk(project_path):
        for fname in files:
            fpaths.append(os.path.join(dirpath, fname))

    # POST metadata to Metax
    response = metax_client.post_metadata(
        fpaths, conf["UPLOAD_PATH"], username, md.PAS_FILE_STORAGE_ID
    )

    print("Success: %d" % len(response["success"]))
    print("Failed: %d" % len(response["failed"]))

    # Write created identifiers to output file
    with open(args.output, "w") as f_out:
        for _file_md in response["success"]:
            f_out.write("%s\t%s\t%s\t%s\n" % (
                _file_md["object"]["parent_directory"]["identifier"],
                _file_md["object"]["identifier"],
                _file_md["object"]["checksum"]["value"],
                _file_md["object"]["file_path"]
            ))


def _create(args):
    """Create a new user."""
    user = db.Database().user(args.username)
    passwd = user.create(args.project)
    print("%s:%s" % (args.username, passwd))


def _delete(args):
    """Delete an existing user."""
    db.Database().user(args.username).delete()
    print("Deleted")


def _modify(args):
    """Modify an existing user."""
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
