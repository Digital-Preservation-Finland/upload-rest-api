"""Commandline interface for upload_rest_api package."""
import argparse
import json
import os

import upload_rest_api.config
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
from pymongo.errors import DuplicateKeyError
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
    _setup_get_user_args(subparsers)
    _setup_create_user_args(subparsers)
    _setup_grant_user_projects_args(subparsers)
    _setup_revoke_user_projects_args(subparsers)
    _setup_delete_user_args(subparsers)
    _setup_modify_user_args(subparsers)
    _setup_create_project_args(subparsers)
    _setup_modify_project_args(subparsers)
    _setup_delete_project_args(subparsers)
    _setup_migrate_database_projects_args(subparsers)

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


def _setup_get_user_args(subparsers):
    """Define get-user subparser and its arguments."""
    parser = subparsers.add_parser(
        'get', help='Get mongo documents'
    )
    parser.set_defaults(func=_get)
    parser.add_argument('--user', help="Get one user")
    parser.add_argument('--project', help="Get one project")
    parser.add_argument(
        '--users', action="store_true", default=False,
        help="Get all users"
    )
    parser.add_argument(
        '--projects', action="store_true", default=False,
        help="Get all projects"
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
    parser.add_argument('project')
    parser.add_argument(
        '-o', '--output', default="identifiers.txt", help="Output filepath"
    )


def _setup_create_user_args(subparsers):
    """Define create-user subparser and its arguments."""
    parser = subparsers.add_parser(
        'create-user', help='Create a new user'
    )
    parser.set_defaults(func=_create_user)
    parser.add_argument('username')


def _setup_grant_user_projects_args(subparsers):
    """Define grant-user-project subparser and its arguments."""
    parser = subparsers.add_parser(
        'grant-user-projects', help='Grant user access to project(s)'
    )
    parser.set_defaults(func=_grant_user_projects)
    parser.add_argument('username')
    parser.add_argument('project', nargs='+')


def _setup_revoke_user_projects_args(subparsers):
    """Define revoke-user-project subparser and its arguments."""
    parser = subparsers.add_parser(
        'revoke-user-projects', help='Revoke user access to project(s)'
    )
    parser.set_defaults(func=_revoke_user_projects)
    parser.add_argument('username')
    parser.add_argument('project', nargs='+')


def _setup_delete_user_args(subparsers):
    """Define delete-user subparser and its arguments."""
    parser = subparsers.add_parser(
        'delete-user', help='Delete an existing user'
    )
    parser.set_defaults(func=_delete_user)
    parser.add_argument('username')


def _setup_modify_user_args(subparsers):
    """Define modify-user subparser and its arguments."""
    parser = subparsers.add_parser(
        'modify-user', help='Modify an existing user'
    )
    parser.set_defaults(func=_modify_user)
    parser.add_argument('username')
    parser.add_argument(
        '--password', action="store_true", default=False,
        help="Generate new password"
    )


def _setup_create_project_args(subparsers):
    """Define create-project subparser and its arguments."""
    parser = subparsers.add_parser(
        'create-project', help='Create a new project'
    )
    parser.set_defaults(func=_create_project)
    parser.add_argument('project')
    parser.add_argument(
        '--quota', type=int, required=True, help="Set project quota"
    )


def _setup_modify_project_args(subparsers):
    """Define modify-project subparser and its arguments."""
    parser = subparsers.add_parser(
        'modify-project', help='Modify an existing project'
    )
    parser.set_defaults(func=_modify_project)
    parser.add_argument('project')
    parser.add_argument('--quota', type=int, help="Set project quota")


def _setup_delete_project_args(subparsers):
    """Define delete-project subparser and its arguments."""
    parser = subparsers.add_parser(
        'delete-project', help="Delete an existing project"
    )
    parser.set_defaults(func=_delete_project)
    parser.add_argument('project')


def _setup_migrate_database_projects_args(subparsers):
    """Define migrate-database-projects subparser and its arguments."""
    # TODO: Remove this CLI command once all environments have been migrated
    parser = subparsers.add_parser(
        'migrate-database-projects',
        help="Perform database migration to separate user and project entries"
    )
    parser.set_defaults(func=_migrate_database_projects)


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
            "projects": user["projects"]
        }
        print(json.dumps(response, indent=4))


def _get_projects(args):
    """Get projects from mongo."""
    database = db.Database()

    if args.projects:
        projects = database.projects.get_all_projects()
        for project in projects:
            print(project["_id"])
    elif args.project:
        project = database.projects.get(args.project)
        if project:
            print(json.dumps(project, indent=4))
        else:
            print("Project not found")


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
    _get_projects(args)
    _get_checksums(args)
    _get_identifiers(args)


def _generate_metadata(args):
    """Generate metadata for the specified project."""
    if os.path.exists(args.output):
        raise ValueError("Output file exists")

    conf = upload_rest_api.config.CONFIG
    project = args.project
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
        fpaths, conf["UPLOAD_PATH"], project, md.PAS_FILE_STORAGE_ID
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


def _create_user(args):
    """Create a new user."""
    user = db.Database().user(args.username)
    passwd = user.create()
    print("%s:%s" % (args.username, passwd))


def _grant_user_projects(args):
    """Grant user access to project(s)"""
    user = db.Database().user(args.username)
    for project in args.project:
        user.grant_project(project)


def _revoke_user_projects(args):
    """Revoke user access to project(s)"""
    user = db.Database().user(args.username)
    for project in args.project:
        user.revoke_project(project)


def _delete_user(args):
    """Delete an existing user."""
    db.Database().user(args.username).delete()
    print("Deleted")


def _modify_user(args):
    """Modify an existing user."""
    user = db.Database().user(args.username)
    if args.password:
        passwd = user.change_password()

    user = user.get()
    response = {
        "_id": user["_id"],
        "project": user["project"]
    }
    if args.password:
        response["password"] = passwd

    print(json.dumps(response, indent=4))


def _create_project(args):
    """Create a new project."""
    project = db.Database().projects.create(
        identifier=args.project, quota=args.quota
    )
    print(json.dumps(project, indent=4))


def _modify_project(args):
    """Modify an existing project."""
    database = db.Database()

    if not database.projects.get(args.project):
        print(f"Project '{args.project}' does not exist.")
        return

    if args.quota is not None:
        database.projects.set_quota(args.project, args.quota)

    project = database.projects.get(args.project)
    print(json.dumps(project, indent=4))


def _delete_project(args):
    """Delete a project."""
    db.Database().projects.delete(args.project)
    print("Project was deleted")


def _migrate_database_projects(args):
    """Perform database migration to separate users and projects."""
    database = db.Database()

    users_to_migrate = list(
        database.client.upload.users.find(
            {"quota": {"$exists": True}}
        )
    )

    print(f"{len(users_to_migrate)} user(s) to migrate")

    for user in users_to_migrate:
        print(f"Migrating user '{user['_id']}'")

        project = {
            "_id": user["project"],
            "quota": user["quota"],
            "used_quota": user["used_quota"]
        }
        # Create project first
        try:
            database.client.upload.projects.insert_one(project)
        except DuplicateKeyError:
            # Project already exists
            print(f"Project {user['project']} already exists! Skipping it...")

        # Update user
        database.client.upload.users.update(
            {"_id": user["_id"]},
            {
                "$unset": {"quota": 1, "used_quota": 1, "project": 1},
                "$set": {"projects": [user["project"]]}
            }
        )

        print(f"Migrated user '{user['_id']}'")


def main():
    """Parse command line arguments and execute the commands.

    :returns: None
    """
    # Parse arguments and call function defined by chosen subparser.
    args = _parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
