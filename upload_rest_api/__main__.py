"""Commandline interface for upload_rest_api package."""
import json
import os

import click

import upload_rest_api.config
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
from pymongo.errors import DuplicateKeyError
from upload_rest_api.cleanup import clean_disk, clean_mongo


@click.group()
def cli():
    """upload_rest_api CLI"""
    pass


@cli.command()
@click.option(
    "--tokens", is_flag=True,
    help="Clean expired session tokens from the database.")
@click.option(
    "--files", is_flag=True, help="Clean files from the disk.")
@click.option(
    "--mongo", is_flag=True,
    help="Clean Mongo from file identifiers that are not found in Metax.")
def cleanup(tokens, files, mongo):
    """Clean up database and disk."""
    if tokens:
        _cleanup_tokens()
    if files:
        _cleanup_files()
    if mongo:
        _cleanup_mongo()


def _cleanup_tokens():
    """Clean expired session tokens from the database."""
    database = db.Database()
    deleted_count = database.tokens.clean_session_tokens()
    click.echo(f"Cleaned {deleted_count} expired token(s)")


def _cleanup_files():
    """Clean files from the disk."""
    deleted_count = clean_disk()
    click.echo(f"Cleaned {deleted_count} files")


def _cleanup_mongo():
    """Clean identifiers from mongo."""
    deleted_count = clean_mongo()
    click.echo(f"Cleaned {deleted_count} identifiers from Mongo")


def _get_users(user, users):
    """Get users from mongo."""
    database = db.Database()

    if users:
        users = database.get_all_users()
        for user in users:
            click.echo(user)
    elif user:
        try:
            user = database.user(user).get()
        except db.UserNotFoundError:
            click.echo("User not found")
            return

        response = {
            "_id": user["_id"],
            "projects": user["projects"]
        }
        click.echo(json.dumps(response, indent=4))


def _get_projects(project, projects):
    """Get projects from mongo."""
    database = db.Database()

    if projects:
        projects = database.projects.get_all_projects()
        for project in projects:
            click.echo(project["_id"])
    elif project:
        project = database.projects.get(project)
        if project:
            click.echo(json.dumps(project, indent=4))
        else:
            click.echo("Project not found")


def _get_checksums(checksum, checksums):
    """Get checksums from mongo."""
    database = db.Database()

    if checksums:
        checksums = database.checksums.get_checksums()
        click.echo(json.dumps(checksums, indent=4))
    elif checksum:
        checksum = database.checksums.get_checksum(checksum)
        if checksum:
            click.echo(checksum)
        else:
            click.echo("Checksum not found")


def _get_identifiers(identifier, identifiers):
    """Get identifiers from mongo."""
    database = db.Database()

    if identifiers:
        identifiers = database.files.get_all_ids()
        for identifier in identifiers:
            click.echo(identifier)
    elif identifier:
        path = database.files.get_path(identifier)
        if path:
            click.echo(path)
        else:
            click.echo("Identifier not found")


@cli.command()
@click.option("--user", help="Get one user")
@click.option("--users", is_flag=True, help="Get all users")
@click.option("--project", help="Get one project")
@click.option("--projects", is_flag=True, help="Get all projects")
@click.option("--identifier", help="Get path based on Metax identifier")
@click.option("--identifiers", is_flag=True, help="Get all Metax identifiers")
@click.option("--checksum", help="Get one checksum")
@click.option("--checksums", is_flag=True, help="Get all checksums")
def get(user, users, project, projects, identifier, identifiers, checksum,
        checksums):
    """Get mongo documents."""
    _get_users(user, users)
    _get_projects(project, projects)
    _get_checksums(checksum, checksums)
    _get_identifiers(identifier, identifiers)


@cli.group()
def users():
    """Manage users and user rights."""
    pass


@users.command("create")
@click.argument("username")
def create_user(username):
    """Create a new user with spesified USERNAME."""
    user = db.Database().user(username)
    passwd = user.create()
    click.echo(f"{username}:{passwd}")


@users.command("project-rights")
@click.argument("username")
@click.argument("projects", nargs=-1)
@click.option("--grant", is_flag=True, help="Grant access to PROJECTS.")
@click.option("--revoke", is_flag=True, help="Revoke access to PROJECTS.")
def user_project_rights(username, projects, grant, revoke):
    """Manage USERNAME's access to PROJECTS."""
    if (grant and revoke) or (not grant and not revoke):
        raise click.UsageError("Set one and only one of --grant or --revoke.")

    if grant:
        _grant_user_projects(username, projects)
    elif revoke:
        _revoke_user_projects(username, projects)


def _grant_user_projects(username, projects):
    """Grant user access to projects"""
    user = db.Database().user(username)
    for project in projects:
        user.grant_project(project)


def _revoke_user_projects(username, projects):
    """Revoke user rights to access projects"""
    user = db.Database().user(username)
    for project in projects:
        user.revoke_project(project)


@users.command("delete")
@click.argument("username")
def delete_user(username):
    """Delete an existing user with spesified USERNAME."""
    db.Database().user(username).delete()
    click.echo("Deleted")


@users.command("modify")
@click.argument("username")
@click.option("--password", is_flag=True, help="Generate new password.")
def modify_user(username, password):
    """Modify an existing user with spesified USERNAME."""
    user = db.Database().user(username)
    if password:
        passwd = user.change_password()

    user = user.get()
    response = {
        "_id": user["_id"],
        "projects": user["projects"]
    }
    if password:
        response["password"] = passwd

    click.echo(json.dumps(response, indent=4))


@cli.group()
def projects():
    """Manage projects."""
    pass


@projects.command("create")
@click.argument("project")
@click.option("--quota", required=True, type=int, help="Set project quota.")
def create_project(project, quota):
    """Create a new PROJECT."""
    project = db.Database().projects.create(
        identifier=project, quota=quota
    )
    click.echo(json.dumps(project, indent=4))


@projects.command("modify")
@click.argument("project")
@click.option("--quota", type=int, help="Set project quota.")
def modify_project(project, quota):
    """Modify an existing PROJECT."""
    database = db.Database()

    if not database.projects.get(project):
        click.echo(f"Project '{project}' does not exist.")
        return

    if quota:
        database.projects.set_quota(project, quota)

    project = database.projects.get(project)
    click.echo(json.dumps(project, indent=4))


@projects.command("delete")
@click.argument("project")
def delete_project(project):
    """Delete PROJECT."""
    db.Database().projects.delete(project)
    click.echo("Project was deleted")


@projects.command("generate-metadata")
@click.argument("project")
@click.option(
    "-o", "--output",
    type=click.Path(
        dir_okay=False, writable=True, allow_dash=True),
    default="identifiers.txt",
    show_default=True,
    help="Output filepath."
)
def generate_metadata(project, output):
    """Generate metadata for the specified PROJECT."""
    if os.path.exists(output):
        raise ValueError("Output file exists")

    conf = upload_rest_api.config.CONFIG
    project_path = os.path.join(conf["UPLOAD_PROJECTS_PATH"], project)
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
        fpaths, conf["UPLOAD_PROJECTS_PATH"], project, md.PAS_FILE_STORAGE_ID
    )

    click.echo(f"Success: {len(response['success'])}")
    click.echo(f"Failed: {len(response['failed'])}")

    # Write created identifiers to output file
    with click.open_file(output, "wt", encoding="utf-8") as f_out:
        for _file_md in response["success"]:
            # pylint: disable=consider-using-f-string
            f_out.write("{}\t{}\t{}\t{}\n".format(
                _file_md["object"]["parent_directory"]["identifier"],
                _file_md["object"]["identifier"],
                _file_md["object"]["checksum"]["value"],
                _file_md["object"]["file_path"]
            ))


# TODO: Remove this CLI command once all environments have been migrated
@cli.command("migrate-database-projects")
def migrate_database_projects():
    """Perform database migration to separate users and projects."""
    database = db.Database()

    users_to_migrate = list(
        database.client.upload.users.find(
            {"quota": {"$exists": True}}
        )
    )

    click.echo(f"{len(users_to_migrate)} user(s) to migrate")

    for user in users_to_migrate:
        click.echo(f"Migrating user '{user['_id']}'")

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
            click.echo(
                f"Project {user['project']} already exists! Skipping it...")

        # Update user
        database.client.upload.users.update(
            {"_id": user["_id"]},
            {
                "$unset": {"quota": 1, "used_quota": 1, "project": 1},
                "$set": {"projects": [user["project"]]}
            }
        )

        click.echo(f"Migrated user '{user['_id']}'")


if __name__ == '__main__':
    cli()
