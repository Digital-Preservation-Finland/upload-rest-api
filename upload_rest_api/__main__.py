"""Commandline interface for upload_rest_api package."""
import getpass
import json
import os
import pathlib

import click

import upload_rest_api.config
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
from upload_rest_api.cleanup import clean_disk, clean_mongo, clean_tus_uploads


def _echo_dict(dictionary):
    """Echo a dictionary to stdout.

    :param dictionary: dictionary to echo.
    :returns: None
    """
    click.echo(json.dumps(dictionary, indent=4))


@click.group()
def cli():
    """Upload REST API command line tool."""
    base_path = pathlib.Path(
        upload_rest_api.config.CONFIG['UPLOAD_BASE_PATH']
    )
    if getpass.getuser() != base_path.owner():
        raise click.UsageError(
            f'The owner of base directory ({base_path}) is '
            f'{base_path.owner()}. Only the owner of base directory is '
            'allowed to run this script.'
        )


@cli.group()
def cleanup():
    """Clean up database and disk."""
    pass


@cleanup.command("tokens")
def cleanup_tokens():
    """Clean expired session tokens from the database."""
    database = db.Database()
    deleted_count = database.tokens.clean_session_tokens()
    click.echo(f"Cleaned {deleted_count} expired token(s)")


@cleanup.command("files")
def cleanup_files():
    """Clean files from the disk."""
    deleted_count = clean_disk()
    click.echo(f"Cleaned {deleted_count} file(s)")


@cleanup.command("mongo")
def cleanup_mongo():
    """Clean Mongo from old tasks and file identifiers that are not found in
    Metax.
    """
    deleted_count = clean_mongo()
    click.echo(
        f"Cleaned old tasks and {deleted_count} identifier(s) from Mongo"
    )


@cleanup.command("tus-uploads")
def cleanup_tus_uploads():
    """Clean old tus uploads without tus workspace directories."""
    deleted_count = clean_tus_uploads()
    click.echo(f"Cleaned {deleted_count} aborted tus upload(s)")


@cli.group()
def users():
    """Manage users and user project rights."""
    pass


@users.command("create")
@click.argument("username")
def create_user(username):
    """Create a new user with specified USERNAME."""
    user = db.Database().user(username)
    passwd = user.create()
    click.echo(f"{username}:{passwd}")


@users.group("project-rights")
def project_rights():
    """Manage user access to projects."""


@project_rights.command("grant")
@click.argument("username")
@click.argument("projects", nargs=-1)
def grant_user_projects(username, projects):
    """Grant USERNAME access to PROJECTS."""
    user = db.Database().user(username)
    for project in projects:
        user.grant_project(project)
    click.echo(
        (f"Granted user '{username}' access to project(s): "
         f"{', '.join(projects)}")
    )


@project_rights.command("revoke")
@click.argument("username")
@click.argument("projects", nargs=-1)
def revoke_user_projects(username, projects):
    """Revoke USERNAME access to PROJECTS."""
    user = db.Database().user(username)
    for project in projects:
        user.revoke_project(project)
    click.echo(
        (f"Revoked user '{username}' access to project(s): "
         f"{', '.join(projects)}")
    )


@users.command("delete")
@click.argument("username")
def delete_user(username):
    """Delete an existing user with specified USERNAME."""
    db.Database().user(username).delete()
    click.echo(f"Deleted user '{username}'")


@users.command("modify")
@click.argument("username")
@click.option("--generate-password", is_flag=True,
              help="Generate new password.")
def modify_user(username, generate_password):
    """Modify an existing user with specified USERNAME."""
    user = db.Database().user(username)
    if generate_password:
        passwd = user.change_password()

    user = user.get()
    response = {
        "_id": user["_id"],
        "projects": user["projects"]
    }
    if generate_password:
        response["password"] = passwd

    _echo_dict(response)


@users.command("get")
@click.argument("username")
def get_user(username):
    """Show information of USERNAME."""
    database = db.Database()

    try:
        user = database.user(username).get()
    except db.UserNotFoundError:
        click.echo(f"User '{username}' not found")
        return

    response = {
        "_id": user["_id"],
        "projects": user["projects"]
    }
    _echo_dict(response)


@users.command("list")
def list_users():
    """List all users."""
    database = db.Database()
    users = database.get_all_users()
    if users:
        for user in users:
            click.echo(user)
    else:
        click.echo("No users found")


@cli.group()
def projects():
    """Manage projects."""
    pass


@projects.command("create")
@click.argument("project")
@click.option("--quota", required=True, type=click.IntRange(min=0),
              help="Set project quota in bytes.")
def create_project(project, quota):
    """Create a new PROJECT."""
    project = db.Database().projects.create(
        identifier=project, quota=quota
    )
    _echo_dict(project)


@projects.command("modify")
@click.argument("project")
@click.option("--quota", type=click.IntRange(min=0),
              help="Set project quota in bytes.")
def modify_project(project, quota):
    """Modify an existing PROJECT."""
    database = db.Database()

    if not database.projects.get(project):
        click.echo(f"Project '{project}' does not exist.")
        return

    if quota is not None:
        database.projects.set_quota(project, quota)

    project = database.projects.get(project)
    _echo_dict(project)


@projects.command("delete")
@click.argument("project")
def delete_project(project):
    """Delete PROJECT."""
    db.Database().projects.delete(project)
    click.echo(f"Project '{project}' was deleted")


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
    """Generate metadata for PROJECT."""
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
    click.echo(f"Created identifiers written to {output}")


@projects.command("list")
def list_projects():
    """List all projects."""
    database = db.Database()

    projects = database.projects.get_all_projects()
    if projects:
        for project in projects:
            click.echo(project["_id"])
    else:
        click.echo("No projects found")


@projects.command("get")
@click.argument("project")
def get_project(project):
    """Show information of PROJECT."""
    database = db.Database()

    project_entry = database.projects.get(project)
    if project_entry:
        _echo_dict(project_entry)
    else:
        click.echo(f"Project '{project}' not found")


@cli.group("files")
def files():
    """Manage files."""
    pass


@files.group("get")
def files_get():
    """Show information of a file."""
    pass


def _create_file_response(identifier, checksum, file_path):
    """Create a dict holding the given file information.

    :param identifier: File identifier
    :param checksum: File checksum
    :param path: File path:
    :returns: Dict holding the given information
    """
    return {
        "_id": identifier,
        "checksum": checksum,
        "file_path": file_path
    }


@files_get.command("path")
@click.argument("path")
def get_file_by_path(path):
    """Show information of file in PATH."""
    database = db.Database()

    checksum = database.checksums.get_checksum(path)
    identifier = database.files.get_identifier(path)
    if checksum and identifier:
        response = _create_file_response(identifier, checksum, path)
        _echo_dict(response)
    else:
        click.echo(f"File not found in path '{path}'")


@files_get.command("identifier")
@click.argument("identifier")
def get_file_by_identifier(identifier):
    """Show information of file specified by IDENTIFIER."""
    database = db.Database()
    path = database.files.get_path(identifier)
    checksum = database.checksums.get_checksum(path)
    if path and checksum:
        response = _create_file_response(identifier, checksum, path)
        _echo_dict(response)
    else:
        click.echo(f"File '{identifier}' not found")


@files.command("list")
@click.option("--identifiers-only", is_flag=True)
@click.option("--checksums-only", is_flag=True)
def list_files(identifiers_only, checksums_only):
    """List all files."""
    if identifiers_only:
        _list_file_identifiers()
        return

    if checksums_only:
        _list_checksums()
        return

    database = db.Database()

    files = database.files.get_all_files_with_checksums()
    if not files:
        click.echo("No files found")
    else:
        _echo_dict(files)


def _list_file_identifiers():
    """List all file identifiers."""
    database = db.Database()

    identifiers = database.files.get_all_ids()
    if identifiers:
        for identifier in identifiers:
            click.echo(identifier)
    else:
        click.echo("No identifiers found")


def _list_checksums():
    """List all checksums of files."""
    database = db.Database()

    checksums = database.checksums.get_checksums()
    if checksums:
        _echo_dict(checksums)
    else:
        click.echo("No checksums found")


# TODO: This command can be removed once the database migration is finished.
@cli.command("migrate-db")
def migrate_db():
    """Migrate database to merge files and checksums collections into one
    collection.

    Collects existing files and turns them into the following form:
    {
        "_id" : <file path>,
        "identifier" : <metax identifier>,
        "checksum" : <checksum>
    }

    Once new files documents are inserted into the database, the old file
    documents and the checksums collection are removed. Also, index for the
    identifier field in the new files collection is created to speed up
    queries.
    """
    database = db.Database()

    # Pipeline to aggregate all existing files into documents with the new
    # form
    pipeline = [
        # Join files collection with checsum collection as an array called
        # fromItems
        {
            "$lookup": {
                "from": "files",
                "localField": "_id",
                "foreignField": "file_path",
                "as": "fromItems"
            }
        },
        # Create new document for each document in fromItems, even when the
        # fromItems array is empty (creating a file document without file
        # identifier in cases when file metadata has not been generated)
        {
            "$unwind": {
                "path": "$fromItems",
                "preserveNullAndEmptyArrays": True
            }
        },
        # Include following fields in the new documents
        {
            "$project": {
                "_id": "$_id",
                "checksum": "$checksum",
                "identifier": "$fromItems._id"
            }
        }
    ]
    documents = list(database.checksums.checksums.aggregate(pipeline))

    # Insert new file documents with the checksum field
    database.files.insert(documents)
    # Remove old file documents (identified by not having the checksum field)
    database.files.files.remove({"checksum": {"$exists": False}})
    # Drop checksums collection
    database.checksums.checksums.drop()

    # Create index for identifier field (for documents where identifier exists)
    # to speed up identifier queries
    database.files.files.create_index(
        "identifier",
        unique=True,
        partialFilterExpression={"identifier": {"$exists": True}}
    )


if __name__ == '__main__':
    cli()
