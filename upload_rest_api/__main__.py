"""Commandline interface for upload_rest_api package."""
import getpass
import json
import pathlib

import click

import upload_rest_api.config
from upload_rest_api.cleanup import clean_disk, clean_mongo, clean_tus_uploads
from upload_rest_api.models import (FileEntry, Project, ProjectEntry,
                                    TokenEntry, User, UserEntry)


def _echo_json(data):
    """Echo json data to stdout.

    :param data: json data to echo.
    :returns: None
    """
    if isinstance(data, list):
        try:
            for i, entry in enumerate(data):
                # Convert MongoEngine documents to dicts
                data[i] = entry.to_mongo()
        except AttributeError:
            pass
    else:
        try:
            # Convert MongoEngine document to dict
            data = data.to_mongo()
        except AttributeError:
            pass

    click.echo(json.dumps(data, indent=4))


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
    deleted_count = TokenEntry.objects.clean_session_tokens()
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
    user = User.create(username)
    passwd = user.generate_password()
    click.echo(f"{username}:{passwd}")


@users.group("project-rights")
def project_rights():
    """Manage user access to projects."""


@project_rights.command("grant")
@click.argument("username")
@click.argument("projects", nargs=-1)
def grant_user_projects(username, projects):
    """Grant USERNAME access to PROJECTS."""
    user = User.get(username=username)
    for project in projects:
        user.grant_project(project)
    click.echo(
        f"Granted user '{username}' access to project(s): "
        + ", ".join(projects)
    )


@project_rights.command("revoke")
@click.argument("username")
@click.argument("projects", nargs=-1)
def revoke_user_projects(username, projects):
    """Revoke USERNAME access to PROJECTS."""
    user = User.get(username=username)
    for project in projects:
        user.revoke_project(project)
    click.echo(
        f"Revoked user '{username}' access to project(s): "
        + ", ".join(projects)
    )


@users.command("delete")
@click.argument("username")
def delete_user(username):
    """Delete an existing user with specified USERNAME."""
    User.get(username=username).delete()
    click.echo(f"Deleted user '{username}'")


@users.command("modify")
@click.argument("username")
@click.option("--generate-password", is_flag=True,
              help="Generate new password.")
def modify_user(username, generate_password):
    """Modify an existing user with specified USERNAME."""
    user = User.get(username=username)
    if generate_password:
        passwd = user.generate_password()

    response = {
        "_id": user.username,
        "projects": user.projects
    }
    if generate_password:
        response["password"] = passwd

    _echo_json(response)


@users.command("get")
@click.argument("username")
def get_user(username):
    """Show information of USERNAME."""
    try:
        user = User.get(username=username)
    except User.DoesNotExist:
        click.echo(f"User '{username}' not found")
        return

    response = {
        "_id": user.username,
        "projects": user.projects
    }
    _echo_json(response)


@users.command("list")
def list_users():
    """List all users."""
    users = list(UserEntry.objects.only("username"))
    if users:
        for user in users:
            click.echo(user.username)
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
    project = Project.create(identifier=project, quota=quota)
    _echo_json(project)


@projects.command("modify")
@click.argument("project")
@click.option("--quota", type=click.IntRange(min=0),
              help="Set project quota in bytes.")
def modify_project(project, quota):
    """Modify an existing PROJECT."""
    try:
        project_ = Project.get(id=project)
    except Project.DoesNotExist:
        click.echo(f"Project '{project}' does not exist.")
        return

    if quota is not None:
        project_.set_quota(quota)

    _echo_json(project_)


@projects.command("delete")
@click.argument("project")
def delete_project(project):
    """Delete PROJECT."""
    Project.get(id=project).delete()
    click.echo(f"Project '{project}' was deleted")


@projects.command("list")
def list_projects():
    """List all projects."""
    projects = list(ProjectEntry.objects)
    if projects:
        for project in projects:
            click.echo(project.id)
    else:
        click.echo("No projects found")


@projects.command("get")
@click.argument("project")
def get_project(project):
    """Show information of PROJECT."""
    try:
        project = Project.get(id=project)
        _echo_json(project)
    except Project.DoesNotExist:
        click.echo(f"Project '{project}' not found")


@cli.group("files")
def files():
    """Manage files."""
    pass


@files.group("get")
def files_get():
    """Show information of a file."""
    pass


@files_get.command("path")
@click.argument("path")
def get_file_by_path(path):
    """Show information of file in PATH."""
    try:
        file_ = FileEntry.objects.get(path=path)
        _echo_json(file_)
    except FileEntry.DoesNotExist:
        click.echo(f"FileEntry not found in path '{path}'")


@files_get.command("identifier")
@click.argument("identifier")
def get_file_by_identifier(identifier):
    """Show information of file specified by IDENTIFIER."""
    try:
        file_ = FileEntry.objects.get(identifier=identifier)
        _echo_json(file_)
    except FileEntry.DoesNotExist:
        click.echo(f"FileEntry '{identifier}' not found")


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

    files = list(FileEntry.objects)
    if not files:
        click.echo("No files found")
    else:
        _echo_json(files)


def _list_file_identifiers():
    """List all file identifiers."""
    identifiers = [
        file_.identifier for file_
        in FileEntry.objects.only("identifier")
    ]
    if identifiers:
        _echo_json(identifiers)
    else:
        click.echo("No identifiers found")


def _list_checksums():
    """List all checksums of files."""
    checksums = [
        file_.checksum for file_ in
        FileEntry.objects.only("checksum")
    ]
    if checksums:
        _echo_json(checksums)
    else:
        click.echo("No checksums found")


if __name__ == '__main__':
    cli()
