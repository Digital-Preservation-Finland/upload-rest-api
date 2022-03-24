"""Commandline interface for upload_rest_api package."""
import json
import os

import click

import upload_rest_api.config
import upload_rest_api.database as db
import upload_rest_api.gen_metadata as md
from upload_rest_api.cleanup import clean_disk, clean_mongo


@click.group()
def cli():
    """Upload REST API command line tool."""
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
    click.echo(f"Cleaned {deleted_count} file(s)")


def _cleanup_mongo():
    """Clean Mongo from file identifiers that are not found in Metax."""
    deleted_count = clean_mongo()
    click.echo(f"Cleaned {deleted_count} identifier(s) from Mongo")


def _list_users(username, users):
    """List users from Mongo."""
    database = db.Database()

    if users:
        users = database.get_all_users()
        if users:
            for user in users:
                click.echo(user)
        else:
            click.echo("No users found")
    elif username:
        try:
            user = database.user(username).get()
        except db.UserNotFoundError:
            click.echo(f"User '{username}' not found")
            return

        response = {
            "_id": user["_id"],
            "projects": user["projects"]
        }
        click.echo(json.dumps(response, indent=4))


def _list_projects(project_name, projects):
    """List projects from Mongo."""
    database = db.Database()

    if projects:
        projects = database.projects.get_all_projects()
        if projects:
            for project in projects:
                click.echo(project["_id"])
        else:
            click.echo("No projects found")
    elif project_name:
        project = database.projects.get(project_name)
        if project:
            click.echo(json.dumps(project, indent=4))
        else:
            click.echo(f"Project '{project_name}' not found")


def _list_checksums(checksum_query, checksums):
    """List checksums from Mongo."""
    database = db.Database()

    if checksums:
        checksums = database.checksums.get_checksums()
        if checksums:
            click.echo(json.dumps(checksums, indent=4))
        else:
            click.echo("No checksums found")

    elif checksum_query:
        checksum = database.checksums.get_checksum(checksum_query)
        if checksum:
            click.echo(checksum)
        else:
            click.echo(f"Checksum '{checksum_query}' not found")


def _list_identifiers(identifier, identifiers):
    """List identifiers from Mongo."""
    database = db.Database()

    if identifiers:
        identifiers = database.files.get_all_ids()
        if identifiers:
            for identifier in identifiers:
                click.echo(identifier)
        else:
            click.echo("No identifiers found")
    elif identifier:
        path = database.files.get_path(identifier)
        if path:
            click.echo(path)
        else:
            click.echo(f"Identifier '{identifier}' not found")


@cli.command("list")
@click.option("--user", help="List one user.")
@click.option("--users", is_flag=True, help="List all users.")
@click.option("--project", help="List one project.")
@click.option("--projects", is_flag=True, help="List all projects.")
@click.option("--identifier", help="List path based on Metax identifier.")
@click.option("--identifiers", is_flag=True,
              help="List all Metax identifiers.")
@click.option("--checksum", help="List one checksum.")
@click.option("--checksums", is_flag=True, help="List all checksums.")
def list_resources(user, users, project, projects, identifier, identifiers,
                   checksum, checksums):
    """List resources."""
    _list_users(user, users)
    _list_projects(project, projects)
    _list_checksums(checksum, checksums)
    _list_identifiers(identifier, identifiers)


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
    """Grant user access to projects."""
    user = db.Database().user(username)
    for project in projects:
        user.grant_project(project)
    click.echo(
        (f"Granted user '{username}' access to project(s): "
         f"{', '.join(projects)}")
    )


def _revoke_user_projects(username, projects):
    """Revoke user rights to access projects."""
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
    """Delete an existing user with spesified USERNAME."""
    db.Database().user(username).delete()
    click.echo(f"Deleted user '{username}'")


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
@click.option("--quota", required=True, type=click.IntRange(min=0),
              help="Set project quota in bytes.")
def create_project(project, quota):
    """Create a new PROJECT."""
    project = db.Database().projects.create(
        identifier=project, quota=quota
    )
    click.echo(json.dumps(project, indent=4))


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
    click.echo(json.dumps(project, indent=4))


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
    click.echo(f"Created identifiers written to {output}")


if __name__ == '__main__':
    cli()
