import hashlib
import random
import string

from bson.binary import Binary
from mongoengine import (BinaryField, Document, ListField, NotUniqueError,
                         StringField, ValidationError)

from upload_rest_api import models

# Password vars
PASSWD_LEN = 20
SALT_LEN = 20


# Hashing vars
ITERATIONS = 200000
HASH_ALG = "sha512"


def get_random_string(chars):
    """Generate random string.

    String contains given number of ascii letters or digits.

    :param chars: Lenght of the string to generate
    :returns: Generated random string
    """
    passwd = ""
    rand = random.SystemRandom()
    for _ in range(chars):
        passwd += rand.choice(string.ascii_letters + string.digits)

    return passwd


def hash_passwd(password, salt):
    """Salt and hash password.

    PBKDF2 with HMAC PRNG and SHA512 hashing
    algorithm is used.

    :returns: hexadecimal representation of the 512 bit digest
    """
    digest = hashlib.pbkdf2_hmac(
        HASH_ALG, password.encode("utf-8"), salt.encode("utf-8"), ITERATIONS)
    return Binary(digest)


def _validate_projects(projects):
    if len(projects) == 0:
        # Nothing to check
        return

    projects = set(projects)
    existing_projects = set(
        project.id for project in
        models.ProjectEntry.objects.filter(id__in=projects).only("id")
    )

    missing_projects = projects - existing_projects

    if missing_projects:
        raise ValidationError(
            f"Projects don't exist: {','.join(missing_projects)}"
        )


class UserExistsError(Exception):
    """Exception for trying to create a user, which already exists."""


class User(Document):
    """Database collection for users"""
    username = StringField(primary_key=True, required=True)

    # Salt and digest if password authentication is enabled for this user.
    salt = StringField(required=False, null=True, default=None)
    digest = BinaryField(required=False, null=True, default=None)

    # Projects this user has access to
    projects = ListField(StringField(), validation=_validate_projects)

    meta = {"collection": "users"}

    @classmethod
    def create(cls, username, projects=None, password=None):
        """Add new user to the database.

        Salt is always generated randomly, but password can be set by
        providing to optional argument password.

        :param projects: Projects the user is associated with.
        :param password: Password of the created user
        :returns: The password
        """
        new_user = cls(username=username)
        if projects is None:
            projects = []

        new_user.projects = projects

        if password is not None:
            passwd = password
        else:
            passwd = get_random_string(PASSWD_LEN)

        new_user.salt = get_random_string(SALT_LEN)
        new_user.digest = hash_passwd(passwd, new_user.salt)

        try:
            new_user.save(force_insert=True)
        except NotUniqueError as exc:
            raise UserExistsError(
                f"User '{username}' already exists"
            ) from exc
        return new_user

    def change_password(self):
        """Change user password."""
        passwd = get_random_string(PASSWD_LEN)
        self.salt = get_random_string(SALT_LEN)
        self.digest = hash_passwd(passwd, self.salt)
        self.save()

        return passwd

    def grant_project(self, project):
        """Grant user access to the given project."""
        project = models.Project.get(id=project)

        if project.id not in self.projects:
            self.projects.append(project.id)

        self.save()

    def revoke_project(self, project):
        """Revoke user access to the given project."""
        self.projects.remove(project)
        self.save()
