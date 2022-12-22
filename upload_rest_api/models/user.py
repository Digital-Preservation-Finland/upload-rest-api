"""User model."""
import hashlib
import random
import string

from bson.binary import Binary
from mongoengine import NotUniqueError, ValidationError

from upload_rest_api.models.project import ProjectEntry, Project
from upload_rest_api.models.user_entry import UserEntry

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
        ProjectEntry.objects.filter(id__in=projects).only("id")
    )

    missing_projects = projects - existing_projects

    if missing_projects:
        raise ValidationError(
            f"Projects don't exist: {','.join(missing_projects)}"
        )


class UserExistsError(Exception):
    """Exception for trying to create a user, which already exists."""


class User:
    """Pre-ingest file storage user"""
    def __init__(self, db_user):
        self._db_user = db_user

    # Read-only properties for database fields
    username = property(lambda x: x._db_user.username)
    salt = property(lambda x: x._db_user.salt)
    digest = property(lambda x: x._db_user.digest)
    projects = property(lambda x: tuple(x._db_user.projects))

    DoesNotExist = UserEntry.DoesNotExist

    @classmethod
    def create(cls, username, projects=None, password=None):
        """Add new user to the database.

        Salt is always generated randomly, but password can be set by
        providing to optional argument password.

        :param projects: Projects the user is associated with.
        :param password: Password of the created user
        :returns: The password
        """
        db_user = UserEntry(username=username)
        if projects is None:
            projects = []

        db_user.projects = projects

        if password is not None:
            passwd = password
        else:
            passwd = get_random_string(PASSWD_LEN)

        db_user.salt = get_random_string(SALT_LEN)
        db_user.digest = hash_passwd(passwd, db_user.salt)

        try:
            db_user.save(force_insert=True)
        except NotUniqueError as exc:
            raise UserExistsError(
                f"User '{username}' already exists"
            ) from exc
        return cls(db_user=db_user)

    @classmethod
    def get(cls, *args, **kwargs):
        """
        Retrieve an existing user

        :param kwargs: Field arguments used to retrieve the user

        :returns: User instance
        """
        return User(
            db_user=UserEntry.objects.get(**kwargs)
        )

    def generate_password(self):
        """Generate new user password."""
        passwd = get_random_string(PASSWD_LEN)
        self._db_user.salt = get_random_string(SALT_LEN)
        self._db_user.digest = hash_passwd(passwd, self.salt)
        self._db_user.save()

        return passwd

    def grant_project(self, project):
        """Grant user access to the given project."""
        project = Project.get(id=project)

        if project.id not in self.projects:
            self._db_user.projects.append(project.id)

        self._db_user.save()

    def revoke_project(self, project):
        """Revoke user access to the given project."""
        self._db_user.projects.remove(project)
        self._db_user.save()

    def delete(self):
        self._db_user.delete()
