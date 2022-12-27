"""Unit tests for database module."""
import binascii
import re

import pytest
from mongoengine import ValidationError

from upload_rest_api.models.project import ProjectEntry
from upload_rest_api.models.user import (User, UserEntry, get_random_string,
                                         hash_passwd)


def test_correct_document_structure(users_col):
    """
    Test that saved User has the same structure as the pre-MongoEngine
    implementation
    """
    ProjectEntry(id="test_project").save()
    ProjectEntry(id="test_project_2").save()
    user = UserEntry(
        username="test_user",
        salt="salty",
        # Digest is a binary field
        digest=b"digesty",
        projects=["test_project", "test_project_2"]
    )
    user.save()

    docs = list(users_col.find())
    assert len(docs) == 1
    assert docs[0] == {
        "_id": "test_user",
        "salt": "salty",
        "digest": b"digesty",
        "projects": ["test_project", "test_project_2"],
    }


def test_nonexistent_projects():
    """
    Test that User rejects projects that don't exist
    """
    ProjectEntry(id="project_a").save()
    ProjectEntry(id="project_b").save()

    with pytest.raises(ValidationError) as exc:
        User.create(
            "test_user", projects=["project_a", "project_b", "project_c"]
        )

    assert "Projects don't exist: project_c" in str(exc.value)


@pytest.mark.parametrize(
    'projects',
    [
        None,
        (),
        ('test_project',),
        ('project1', 'project2')
    ]
)
def test_create_user(projects):
    """Test creation of new user.

    :param projects: List of user projects
    """
    if projects:
        for project in projects:
            ProjectEntry(id=project).save()

    user = User.create("test_user", projects=projects)

    data = UserEntry.objects.get(username="test_user").to_mongo()
    assert data["_id"] == "test_user"

    assert len(user.salt) == 20
    assert len(user.digest) == 64

    if projects:
        assert set(project.id for project in user.projects) == set(projects)
    else:
        assert len(user.projects) == 0


def test_get_random_string():
    """Test get_random_string method.

    Test that get_random_string() returns random strings
    of given lenght with only ascii letters and digits.
    """
    strings = set()

    for _ in range(1000):
        string = get_random_string(20)

        assert len(string) == 20
        assert re.match("^[A-Za-z0-9_-]*$", string)
        assert string not in strings
        strings.add(string)


def test_hash_passwd():
    """Test that salting and hashing returns the correct digest."""
    digest = binascii.hexlify(hash_passwd("test", "test")[:16])
    assert digest == b"4b119f6da6890ed1cc68d5b3adf7d053"
