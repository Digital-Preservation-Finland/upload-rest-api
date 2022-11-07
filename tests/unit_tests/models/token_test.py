"""Unit tests for Token database class"""
import datetime


from upload_rest_api.models.token import Token


def test_correct_document_structure(tokens_col):
    token = Token(
        id="urn:uuid:b58cc800-b3a6-46e5-8869-b8a30273b23c",
        name="Test token",
        username="test_user",
        projects=["test_project", "test_project_2"],
        token_hash="shashasha",
        expiration_date=datetime.datetime(
            2020, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
        ),
        admin=False,
        session=True
    )
    token.save()

    docs = list(tokens_col.find())
    assert len(docs) == 1
    assert docs[0] == {
        "_id": "urn:uuid:b58cc800-b3a6-46e5-8869-b8a30273b23c",
        "name": "Test token",
        "username": "test_user",
        "projects": ["test_project", "test_project_2"],
        "token_hash": "shashasha",
        # MongoDB does not store time zone information
        "expiration_date": datetime.datetime(2020, 1, 1, 12, 0),
        "admin": False,
        "session": True
    }
