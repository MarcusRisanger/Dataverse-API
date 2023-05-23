import pytest
from pytest_mock import mocker  # noqa F401

from dataverse_api.dataverse import DataverseClient


@pytest.fixture
def dataverse_scopes():
    return ["scope"]


@pytest.fixture
def dataverse_auth(dataverse_scopes):
    class DataverseAuth:
        def __init__(self, scopes):
            self.scopes = scopes

        def _get_access_token(self):
            return {
                "access_token": "abc123",
                "token_type": "Bearer",
                "expires_in": 123,
            }

    return DataverseAuth(scopes=dataverse_scopes)


@pytest.fixture
def dataverse_client_unvalidated(
    mocker, dataverse_scopes, dataverse_auth  # noqa F811
) -> DataverseClient:
    mocker.patch.object(
        DataverseClient,
        "_authenticate",
        return_value=dataverse_auth,
    )

    client = DataverseClient(
        app_id="abc",
        client_secret="xyz",
        authority_url="https://foo",
        dynamics_url="https://dyn",
        scopes=dataverse_scopes,
        validate=False,
    )
    return client


def test_dataverse_instantiation(dataverse_client_unvalidated, dataverse_scopes):
    c: DataverseClient = dataverse_client_unvalidated

    assert c.api_url == "https://dyn/api/data/v9.2/"
    assert c._auth.scopes == dataverse_scopes
    assert c._auth._get_access_token() == {
        "access_token": "abc123",
        "token_type": "Bearer",
        "expires_in": 123,
    }
    assert c._entity_cache == dict()
    assert c._default_headers == {
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Content-Type": "application/json",
    }

    with pytest.raises(AttributeError, match=r"object has no attribute 'schema'"):
        assert c.schema
