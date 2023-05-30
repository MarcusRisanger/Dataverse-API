import json
from urllib.parse import urljoin

import pytest
import requests
import responses
from pytest_mock import mocker  # noqa F401

from dataverse_api.dataverse import DataverseClient, DataverseEntity
from dataverse_api.utils import DataverseBatchCommand, DataverseColumn, DataverseError


@pytest.fixture
def dataverse_entity_name():
    return "test"


@pytest.fixture
def dataverse_entity_init_response():
    with open("tests/sample_data/test_entity_init.txt") as f:
        return f.read()


@pytest.fixture
def dataverse_entity_init_response_bad():
    with open("tests/sample_data/test_entity_init_bad.txt") as f:
        return f.read()


@pytest.fixture
def dataverse_api_url():
    return "https://xxx"


@pytest.fixture
def microsoft_tenant():
    return "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def dataverse_access_token():
    token = {"access_token": "abc123", "token_type": "Bearer", "expires_in": 123}
    return token


@pytest.fixture
def msal_authority_response(microsoft_tenant):
    tenant = microsoft_tenant
    return json.dumps(
        {
            "authorization_endpoint": (
                "https://login.microsoftonline.com/" + f"{tenant}/oauth2/v2.0/authorize"
            ),
            "token_endpoint": (
                "https://login.microsoftonline.com/" + f"{tenant}/oauth2/v2.0/token"
            ),
        }
    )


@pytest.fixture()
def auth_discovery_response():
    return json.dumps(
        {
            "metadata": [
                {
                    "preferred_network": "login.microsoftonline.com",
                    "preferred_cache": "login.windows.net",
                    "aliases": [
                        "login.microsoftonline.com",
                        "login.windows.net",
                        "login.microsoft.com",
                        "sts.windows.net",
                    ],
                }
            ],
        }
    )


@pytest.fixture()
def entity_initialization_response():
    with open("tests/sample_data/test_entity_init.txt") as f:
        return f.read()


@pytest.fixture(autouse=True)
def mocked_responses(
    dataverse_authority_url,
    msal_authority_response,
    auth_discovery_response,
    microsoft_tenant,
    dataverse_api_url,
    dataverse_access_token,
    entity_initialization_response,
):
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        # Common arguments
        api_url = f"{dataverse_api_url}/api/data/v9.2/"
        postfix = "foo"

        # Authority Response
        rsps.add(
            method="GET",
            url=f"{dataverse_authority_url}/v2.0/.well-known/openid-configuration",
            body=msal_authority_response,
        )

        # Auth code call
        rsps.add(
            method="GET",
            url="https://login.microsoftonline.com/common/discovery/instance",
            body=auth_discovery_response,
        )

        # Dataverse access token
        rsps.add(
            method="POST",
            url=(
                "https://login.microsoftonline.com/"
                + f"{microsoft_tenant}/oauth2/v2.0/token"
            ),
            body=json.dumps(dataverse_access_token),
        )

        # Client failure calls

        rsps.add(method="GET", url=urljoin(api_url, postfix), status=400)
        rsps.add(method="POST", url=urljoin(api_url, postfix), status=400)
        rsps.add(method="PUT", url=urljoin(api_url, postfix), status=400)
        rsps.add(method="PATCH", url=urljoin(api_url, postfix), status=400)
        rsps.add(method="DELETE", url=urljoin(api_url, postfix), status=400)

        # Entity validation calls
        rsps.add(
            method="POST",
            url=urljoin(
                api_url,
                "$batch",
            ),
            status=200,
            body=entity_initialization_response,
        )

        yield rsps


@pytest.fixture
def dataverse_auth(dataverse_scopes, dataverse_access_token):
    class DataverseAuth:
        def __init__(self, scopes):
            self.scopes = scopes

        def _get_access_token(self):
            return dataverse_access_token

        # Replacement Auth must be callable in this way.
        def __call__(
            self, input_request: requests.PreparedRequest
        ) -> requests.PreparedRequest:
            token = self._get_access_token()
            input_request.headers[
                "Authorization"
            ] = f"{token['token_type']} {token['access_token']}"
            return input_request

    auth = DataverseAuth(scopes=dataverse_scopes)

    assert auth.scopes == dataverse_scopes
    assert auth._get_access_token() == dataverse_access_token

    return auth


@pytest.fixture
def dataverse_client(
    dataverse_scopes, dataverse_api_url, dataverse_authority_url
) -> DataverseClient:
    client = DataverseClient(
        app_id="abc",
        client_secret="xyz",
        authority_url=dataverse_authority_url,
        dynamics_url=dataverse_api_url,
        scopes=dataverse_scopes,
    )
    return client


@pytest.fixture
def dataverse_batch_commands():
    data = [
        DataverseBatchCommand(uri="uri1", mode="mode1", data={"col1": 1, "col2": 2}),
        DataverseBatchCommand(uri="uri2", mode="mode1", data={"col1": 3, "col2": 4}),
        DataverseBatchCommand(uri="uri3", mode="mode1", data={"col1": 5, "col2": 6}),
    ]
    return data


def test_dataverse_instantiation(
    dataverse_client,
    dataverse_scopes,
    dataverse_api_url,
    dataverse_access_token,
):
    c: DataverseClient = dataverse_client

    assert c.api_url == f"{dataverse_api_url}/api/data/v9.2/"
    assert c._auth.scopes == dataverse_scopes
    assert c._auth._get_access_token() == dataverse_access_token
    assert c._entity_cache == dict()
    assert c._default_headers == {
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Content-Type": "application/json",
    }

    # Mocking endpoint responses raising errors
    postfix = "foo"

    with pytest.raises(DataverseError, match=r"Error with GET request: .+"):
        c.get(postfix)
    with pytest.raises(DataverseError, match=r"Error with POST request: .+"):
        c.post(postfix)
    with pytest.raises(DataverseError, match=r"Error with PUT request: .+"):
        c.put(postfix, key="test", column="col", value=1)
    with pytest.raises(DataverseError, match=r"Error with PATCH request: .+"):
        c.patch(postfix, data={"col": 1})
    with pytest.raises(DataverseError, match=r"Error with DELETE request: .+"):
        c.delete(postfix)


@pytest.fixture
def entity_validated(dataverse_client, dataverse_entity_name):
    c: DataverseClient = dataverse_client
    # assert c._auth._get_access_token() is not None

    entity = c.entity(logical_name=dataverse_entity_name)

    return entity


def test_entity_validated(entity_validated, dataverse_entity_name):
    entity: DataverseEntity = entity_validated

    assert entity.schema.name == dataverse_entity_name + "s"
    assert entity.schema.key == "testid"
    assert entity.schema.altkeys == [{"test_pk"}, {"test_int", "test_string"}]
    assert len(entity.schema.columns) == 13
    assert entity.schema.columns["test_pk"] == DataverseColumn(
        schema_name="test_pk", can_create=True, can_update=True
    )
    assert all(
        i in entity.schema.columns
        for i in [
            "testid",
            "test_pk",
            "test_string",
            "test_int",
        ]
    )
    assert entity.schema.key in entity.schema.columns
    for key in entity.schema.altkeys:
        assert all(col in entity.schema.columns for col in key)


@pytest.fixture
def entity_unvalidated(
    dataverse_client,
    dataverse_entity_name,
):
    c: DataverseClient = dataverse_client

    entity_name = dataverse_entity_name
    entity = c.entity(entity_set_name=entity_name)

    return entity


def test_entity_unvalidated(
    entity_unvalidated, dataverse_api_url, dataverse_entity_name
):
    entity: DataverseEntity = entity_unvalidated

    assert entity.schema.name == dataverse_entity_name
    assert entity.schema.key is None
    assert entity.schema.altkeys is None
    assert entity.schema.columns is None
    assert entity._client.api_url == f"{dataverse_api_url}/api/data/v9.2/"
