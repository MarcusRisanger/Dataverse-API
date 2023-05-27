import json
from urllib.parse import urljoin

import pytest
import requests
import responses
from pytest_mock import mocker  # noqa F401
from responses import matchers

from dataverse_api.dataverse import (
    ENTITY_ATTR_PARAMS,
    ENTITY_KEY_PARAMS,
    ENTITY_SET_PARAMS,
    DataverseClient,
    DataverseEntity,
)
from dataverse_api.utils import DataverseBatchCommand, DataverseColumn, DataverseError


@pytest.fixture
def dataverse_entity_name():
    return "test"


@pytest.fixture
def dataverse_entity_set():
    with open("tests/sample_data/test_entity_set.json") as f:
        return json.load(f)


@pytest.fixture
def dataverse_entity_attrs():
    with open("tests/sample_data/test_entity_attributes.json") as f:
        return json.load(f)


@pytest.fixture
def dataverse_entity_attrs_bad():
    with open("tests/sample_data/test_entity_attributes_bad.json") as f:
        return json.load(f)


@pytest.fixture
def dataverse_entity_keys():
    with open("tests/sample_data/test_entity_keys.json") as f:
        return json.load(f)


@pytest.fixture
def dataverse_api_url():
    return "https://xxx"


@pytest.fixture
def dataverse_scopes(dataverse_api_url):
    return [urljoin(dataverse_api_url, ".myscope")]


@pytest.fixture
def dataverse_access_token():
    token = {
        "access_token": "abc123",
        "token_type": "Bearer",
        "expires_in": 123,
    }
    return token


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
    mocker,  # noqa F811
    dataverse_scopes,
    dataverse_auth,
    dataverse_api_url,
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


@responses.activate
def test_dataverse_instantiation(
    dataverse_client, dataverse_scopes, dataverse_api_url, dataverse_batch_commands
):
    c: DataverseClient = dataverse_client

    assert c.api_url == f"{dataverse_api_url}/api/data/v9.2/"
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

    # Mocking endpoint responses raising errors
    postfix = "foo"
    responses.get(urljoin(c.api_url, postfix), status=400)
    responses.post(urljoin(c.api_url, postfix), status=400)
    responses.put(urljoin(c.api_url, postfix), status=400)
    responses.patch(urljoin(c.api_url, postfix), status=400)
    responses.delete(urljoin(c.api_url, postfix), status=400)
    responses.post(urljoin(c.api_url, "$batch"), status=400)

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
    with pytest.raises(DataverseError, match=r"Error with POST request: .+"):
        c.batch_operation(data=dataverse_batch_commands)


@pytest.fixture
@responses.activate
def entity_validated(
    dataverse_client,
    dataverse_entity_name,
    dataverse_entity_set,
    dataverse_entity_attrs,
    dataverse_entity_keys,
):
    c: DataverseClient = dataverse_client
    assert c._auth._get_access_token() is not None
    logical_name = dataverse_entity_name
    base_url = f"{c.api_url}EntityDefinitions(LogicalName='{logical_name}')"

    # Mocking responses generated by Entity instantiation

    # EntitySet query
    responses.add(
        method="GET",
        url=base_url,
        match=[matchers.query_param_matcher({"$select": ",".join(ENTITY_SET_PARAMS)})],
        json=dataverse_entity_set,
    )
    # EntityAttributes query
    responses.add(
        method="GET",
        url=f"{base_url}/Attributes",
        match=[
            matchers.query_param_matcher(
                {
                    "$select": ",".join(ENTITY_ATTR_PARAMS),
                    "$filter": "IsValidODataAttribute eq true",
                }
            )
        ],
        json=dataverse_entity_attrs,
    )
    # EntityKeys query
    responses.add(
        method="GET",
        url=f"{base_url}/Keys",
        match=[matchers.query_param_matcher({"$select": ",".join(ENTITY_KEY_PARAMS)})],
        json=dataverse_entity_keys,
    )

    entity = c.entity(logical_name=dataverse_entity_name)

    return entity


def test_entity_validated(entity_validated, dataverse_entity_name):
    entity: DataverseEntity = entity_validated

    assert entity.schema.name == dataverse_entity_name
    assert entity.schema.key == "testid"
    assert entity.schema.altkeys == [{"test_pk"}, {"test_int", "test_string"}]
    assert len(entity.schema.columns) == 12
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
