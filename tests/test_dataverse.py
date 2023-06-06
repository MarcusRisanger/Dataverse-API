import logging
from urllib.parse import urljoin

import pytest
import requests
import responses

from dataverse_api.dataclasses import (
    DataverseBatchCommand,
    DataverseColumn,
    DataverseError,
)
from dataverse_api.dataverse import DataverseClient, DataverseEntity
from dataverse_api.utils import convert_data


@pytest.fixture
def dataverse_entity_name():
    return "test"


@pytest.fixture
def dataverse_resource():
    return "https://org.api.region.microsoft.com"


@pytest.fixture
def dataverse_api_url(dataverse_resource):
    return urljoin(dataverse_resource, "/api/data/v9.2/")


@pytest.fixture()
def entity_initialization_response():
    with open("tests/sample_data/test_entity_init.txt") as f:
        return f.read()


@pytest.fixture()
def entity_initialization_response_bad():
    with open("tests/sample_data/test_entity_init_bad.txt") as f:
        return f.read()


@pytest.fixture
def dataverse_access_token():
    token = {"access_token": "abc123", "token_type": "Bearer", "expires_in": 123}
    return token


@pytest.fixture
def dataverse_auth(dataverse_access_token):
    class DataverseAuth:
        def __init__(self):
            pass

        def _get_access_token(self):
            return dataverse_access_token

        def __call__(
            self, input_request: requests.PreparedRequest
        ) -> requests.PreparedRequest:
            token = self._get_access_token()
            input_request.headers[
                "Authorization"
            ] = f"{token['token_type']} {token['access_token']}"
            return input_request

    auth = DataverseAuth()
    return auth


@pytest.fixture
def mocked_init_response(
    dataverse_api_url,
    entity_initialization_response,
):
    with responses.RequestsMock() as rsps:
        # Entity validation calls
        rsps.add(
            method="POST",
            url=urljoin(
                dataverse_api_url,
                "$batch",
            ),
            status=200,
            body=entity_initialization_response,
        )

        yield rsps


@pytest.fixture
def dataverse_client(dataverse_resource, dataverse_auth) -> DataverseClient:
    client = DataverseClient(resource=dataverse_resource, auth=dataverse_auth)
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
    dataverse_api_url,
):
    c: DataverseClient = dataverse_client

    assert c.api_url == dataverse_api_url
    assert c._entity_cache == dict()
    assert c._default_headers == {
        "Accept": "application/json",
        "OData-MaxVersion": "4.0",
        "OData-Version": "4.0",
        "Content-Type": "application/json",
    }


@pytest.fixture
def mocked_failure_responses(dataverse_api_url):
    # Common arguments
    api_url = dataverse_api_url
    postfix = "foo"

    with responses.RequestsMock() as rsps:
        # Client failure calls
        rsps.add(method="GET", url=urljoin(api_url, postfix), status=400)
        rsps.add(method="POST", url=urljoin(api_url, postfix), status=400)
        rsps.add(method="PUT", url=urljoin(api_url, f"{postfix}(test)/col"), status=400)
        rsps.add(method="PATCH", url=urljoin(api_url, postfix), status=400)
        rsps.add(method="DELETE", url=urljoin(api_url, postfix), status=400)
        rsps.add(method="POST", url=urljoin(api_url, "$batch"), status=400)

        yield rsps


@responses.activate
def test_dataverse_client_request_failures(
    dataverse_api_url,
    dataverse_client,
):
    # Common args
    c: DataverseClient = dataverse_client
    postfix = "foo"
    api_url = dataverse_api_url

    # Setting up client failure calls
    responses.add(method="GET", url=urljoin(api_url, postfix), status=400)
    responses.add(method="POST", url=urljoin(api_url, postfix), status=400)
    responses.add(
        method="PUT", url=urljoin(api_url, f"{postfix}(test)/col"), status=400
    )
    responses.add(method="PATCH", url=urljoin(api_url, postfix), status=400)
    responses.add(method="DELETE", url=urljoin(api_url, postfix), status=400)
    responses.add(method="POST", url=urljoin(api_url, "$batch"), status=400)

    # Mocking endpoint responses raising errors
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
        c.post("$batch")


@pytest.fixture
def entity_validated(
    dataverse_client,
    dataverse_entity_name,
    mocked_init_response,
):
    c: DataverseClient = dataverse_client

    entity = c.entity(logical_name=dataverse_entity_name, validate=True)

    return entity


@pytest.fixture
def entity_unvalidated(dataverse_client, dataverse_entity_name, mocked_init_response):
    c: DataverseClient = dataverse_client

    entity_name = dataverse_entity_name
    entity = c.entity(logical_name=entity_name)

    return entity


def test_entity_validated(
    entity_validated,
    dataverse_entity_name,
    mocked_init_response,
):
    entity: DataverseEntity = entity_validated

    assert entity._validate is True
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


@pytest.mark.parametrize(
    "data, mode, result",
    [
        ({"testid": 1, "test_string": "foo"}, None, None),
        ({"testid": 1, "test_pk": "A", "test_string": "foo"}, "write", {"testid"}),
        ({"test_pk": "A", "test_int": 2}, "create", {"test_pk"}),
    ],
)
def test_data_validation(
    data,
    mode,
    result,
    entity_validated,
    mocked_init_response,
):
    entity: DataverseEntity = entity_validated
    data = convert_data(data)

    assert entity._validate_payload(data=data, mode=mode) == result


def test_entity_unvalidated(
    caplog,
    entity_unvalidated,
    dataverse_api_url,
    dataverse_entity_name,
    mocked_init_response,
):
    entity: DataverseEntity = entity_unvalidated

    assert entity.schema.name == dataverse_entity_name + "s"
    assert entity.schema.key == "testid"
    assert entity._validate is False
    assert entity._client.api_url == dataverse_api_url

    with caplog.at_level(logging.INFO):
        validation = entity._validate_payload({"foo": 1, "bar": 2})

    assert validation is None
    assert "Data validation not performed." in caplog.text
