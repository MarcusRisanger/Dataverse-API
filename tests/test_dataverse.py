import json
from typing import Callable

import pytest
import responses

from dataverse.dataverse import Dataverse
from dataverse.errors import DataverseError
from dataverse.metadata.helpers import Publisher, Solution
from dataverse.metadata.entity import EntityMetadata

from responses.matchers import json_params_matcher


def test_api_call(client: Dataverse, mocked_responses: responses.RequestsMock):
    # Mocking an errored request

    mocked_responses.get(url=f"{client._endpoint}Foo", status=500)

    expected = "Error with GET request:"

    with pytest.raises(DataverseError, match=expected):
        client._Dataverse__api_call(method="get", url="Foo")


def test_create_entity(client: Dataverse, mocked_responses: responses.RequestsMock, sample_entity: EntityMetadata):
    # Mocking the request sent by endpoint
    response = {
        "url": f"{client._endpoint}EntityDefinitions",
        "status": 204,
        "content_type": "application/json",
        "match": [json_params_matcher(sample_entity())],
    }
    mocked_responses.post(**response)

    # Running function - this errors if the endpoint URL
    # does not match with the mocked response URL
    resp = client.create_entity(sample_entity)

    # Run some assertions that payload contains critical attributes
    assert json.loads(resp.request.body) == sample_entity()

    # Again for invoking with `solution_name`
    mocked_responses.post(**response)
    resp = client.create_entity(sample_entity, solution_name="Foo")

    # Run assertions again!
    assert resp.request.headers["MSCRM.SolutionName"] == "Foo"


def test_delete_entity(
    client: Dataverse,
    mocked_responses: responses.RequestsMock,
):
    name = "Foo"

    # Setting up response mock
    url = f"{client._endpoint}EntityDefinitions(LogicalName='{name}')"
    mocked_responses.delete(url=url)

    resp = client.delete_entity(logical_name=name)

    assert resp.request.body is None


def test_create_publisher(
    client: Dataverse,
    mocked_responses: responses.RequestsMock,
):
    pub = Publisher("A", "B", "C", "D", 123)

    mocked_responses.post(
        url=f"{client._endpoint}publishers",
        status=204,
        match=[json_params_matcher(pub())],
    )

    resp = client.create_publisher(publisher_definition=pub)

    assert resp.status_code == 204


def test_create_solution(
    client: Dataverse,
    mocked_responses: responses.RequestsMock,
):
    sol = Solution("A", "B", "C", "D")

    mocked_responses.post(
        url=f"{client._endpoint}solutions",
        status=204,
        match=[json_params_matcher(sol())],
    )

    resp = client.create_solution(solution_definition=sol)

    assert resp.status_code == 204
