import json
from typing import Callable

import pytest
import responses

from dataverse.dataverse import Dataverse
from dataverse.errors import DataverseError

from responses.matchers import json_params_matcher


def test_api_call(client: Dataverse, mocked_responses: responses.RequestsMock):
    # Mocking an errored request

    mocked_responses.get(url=f"{client._endpoint}Foo", status=500)

    expected = "Error with GET request:"

    with pytest.raises(DataverseError, match=expected):
        client._Dataverse__api_call(method="get", url="Foo")


def test_create_entity(
    client: Dataverse,
    mocked_responses: responses.RequestsMock,
):
    # Expects a callable metadata entity
    entity_metadata: Callable[[], dict[str, str]] = lambda: {"Test": "Data"}

    # Mocking the request sent by endpoint
    response = {
        "url": f"{client._endpoint}EntityDefinitions",
        "status": 204,
        "content_type": "application/json",
        "match": [json_params_matcher({"Test": "Data"})],
    }
    mocked_responses.post(**response)

    # Running function - this errors if the endpoint URL
    # does not match with the mocked response URL
    resp = client.create_entity(entity_metadata)

    # Run some assertions that payload contains critical attributes
    assert json.loads(resp.request.body) == {"Test": "Data"}

    # Again for invoking with `solution_name`
    mocked_responses.post(**response)
    resp = client.create_entity(entity_metadata, solution_name="Foo")

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
    name, unique_name, description, prefix, option_prefix = "A", "B", "C", "D", 69

    mocked_responses.post(
        url=f"{client._endpoint}publishers",
        status=204,
        match=[
            json_params_matcher(
                {
                    "friendlyname": name,
                    "uniquename": unique_name,
                    "description": description,
                    "customizationprefix": prefix,
                    "customizationoptionvalueprefix": option_prefix,
                }
            )
        ],
    )

    resp = client.create_publisher(
        publisher_name=name,
        unique_name=unique_name,
        description=description,
        prefix=prefix,
        option_prefix=option_prefix,
    )

    assert resp.status_code == 204


def test_create_solution(
    client: Dataverse,
    mocked_responses: responses.RequestsMock,
):
    name, unique_name, description, guid = "A", "B", "C", "D"

    mocked_responses.post(
        url=f"{client._endpoint}solutions",
        status=204,
        match=[
            json_params_matcher(
                {
                    "friendlyname": name,
                    "uniquename": unique_name,
                    "description": description,
                    "version": "1.0.0.0",
                    "publisher@odata.bind": f"publishers({guid})",
                }
            )
        ],
    )

    resp = client.create_solution(
        solution_name=name,
        unique_name=unique_name,
        description=description,
        publisher_guid=guid,
    )

    assert resp.status_code == 204
