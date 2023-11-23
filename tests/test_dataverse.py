import json
from typing import Callable

import pytest
import responses

from dataverse.dataverse import Dataverse


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
