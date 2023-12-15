import json

import pytest
import responses
import re

from dataverse.dataverse import DataverseClient
from dataverse.errors import DataverseError
from dataverse.utils.batching import BatchCommand, BatchMode
from dataverse.metadata.helpers import Publisher, Solution
from dataverse.metadata.entity import EntityMetadata

from responses.matchers import json_params_matcher, header_matcher

from dataverse.metadata.relationships import OneToManyRelationshipMetadata


def test_api_call(client: DataverseClient, mocked_responses: responses.RequestsMock):
    # Mocking an errored request

    mocked_responses.get(url=f"{client._endpoint}Foo", status=500)

    expected = "Error with GET request:"

    with pytest.raises(DataverseError, match=expected):
        client._api_call(method="get", url="Foo")


def test_api_batch(client: DataverseClient, mocked_responses: responses.RequestsMock):
    batch = "funky"
    batch_data = [
        BatchCommand(url="foo", mode=BatchMode.GET),
        BatchCommand(url="bar", mode=BatchMode.PUT, data={"foo": "bar"}),
        BatchCommand(url="moo", mode=BatchMode.POST, data={"foo": "bar"}),
    ]

    mocked_responses.post(
        url=f"{client._endpoint}$batch",
        match=[header_matcher({"Content-Type": f'multipart/mixed; boundary="batch_{batch}"', "If-None-Match": "null"})],
    )

    req = client._batch_api_call(batch_data, id_generator=lambda: batch)[0].request.body

    # Each batch command should be constructed like this:
    full_pattern = (
        rf"--batch_{batch}\nContent-Type: application/http\nContent.Transfer.Encoding: binary\n\n"
        + rf"(?:PUT|GET|DELETE|POST|PATCH) {client._endpoint}.+ (?:HTTP\/1.1)"
        + "\nContent-Type: application/json(?:; type=entry)?\n\n"
    )
    pat = re.compile(full_pattern, re.M)
    assert len(re.findall(pat, req)) == len(batch_data)
    assert len(re.findall(rf"--batch_{batch}--$", req, re.M)) == 1, "Should have only one end of batch line."
    assert req[-2:] == "\n\n", "Should end with 2 clrfs"

    # POST batches have an additional line in Content-Type element header:
    pat = re.compile(r"Content-Type: application\/json; type=entry")
    assert len(re.findall(pat, req)) == len(list(filter(lambda x: x.mode == BatchMode.POST, batch_data)))


def test_create_entity(
    client: DataverseClient, mocked_responses: responses.RequestsMock, sample_entity: EntityMetadata
):
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
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
):
    name = "Foo"

    # Setting up response mock
    url = f"{client._endpoint}EntityDefinitions(LogicalName='{name}')"
    mocked_responses.delete(url=url)

    resp = client.delete_entity(logical_name=name)

    assert resp.request.body is None


def test_create_publisher(
    client: DataverseClient,
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
    client: DataverseClient,
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


def test_(
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
    one_many_relationship: OneToManyRelationshipMetadata,
):
    mocked_responses.post(
        url=f"{client._endpoint}RelationshipDefinitions",
        status=204,
        match=[json_params_matcher(one_many_relationship())],
    )

    resp = client.create_relationship(relationship_definition=one_many_relationship)

    assert resp.status_code == 204
