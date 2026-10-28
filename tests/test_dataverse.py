import json
import logging
import re
import subprocess
import sys
from textwrap import dedent
from typing import Any

import pytest
import responses
from responses.matchers import header_matcher, json_params_matcher

from dataverse_api.dataverse import DataverseClient
from dataverse_api.errors import DataverseAPIError
from dataverse_api.metadata.entity import EntityMetadata
from dataverse_api.metadata.enums import OwnershipType
from dataverse_api.metadata.helpers import Publisher, Solution
from dataverse_api.metadata.relationships import OneToManyRelationshipMetadata
from dataverse_api.utils.batching import APICommand, BatchCommand, RequestMethod


@pytest.mark.parametrize("configured", [False, True])
def test_library_logging_preserves_application_configuration(configured: bool) -> None:
    script = dedent("""
        import logging
        import sys

        root_logger = logging.getLogger()
        if sys.argv[1] == "True":
            logging.basicConfig(level=logging.INFO, format="%(name)s: %(message)s")
        handlers = root_logger.handlers[:]
        level = root_logger.level

        from dataverse_api.utils.batching import check_altkey_support

        assert root_logger.handlers == handlers
        assert root_logger.level == level
        check_altkey_support(["key"], [{"key": []}])
        assert root_logger.handlers == handlers
        assert root_logger.level == level
    """)
    result = subprocess.run(
        [sys.executable, "-c", script, str(configured)],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout == ""
    if configured:
        assert result.stderr.count("dataverse_api.utils.batching: ") == 1
        assert "complex types" in result.stderr
    else:
        assert result.stderr == ""


def test_api_call(
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
    caplog: pytest.LogCaptureFixture,
):
    # Mocking an errored request

    mocked_responses.get(url=f"{client._endpoint}Foo", status=500, json={"error": {"message": "Whoopsie!"}})

    with pytest.raises(DataverseAPIError, match=r".*request failed.*"):
        client._api_call(method=RequestMethod.GET, url="Foo")


@pytest.mark.parametrize("threading", [False, True])
def test_api_request_error_logging(
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
    caplog: pytest.LogCaptureFixture,
    threading: bool,
) -> None:
    mocked_responses.get(url=f"{client._endpoint}Foo", status=500, json={"error": {"message": "Whoopsie!"}})
    calls = [APICommand(method=RequestMethod.GET, url="Foo")]

    with caplog.at_level(logging.ERROR, logger="dataverse_api"):
        if threading:
            result = client._threaded_call(calls)
        else:
            result = client._individual_call(calls)

    assert len(result) == 1
    assert result[0].status_code == 500
    assert [(record.name, record.levelno) for record in caplog.records] == [("dataverse_api._api", logging.ERROR)]
    assert "API request error:" in caplog.messages[0]


def test_api_batch(client: DataverseClient, mocked_responses: responses.RequestsMock):
    batch = "funky"
    batch_data = [
        BatchCommand(url="foo", method=RequestMethod.GET),
        BatchCommand(url="bar", method=RequestMethod.PUT, data={"foo": "bar"}),
        BatchCommand(url="moo", method=RequestMethod.POST, data={"foo": "bar"}),
    ]

    mocked_responses.post(
        url=f"{client._endpoint}$batch",
        match=[header_matcher({"Content-Type": f'multipart/mixed; boundary="batch_{batch}"', "If-None-Match": "null"})],
    )

    req = client._batch_api_call(batch_data, batch_size=500, id_generator=lambda: batch)[0].request.body

    assert isinstance(req, str)
    # Each batch command should be constructed like this:
    full_pattern = (
        rf"--batch_{batch}\nContent-Type: application/http\nContent.Transfer.Encoding: binary\n\n"
        + rf"(?:PUT|GET|DELETE|POST|PATCH) {client._endpoint}.+ (?:HTTP\/1.1)"
        + "\nContent-Type: application/json(?:; type=entry)?\n\n"
    )
    pat = re.compile(full_pattern, re.MULTILINE)
    assert len(re.findall(pat, req)) == len(batch_data)
    assert len(re.findall(rf"--batch_{batch}--$", req, re.MULTILINE)) == 1, "Should have only one end of batch line."
    assert req[-2:] == "\n\n", "Should end with 2 clrfs"

    # POST batches have an additional line in Content-Type element header:
    pat = re.compile(r"Content-Type: application\/json; type=entry")
    assert len(re.findall(pat, req)) == len(list(filter(lambda x: x.method == RequestMethod.POST, batch_data)))


@pytest.fixture
def create_entity_response(client: DataverseClient, sample_entity: EntityMetadata):
    return {
        "url": f"{client._endpoint}EntityDefinitions",
        "status": 204,
        "content_type": "application/json",
        "match": [json_params_matcher(sample_entity.dump_to_dataverse())],
    }


@pytest.mark.parametrize(
    "kwargs",
    [pytest.param({}, id="default"), pytest.param({"solution_name": "Foo"}, id="with-solution")],
)
def test_create_entity(
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
    sample_entity: EntityMetadata,
    create_entity_response: dict[str, Any],
    kwargs: dict[str, str],
):
    mocked_responses.post(**create_entity_response)

    resp = client.create_entity(sample_entity, **kwargs)

    assert resp.request.body
    assert json.loads(resp.request.body) == sample_entity.dump_to_dataverse()
    assert resp.request.headers.get("MSCRM.SolutionUniqueName") == kwargs.get("solution_name")


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


def test_create_relationship(
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
    one_many_relationship: OneToManyRelationshipMetadata,
):
    mocked_responses.post(
        url=f"{client._endpoint}RelationshipDefinitions",
        status=204,
        match=[json_params_matcher(one_many_relationship.dump_to_dataverse())],
    )

    resp = client.create_relationship(relationship_definition=one_many_relationship)

    assert resp.status_code == 204


def test_get_language_codes(
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
):
    mocked_responses.get(
        url=f"{client._endpoint}RetrieveAvailableLanguages",
        status=200,
        json={"LocaleIds": [123, 456], "Foo": "Bar"},
    )
    resp = client.get_language_codes()

    assert resp == [123, 456]


@pytest.fixture
def test_get_entity_definition(
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
    sample_entity_definition: dict[str, Any],
    schema_name: str,
) -> EntityMetadata:
    mocked_responses.get(
        url=f"{client._endpoint}EntityDefinitions(LogicalName='foo')", status=200, json=sample_entity_definition
    )

    resp = client.get_entity_definition(logical_name="foo")

    assert resp.display_name.localized_labels[0].label == "Display Name Test"
    assert resp.description.localized_labels[0].label == "Description Test"
    assert resp.ownership_type == OwnershipType.NONE
    assert resp.schema_name == schema_name

    return resp


@pytest.mark.parametrize(
    ("kwargs", "extra_headers"),
    [
        pytest.param({}, {}, id="default"),
        pytest.param(
            {"solution_name": "foo", "preserve_localized_labels": True},
            {"MSCRM.UniqueSolutionName": "foo", "MSCRM.PreserveLabels": "true"},
            id="solution-and-preserve-labels",
        ),
    ],
)
def test_update_entity(
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
    test_get_entity_definition: EntityMetadata,
    schema_name: str,
    kwargs: dict[str, Any],
    extra_headers: dict[str, str],
):
    mocked_responses.put(
        url=f"{client._endpoint}EntityDefinitions(LogicalName='{schema_name.lower()}')",
        status=204,
        match=[
            json_params_matcher(test_get_entity_definition.dump_to_dataverse()),
            header_matcher({"Content-Type": "application/json; charset=utf-8", **extra_headers}),
        ],
    )

    resp = client.update_entity(test_get_entity_definition, **kwargs)
    assert resp.status_code == 204
