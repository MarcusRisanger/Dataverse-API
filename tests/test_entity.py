import pytest
import responses
from responses.matchers import query_param_matcher

from dataverse.dataverse import DataverseClient
from dataverse.entity import DataverseEntity


@pytest.fixture
def entity_name() -> str:
    return "foo"


@pytest.fixture
def entity_set_name() -> str:
    return "foos"


@pytest.fixture
def primary_id() -> str:
    return "fooid"


@pytest.fixture
def primary_img() -> str:
    return "fooimg"


@pytest.fixture
def altkey_1(primary_id) -> tuple[str, list[str]]:
    return "foo_key", [primary_id]


@pytest.fixture
def altkey_2(primary_id: str, primary_img: str) -> tuple[str, list[str]]:
    return "foo_key2", [primary_id, primary_img]


@pytest.fixture
def entity(
    client: DataverseClient,
    mocked_responses: responses.RequestsMock,
    entity_name: str,
    entity_set_name: str,
    primary_id: str,
    primary_img: str,
    altkey_1: tuple[str, list[str]],
    altkey_2: tuple[str, list[str]],
):
    # Initial call
    columns = ["EntitySetName", "PrimaryIdAttribute", "PrimaryImageAttribute"]
    mocked_responses.get(
        url=client._endpoint + f"EntityDefinitions(LogicalName='{entity_name}')",
        status=200,
        match=[query_param_matcher({"$select": ",".join(columns)})],
        json={"EntitySetName": entity_set_name, "PrimaryIdAttribute": primary_id, "PrimaryImageAttribute": primary_img},
    )

    # Keys call
    columns = ["SchemaName", "KeyAttributes"]
    mocked_responses.get(
        url=client._endpoint + f"EntityDefinitions(LogicalName='{entity_name}')/Keys",
        status=200,
        match=[query_param_matcher({"$select": ",".join(columns)})],
        json={
            "value": [
                {"SchemaName": altkey_1[0], "KeyAttributes": altkey_1[1]},
                {"SchemaName": altkey_2[0], "KeyAttributes": altkey_2[1]},
            ]
        },
    )

    # Action SDK Messages call

    mocked_responses.get(
        url=client._endpoint + "sdkmessagefilters",
        match=[
            query_param_matcher(
                {
                    "$select": "sdkmessagefilterid",
                    "$expand": "sdkmessageid($select=name)",
                    "$filter": (
                        "(sdkmessageid/name eq 'CreateMultiple' or "
                        + "sdkmessageid/name eq 'UpdateMultiple') and "
                        + f"primaryobjecttypecode eq '{entity_name}'"
                    ),
                }
            )
        ],
        json={
            "value": [
                {"sdkmessageid": {"name": "CreateMultiple"}},
                {"sdkmessageid": {"name": "UpdateMultiple"}},
            ]
        },
    )

    return client.entity(entity_name)


def test_entity_instantiation(
    entity: DataverseEntity,
    entity_name: str,
    entity_set_name: str,
    primary_id: str,
    primary_img: str,
    altkey_1: tuple[str, list[str]],
    altkey_2: tuple[str, list[str]],
):
    assert entity.logical_name == entity_name
    assert entity.entity_set_name == entity_set_name
    assert entity.primary_id_attr == primary_id
    assert entity.primary_img_attr == primary_img
    assert len(entity.alternate_keys) == 2
    assert all(i in entity.alternate_keys for i in [altkey_1[0], altkey_2[0]])
    assert entity.alternate_keys[altkey_1[0]] == altkey_1[1]
    assert entity.alternate_keys[altkey_2[0]] == altkey_2[1]
    assert entity.supports_create_multiple is True
    assert entity.supports_update_multiple is True


def test_entity_read(entity: DataverseEntity, entity_set_name: str, mocked_responses: responses.RequestsMock):
    # Reading without any args
    mock_data = {"value": [{"data": 1}, {"data": 2}]}

    mocked_responses.get(url=entity._endpoint + entity_set_name, json=mock_data)

    resp = entity.read()

    assert resp == mock_data["value"]
