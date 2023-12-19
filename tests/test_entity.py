from dataverse.dataverse import DataverseClient
from responses.matchers import query_param_matcher


def test_entity_instantiation(client: DataverseClient, mocked_responses):
    # Entity instantiation sends a few API calls:
    #   - to /EntityDefinitions(logical_name='<foo>')
    #   - to /EntityDefinitions(logical_name='<foo>'/Keys)

    entity_name = "foo"
    entity_set_name = "foos"
    primary_id = "fooid"
    primary_img = "fooimg"

    # Initial call
    columns = ["EntitySetName", "PrimaryIdAttribute", "PrimaryImageAttribute"]
    mocked_responses.get(
        url=client._endpoint + f"EntityDefinitions(LogicalName='{entity_name}')",
        status=200,
        match=[query_param_matcher({"$select": ",".join(columns)})],
        json={"EntitySetName": entity_set_name, "PrimaryIdAttribute": primary_id, "PrimaryImageAttribute": primary_img},
    )

    # Keys call
    altkey_1, altkey_cols_1 = "foo_key", [primary_id]
    altkey_2, altkey_cols_2 = "foo_key2", [primary_id, primary_img]

    columns = ["SchemaName", "KeyAttributes"]
    mocked_responses.get(
        url=client._endpoint + f"EntityDefinitions(LogicalName='{entity_name}')/Keys",
        status=200,
        match=[query_param_matcher({"$select": ",".join(columns)})],
        json={
            "value": [
                {"SchemaName": altkey_1, "KeyAttributes": altkey_cols_1},
                {"SchemaName": altkey_2, "KeyAttributes": altkey_cols_2},
            ]
        },
    )

    entity = client.entity(entity_name)

    assert entity.logical_name == entity_name
    assert entity.entity_set_name == entity_set_name
    assert entity.primary_id_attr == primary_id
    assert entity.primary_img_attr == primary_img
    assert len(entity.alternate_keys) == 2
    assert all(i in entity.alternate_keys for i in [altkey_1, altkey_2])
    assert entity.alternate_keys[altkey_1] == altkey_cols_1
    assert entity.alternate_keys[altkey_2] == altkey_cols_2
