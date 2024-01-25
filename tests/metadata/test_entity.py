from dataverse_api.metadata.attributes import StringAttributeMetadata
from dataverse_api.metadata.entity import define_entity
from dataverse_api.utils.labels import create_label


def test_create_entity_metadata():
    # Test attributes
    schema_name = "test_schema"
    attributes = [
        StringAttributeMetadata(
            schema_name="test_attr",
            description=create_label(label="Attr desc"),
            display_name=create_label(label="Attr"),
        )
    ]
    description = "fooo"
    display_name = "daff"
    disp_collection_name = "asdf"

    # Expected outputs
    _name = " ".join(schema_name.split("_")[1:])

    base = define_entity(
        schema_name=schema_name,
        attributes=attributes,
    )

    assert base.schema_name == schema_name
    assert base.attributes == attributes
    assert base.description.localized_labels[0].label == "Label"
    assert base.display_name.localized_labels[0].label == _name
    assert base.display_collection_name.localized_labels[0].label == _name + "s"
    assert base.is_activity is False
    assert base.has_activities is False
    assert base.has_notes is False
    assert ".EntityMetadata" in base.odata_type

    base = define_entity(
        schema_name=schema_name,
        attributes=attributes,
        description=description,
        display_name=display_name,
        display_collection_name=disp_collection_name,
    )

    assert base.schema_name == schema_name
    assert base.attributes == attributes
    assert base.description.localized_labels[0].label == description
    assert base.display_name.localized_labels[0].label == display_name
    assert base.display_collection_name.localized_labels[0].label == disp_collection_name
    assert base.is_activity is False
    assert base.has_activities is False
    assert base.has_notes is False
    assert ".EntityMetadata" in base.odata_type

    encoded = base.dump_to_dataverse()

    assert encoded["SchemaName"] == schema_name
    assert encoded["Attributes"][0]["SchemaName"] == attributes[0].schema_name
    assert encoded["IsActivity"] is False
    assert "@odata.type" in encoded.keys()
    assert ".EntityMetadata" in encoded["@odata.type"]
