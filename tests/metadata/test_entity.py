from dataverse.metadata.attributes import StringAttributeMetadata
from dataverse.utils.labels import define_label
from dataverse.metadata.entity import EntityMetadata, define_entity
from dataverse.metadata.enums import StringFormat


def test_create_entity_metadata():
    # Test attributes
    schema_name = "test_schema"
    attributes = [
        StringAttributeMetadata(
            "test_attr",
            description=define_label("Attr desc"),
            display_name=define_label("Attr"),
        )
    ]
    description = "fooo"
    display_name = "daff"
    disp_collection_name = "asdf"

    # Expected outputs
    _name = " ".join(schema_name.split("_")[1:])

    base: EntityMetadata = define_entity(
        schema_name=schema_name,
        attributes=attributes,
    )

    assert base.schema_name == schema_name
    assert base.attributes == attributes
    assert base.description.localized_labels[0].label == ""
    assert base.display_name.localized_labels[0].label == _name
    assert base.display_collection_name.localized_labels[0].label == _name + "s"
    assert base.is_activity is False
    assert base.has_activities is False
    assert base.has_notes is False
    assert ".EntityMetadata" in base._odata_type

    base: EntityMetadata = define_entity(
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
    assert ".EntityMetadata" in base._odata_type

    encoded = base()

    assert encoded["SchemaName"] == schema_name
    assert encoded["Attributes"][0]["SchemaName"] == attributes[0].schema_name
    assert encoded["IsActivity"] is False
    assert "@odata.type" in encoded.keys()
    assert ".EntityMetadata" in encoded["@odata.type"]
