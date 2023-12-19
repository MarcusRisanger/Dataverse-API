import pytest

from dataverse.metadata.attributes import LookupAttributeMetadata, AutoNumberMetadata


@pytest.fixture
def lookup_field(description_label, display_name_label) -> LookupAttributeMetadata:
    return LookupAttributeMetadata(
        schema_name="Lookup",
        description=description_label,
        display_name=display_name_label,
    )


def test_lookup(lookup_field: LookupAttributeMetadata):
    a = lookup_field.dump_to_dataverse()

    assert len(a) == 7
    assert a["RequiredLevel"] == lookup_field.required_level.dump_to_dataverse()
    assert a["AttributeType"] == lookup_field.attribute_type.value  # enum
    assert a["AttributeTypeName"]["Value"] == lookup_field.attribute_type_name.value["value"]  # enum
    assert a["SchemaName"] == lookup_field.schema_name


def test_autonumber(description_label, display_name_label):
    a = AutoNumberMetadata(
        schema_name="autonumber",
        description=description_label,
        display_name=display_name_label,
        auto_number_format="ROWID-{SEQNUM:5}",
    )

    assert a.auto_number_format == "ROWID-{SEQNUM:5}"
