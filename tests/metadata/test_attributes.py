import pytest

from dataverse.metadata.attributes import LookupAttributeMetadata


@pytest.fixture
def lookup_field(description_label, display_name_label) -> LookupAttributeMetadata:
    return LookupAttributeMetadata(
        schema_name="Lookup",
        description=description_label,
        display_name=display_name_label,
    )


def test_lookup(lookup_field: LookupAttributeMetadata):
    assert callable(lookup_field)

    a = lookup_field()

    assert len(a) == 7
    assert a["RequiredLevel"] == lookup_field.required_level()
    assert a["AttributeType"] == lookup_field._attribute_type
    assert a["AttributeTypeName"] == lookup_field._attribute_type_name
    assert a["SchemaName"] == lookup_field.schema_name
