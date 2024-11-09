from dataverse_api.metadata.attributes import (
    AttributeType,
    AttributeTypeName,
    IntegerAttributeMetadata,
    LookupAttributeMetadata,
    StringAttributeMetadata,
    StringFormat,
)
from dataverse_api.metadata.complex_properties import Label


def test_lookup_attr(description_label: Label, display_name_label: Label) -> LookupAttributeMetadata:
    attribute = LookupAttributeMetadata(
        schema_name="Lookup",
        description=description_label,
        display_name=display_name_label,
    )

    attr = attribute.dump_to_dataverse()

    assert len(attr) == 7
    assert attr["RequiredLevel"] == attribute.required_level.dump_to_dataverse()
    assert attr["AttributeType"] == AttributeType.LOOKUP.value
    assert attr["AttributeTypeName"]["Value"] == AttributeTypeName.LOOKUP.value["value"]
    assert attr["SchemaName"] == attribute.schema_name


def test_integer_attr(description_label: Label, display_name_label: Label) -> IntegerAttributeMetadata:
    min, max = -5, 99
    attribute = IntegerAttributeMetadata(
        schema_name="Integer",
        description=description_label,
        display_name=display_name_label,
        min_value=min,
        max_value=max,
    )

    attr = attribute.dump_to_dataverse()

    assert "Precision" not in attr.keys()
    assert attr["MinValue"] == min
    assert attr["MaxValue"] == max
    assert attr["AttributeType"] == AttributeType.INTEGER.value
    assert attr["AttributeTypeName"]["Value"] == AttributeTypeName.INTEGER.value["value"]


def test_autonumber(description_label: Label, display_name_label: Label):
    a = StringAttributeMetadata(
        schema_name="autonumber",
        description=description_label,
        display_name=display_name_label,
        auto_number_format="ROWID-{SEQNUM:5}",
    )

    assert a.auto_number_format == "ROWID-{SEQNUM:5}"
    assert a.format_name == StringFormat.TEXT
