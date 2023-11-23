import pytest

from dataverse.metadata.attributes import LookupAttributeMetadata
from dataverse.metadata.relationships import OneToManyRelationshipMetadata, define_relationship
from dataverse.utils.labels import define_label, Label


@pytest.fixture
def schema_name() -> str:
    return "TestSchema"


@pytest.fixture
def lookup(display_name_label, description_label) -> LookupAttributeMetadata:
    return LookupAttributeMetadata(
        schema_name="Lookup",
        description=description_label,
        display_name=display_name_label,
    )


@pytest.fixture
def one_many_relationship(schema_name, description_label, display_name_label, lookup) -> OneToManyRelationshipMetadata:
    return OneToManyRelationshipMetadata(
        schema_name=schema_name,
        description=description_label,
        display_name=display_name_label,
        referenced_entity="RefdEntity",
        referenced_attribute="RefdAttr",
        referencing_entity="ReffingEntity",
        lookup=lookup,
    )


def test_one_many_relationship(one_many_relationship: OneToManyRelationshipMetadata):
    assert callable(one_many_relationship)

    a = one_many_relationship()

    assert a["SchemaName"] == one_many_relationship.schema_name
    assert (
        a["Description"]["LocalizedLabels"][0]["Label"] == one_many_relationship.description.localized_labels[0].label
    )
    assert (
        a["DisplayName"]["LocalizedLabels"][0]["Label"] == one_many_relationship.display_name.localized_labels[0].label
    )


def test_define_relationship(schema_name, lookup):
    data = {
        "schema_name": schema_name,
        "referenced_entity": "ref'd entity",
        "referencing_entity": "reffing entity",
        "referenced_attribute": "ref'd attr",
        "description": "desc",
        "display_name": "disp",
    }

    rel1 = define_relationship(lookup=lookup, **data)

    assert rel1.lookup == lookup

    rel2 = define_relationship(lookup="some other lookup", **data)

    assert rel2.lookup.schema_name == "rel_ref'd entity"

    for rel in [rel1, rel2]:
        assert rel.schema_name == schema_name
        assert rel.referenced_attribute == "ref'd attr"
        assert rel.referenced_entity == "ref'd entity"
        assert rel.referencing_entity == "reffing entity"
        assert rel.is_valid_for_advanced_find is True
        assert isinstance(rel.description, Label)
        assert isinstance(rel.display_name, Label)
        assert len(rel.description.localized_labels) == 1
        assert len(rel.display_name.localized_labels) == 1
        assert rel.description.localized_labels[0].label == "desc"
        assert rel.display_name.localized_labels[0].label == "disp"
