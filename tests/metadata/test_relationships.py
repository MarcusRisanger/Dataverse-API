from dataverse_api.metadata.attributes import LookupAttributeMetadata
from dataverse_api.metadata.relationships import OneToManyRelationshipMetadata, define_relationship
from dataverse_api.utils.labels import Label


def test_one_many_relationship(one_many_relationship: OneToManyRelationshipMetadata):
    a = one_many_relationship.dump_to_dataverse()
    one_many = one_many_relationship
    assert a["SchemaName"] == one_many_relationship.schema_name
    assert a["Description"]["LocalizedLabels"][0]["Label"] == one_many.description.localized_labels[0].label
    assert a["DisplayName"]["LocalizedLabels"][0]["Label"] == one_many.display_name.localized_labels[0].label


def test_define_relationship(schema_name: str, lookup: LookupAttributeMetadata):
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
