import pytest
import requests
import responses

from dataverse.dataverse import Dataverse
from dataverse.metadata.entity import define_entity
from dataverse.metadata.attributes import StringAttributeMetadata, LookupAttributeMetadata
from dataverse.metadata.relationships import OneToManyRelationshipMetadata
from dataverse.metadata.complex_properties import Label, LocalizedLabel
from dataverse.utils.labels import define_label


@pytest.fixture
def localized_label():
    return LocalizedLabel("Test", 69)


@pytest.fixture
def label(localized_label):
    label2 = LocalizedLabel("Other Label", 420)
    return Label(localized_labels=[localized_label, label2])


@pytest.fixture
def description_label() -> Label:
    return Label([LocalizedLabel("Description")])


@pytest.fixture
def display_name_label() -> Label:
    return Label([LocalizedLabel("Display Name")])


@pytest.fixture
def sample_entity():
    return define_entity(
        schema_name="test_entity",
        description=define_label("Entity Description"),
        display_name=define_label("Entity Display Name"),
        display_collection_name=define_label("Entity Display Collection Name"),
        attributes=[
            StringAttributeMetadata(
                "test_attr",
                description=define_label("Attr Desc"),
                display_name=define_label("Test Attr"),
                is_primary_name=True,
            )
        ],
    )


@pytest.fixture
def session() -> requests.Session:
    s = requests.Session()
    return s


@pytest.fixture
def endpoint() -> str:
    return "http://fun.com"


@pytest.fixture
def client(session, endpoint):
    return Dataverse(session=session, environment_url=endpoint)


@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps


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
