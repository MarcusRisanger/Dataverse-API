import pytest
import requests
import responses

from dataverse.dataverse import Dataverse
from dataverse.metadata.complex_properties import Label, LocalizedLabel


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
