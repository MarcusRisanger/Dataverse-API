import pytest

from dataverse.metadata.complex_properties import Label, LocalizedLabel


@pytest.fixture
def description_label() -> Label:
    return Label([LocalizedLabel("Description")])


@pytest.fixture
def display_name_label() -> Label:
    return Label([LocalizedLabel("Display Name")])
