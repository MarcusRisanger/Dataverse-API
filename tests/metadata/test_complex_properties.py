import logging

import pytest

from dataverse_api.metadata.complex_properties import (
    CascadeConfiguration,
    CascadeType,
    Label,
    LocalizedLabel,
    RequiredLevel,
    create_label,
)


@pytest.fixture
def label_string() -> str:
    return "Test"


@pytest.fixture
def lang_code() -> int:
    return 69


@pytest.fixture
def localized_label(label_string: str) -> LocalizedLabel:
    return LocalizedLabel(label=label_string, language_code=123)


@pytest.fixture
def single_label(localized_label: LocalizedLabel) -> Label:
    return Label(localized_labels=[localized_label])


def test_localized_label(localized_label: LocalizedLabel):
    a = localized_label.dump_to_dataverse()

    assert a["Label"] == localized_label.label
    assert a["@odata.type"] == localized_label.odata_type
    assert a["LanguageCode"] == localized_label.language_code


def test_single_label(single_label: Label):
    a = single_label.dump_to_dataverse()

    assert len(a) == 2
    assert a["@odata.type"] == single_label.odata_type
    assert len(a["LocalizedLabels"]) == len(single_label.localized_labels)

    b: list[dict] = a["LocalizedLabels"]

    assert b[0]["Label"] == single_label.localized_labels[0].label
    assert b[0]["LanguageCode"] == single_label.localized_labels[0].language_code
    assert b[0]["@odata.type"] == single_label.localized_labels[0].odata_type


def test_cascade_config():
    merge = CascadeType.NO_CASCADE
    delete = CascadeType.REMOVE_LINK

    a = CascadeConfiguration(merge=merge, delete=delete).dump_to_dataverse()

    assert a["Delete"] == delete.value
    assert a["Merge"] == merge.value

    assert len(a) - 2 == sum(1 for i in a.values() if i == "Cascade")


@pytest.fixture
def required_level() -> RequiredLevel:
    return RequiredLevel()


def test_required_level(required_level: RequiredLevel):
    a = required_level.dump_to_dataverse()

    assert a["Value"] == required_level.value.value  # required_level.value is an enum!
    assert a["ManagedPropertyLogicalName"] == required_level.managed_property_logical_name
    assert a["CanBeChanged"] == required_level.can_be_changed
    assert len(a) == 3


def test_create_label_with_string(label_string: str):
    # Testing when only supplying a label argument
    a = create_label(label=label_string)
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 1
    assert a.localized_labels[0].label == label_string
    assert a.localized_labels[0].language_code == 1033


def test_create_label_with_string_with_lang_code_none(label_string: str):
    # Explicitly passing `language_code` as None
    a = create_label(label=label_string, language_code=None)
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 1
    assert a.localized_labels[0].label == label_string
    assert a.localized_labels[0].language_code == 1033


def test_create_label_with_string_and_lang_cdode(label_string: str, lang_code: int):
    # Testing for supplied `language_code`
    a = create_label(label=label_string, language_code=lang_code)
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 1
    assert a.localized_labels[0].label == label_string
    assert a.localized_labels[0].language_code == lang_code


def test_create_label_with_list_of_tuples(label_string: str, lang_code: int):
    # Testing for supplying a list of tuples in `labels`
    label_string_2 = "Bye"
    lang_code_2 = 123
    labels = [(label_string, lang_code), (label_string_2, lang_code_2)]
    a = create_label(labels=labels)
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 2
    for localized_label, lang_code in labels:
        for label in a.localized_labels:
            if label.label == localized_label:
                assert label.language_code == lang_code


def test_create_label_failure_1():
    with pytest.raises(TypeError):
        create_label(labels=[123])


def test_create_label_failure_2():
    with pytest.raises(ValueError, match=r"Input should be a valid integer") as e:
        logging.warning(e)
        create_label(label="Hello", language_code="foo")
