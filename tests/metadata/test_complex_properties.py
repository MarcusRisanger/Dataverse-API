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


@pytest.mark.parametrize(
    ("kwargs", "expected_language_code"),
    [
        pytest.param({}, 1033, id="default-language"),
        pytest.param({"language_code": None}, 1033, id="explicit-none-language"),
        pytest.param({"language_code": 69}, 69, id="custom-language"),
    ],
)
def test_create_label_with_string(label_string: str, kwargs: dict[str, int | None], expected_language_code: int):
    label = create_label(label=label_string, **kwargs)

    assert isinstance(label, Label)
    assert len(label.localized_labels) == 1
    assert label.localized_labels[0].label == label_string
    assert label.localized_labels[0].language_code == expected_language_code


def test_create_label_with_list_of_tuples(label_string: str, lang_code: int):
    # Testing for supplying a list of tuples in `labels`
    label_string_2 = "Bye"
    lang_code_2 = 123
    labels = [(label_string, lang_code), (label_string_2, lang_code_2)]
    a = create_label(labels=labels)
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 2
    for localized_label, _lang_code in labels:
        for label in a.localized_labels:
            if label.label == localized_label:
                assert label.language_code == _lang_code


def test_create_label_failure_1():
    with pytest.raises(TypeError):
        create_label(labels=[123])


def test_create_label_failure_2():
    with pytest.raises(ValueError, match=r"Input should be a valid integer"):
        create_label(label="Hello", language_code="foo")
