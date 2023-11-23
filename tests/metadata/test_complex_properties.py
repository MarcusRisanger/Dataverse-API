import pytest

from dataverse.metadata.complex_properties import (
    AssociatedMenuConfiguration,
    CascadeConfiguration,
    Label,
    LocalizedLabel,
    RequiredLevel,
    create_label,
)


@pytest.fixture
def localized_label() -> LocalizedLabel:
    return LocalizedLabel(label="Test", language_code=123)


def test_localized_label(localized_label: LocalizedLabel):
    assert callable(localized_label)

    a = localized_label()

    assert len(a) == 3
    assert a["Label"] == localized_label.label
    assert a["@odata.type"] == localized_label._odata_type
    assert a["LanguageCode"] == localized_label.language_code


@pytest.fixture
def single_label(localized_label) -> Label:
    return Label(localized_labels=[localized_label])


def test_single_label(single_label: Label):
    assert callable(single_label)

    a = single_label()

    assert len(a) == 2
    assert a["@odata.type"] == single_label._odata_type
    assert len(a["LocalizedLabels"]) == len(single_label.localized_labels)

    b: list[dict] = a["LocalizedLabels"]

    assert b[0]["Label"] == single_label.localized_labels[0].label
    assert b[0]["LanguageCode"] == single_label.localized_labels[0].language_code
    assert b[0]["@odata.type"] == single_label.localized_labels[0]._odata_type


def test_cascade_config():
    merge = "NoCascade"
    delete = "RemoveLink"

    a = CascadeConfiguration(merge=merge, delete=delete).__call__()

    assert a["Delete"] == delete
    assert a["Merge"] == merge

    assert len(a) - 2 == sum(1 for i in a.values() if i == "Cascade")


@pytest.fixture
def required_level() -> RequiredLevel:
    return RequiredLevel()


def test_required_level(required_level: RequiredLevel):
    assert callable(required_level)

    a = required_level()

    assert a["Value"] == required_level.value()
    assert a["ManagedPropertyLogicalName"] == required_level.managed_property_logical_name
    assert a["CanBeChanged"] == required_level.can_be_changed
    assert len(a) == 3


def test_create_label():
    # Testing when only supplying a label argument
    label = "Hello"
    a = create_label(label=label)
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 1
    assert a.localized_labels[0].label == label
    assert a.localized_labels[0].language_code == 1033

    # Explicitly passing `language_code` as None
    a = create_label(label=label, language_code=None)
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 1
    assert a.localized_labels[0].label == label
    assert a.localized_labels[0].language_code == 1033

    # Testing for supplied `language_code`
    language_code = 69
    a = create_label(label=label, language_code=language_code)
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 1
    assert a.localized_labels[0].label == label
    assert a.localized_labels[0].language_code == language_code

    # Testing for supplying a tuple in `label`
    a = create_label(label=(label, language_code))
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 1
    assert a.localized_labels[0].label == label
    assert a.localized_labels[0].language_code == language_code

    # Testing for supplying a list of tuples in `labels`
    label_2 = "Bye"
    language_code_2 = 123
    labels = [(label, language_code), (label_2, language_code_2)]
    a = create_label(labels=labels)
    assert isinstance(a, Label)
    assert len(a.localized_labels) == 2
    for localized_label, language_code in labels:
        for l in a.localized_labels:
            if l.label == localized_label:
                assert l.language_code == language_code


def test_create_label_failures():
    error = "Correct input was not provided."

    with pytest.raises(ValueError, match=error):
        create_label(label=123)

    with pytest.raises(TypeError):
        create_label(labels=[123])

    with pytest.raises(AssertionError, match="Labels should be strings!"):
        create_label(labels=[(123, 123), ("Hello", 123)])

    with pytest.raises(AssertionError, match="Language codes should be ints!"):
        create_label(labels=[("Hello", "123")])
