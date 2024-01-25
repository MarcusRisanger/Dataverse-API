import pytest

from dataverse_api.metadata.complex_properties import Label
from dataverse_api.utils.labels import define_label


@pytest.fixture
def label_name() -> str:
    return "test"


def test_define_label_with_label(label: Label):
    lbl = define_label(label)

    assert lbl == label


def test_define_label_with_str(label_name: str):
    lbl = define_label(label_name)

    assert len(lbl.localized_labels) == 1
    assert lbl.localized_labels[0].label == label_name
    assert lbl.localized_labels[0].language_code == 1033


def test_define_label_without_arg():
    lbl = define_label()

    assert len(lbl.localized_labels) == 1
    assert lbl.localized_labels[0].label == "Label"
    assert lbl.localized_labels[0].language_code == 1033


def test_define_label_with_str_and_lang_code(label_name: str):
    lbl = define_label(label_name, language_code=123)

    assert len(lbl.localized_labels) == 1
    assert lbl.localized_labels[0].label == label_name
    assert lbl.localized_labels[0].language_code == 123


def test_define_label_with_none_and_override():
    lbl = define_label(label=None, override="hello")

    assert len(lbl.localized_labels) == 1
    assert lbl.localized_labels[0].label == "hello"
    assert lbl.localized_labels[0].language_code == 1033


def test_define_label_error():
    with pytest.raises(TypeError, match="Wrong type supplied!"):
        define_label(label=123)  # type: ignore
