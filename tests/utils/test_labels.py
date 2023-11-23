from dataverse.utils.labels import define_label
import pytest


def test_define_label(label):
    lbl = define_label(label)

    assert lbl == label

    name = "test"
    lbl = define_label(name)

    assert len(lbl.localized_labels) == 1
    assert lbl.localized_labels[0].label == "test"
    assert lbl.localized_labels[0].language_code == 1033

    lbl = define_label()

    assert len(lbl.localized_labels) == 1
    assert lbl.localized_labels[0].label == ""
    assert lbl.localized_labels[0].language_code == 1033

    lbl = define_label(name, language_code=123)

    assert len(lbl.localized_labels) == 1
    assert lbl.localized_labels[0].label == "test"
    assert lbl.localized_labels[0].language_code == 123

    lbl = define_label(label=None, override="hello")

    assert len(lbl.localized_labels) == 1
    assert lbl.localized_labels[0].label == "hello"
    assert lbl.localized_labels[0].language_code == 1033

    with pytest.raises(TypeError, match="Wrong type supplied!"):
        define_label(label=123)
