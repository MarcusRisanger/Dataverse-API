import pytest

from dataverse_api.metadata.complex_properties import Label
from dataverse_api.utils.labels import define_label


def test_define_label_with_label(label: Label):
    lbl = define_label(label)

    assert lbl == label


@pytest.mark.parametrize(
    ("args", "kwargs", "expected_label", "expected_language_code"),
    [
        pytest.param(("test",), {}, "test", 1033, id="string-default-language"),
        pytest.param((), {}, "Label", 1033, id="no-arguments"),
        pytest.param(("test",), {"language_code": 123}, "test", 123, id="string-custom-language"),
        pytest.param((), {"label": None, "override": "hello"}, "hello", 1033, id="none-with-override"),
    ],
)
def test_define_label_from_arguments(args, kwargs, expected_label: str, expected_language_code: int):
    label = define_label(*args, **kwargs)

    assert len(label.localized_labels) == 1
    assert label.localized_labels[0].label == expected_label
    assert label.localized_labels[0].language_code == expected_language_code


def test_define_label_error():
    with pytest.raises(TypeError, match="Wrong type supplied!"):
        define_label(label=123)  # type: ignore
