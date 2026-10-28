import pytest

from dataverse_api.utils.text import convert_dict_keys_to_snake, convert_dict_keys_to_title, encode_altkeys


def test_conversion_to_title():
    test_dict = {
        "hello_there": [{"data": 1, "foo": 2}],
        "test_string_yeah": {"target": "Kenobi"},
        "single": False,
        "@unconverted": "MooOoOoO",
    }
    out = convert_dict_keys_to_title(test_dict)

    assert out["HelloThere"] == [{"Data": 1, "Foo": 2}]
    assert out["TestStringYeah"] == {"Target": "Kenobi"}
    assert out["Single"] == test_dict["single"]
    assert out["@unconverted"] == test_dict["@unconverted"]


def test_conversion_to_snake():
    test_dict = {
        "HelloThere": [{"Data": 1, "foo": 2}],
        "TestStringYeah": {"Target": "Kenobi"},
        "Single": False,
        "@whatever": True,
    }
    out = convert_dict_keys_to_snake(test_dict)

    assert out["hello_there"] == [{"data": 1, "foo": 2}]
    assert out["test_string_yeah"] == {"target": "Kenobi"}
    assert out["single"] == test_dict["Single"]
    assert out["@whatever"] == test_dict["@whatever"]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("øøå('æø')", "øøå('%C3%A6%C3%B8')", id="preserve-unicode-entity-name"),
        pytest.param("abc('x x')", "abc('x%20x')", id="space-in-value"),
        pytest.param("abc(stuff='æ',more='abc')", "abc(stuff='%C3%A6',more='abc')", id="unicode-first-key"),
        pytest.param("abc(stuff='abc',more='æ')", "abc(stuff='abc',more='%C3%A6')", id="unicode-second-key"),
    ],
)
def test_altkeys_encode(value: str, expected: str):
    assert encode_altkeys(value) == expected
