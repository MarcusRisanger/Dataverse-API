from dataclasses import dataclass

import pandas as pd
import pytest

from dataverse_api.dataclasses import (
    DataverseBatchCommand,
    DataverseExpand,
    DataverseOrderby,
)
from dataverse_api.errors import DataverseError
from dataverse_api.utils import (
    _altkey_encoding,
    _altkey_identify_illegal_symbols,
    chunk_data,
    convert_data,
    encode_altkeys,
    expand_headers,
    extract_batch_response_data,
    extract_key,
    parse_expand,
    parse_expand_element,
    parse_orderby,
)


@pytest.fixture
def test_data_dict():
    data = {"abc": "abc", "b": 2, "c": 3, "d": "hello"}
    return data


@pytest.fixture
def test_data_list(test_data_dict):
    data = [
        test_data_dict,
        {"a": "cba", "b": 4, "c": 6},
        {"a": "moo", "b": 3},
    ]
    return data


@pytest.fixture
def test_data_df(test_data_list):
    data = pd.DataFrame(test_data_list)
    return data


@pytest.fixture
def entity_validation_data():
    with open("tests/sample_data/test_entity_init.txt") as f:
        text = f.read()

    @dataclass
    class Mock:
        text: str

    return Mock(text=text)


@pytest.fixture
def entity_validation_data_bad():
    with open("tests/sample_data/test_entity_init_bad.txt") as f:
        return f.read()


@pytest.fixture
def processed_entity_validation_data(entity_validation_data):
    output = extract_batch_response_data(entity_validation_data)
    return output


@pytest.fixture
def test_data_batch_commands():
    data = [
        DataverseBatchCommand(uri="uri1", mode="mode1", data={"col1": 1, "col2": 2}),
        DataverseBatchCommand(uri="uri2", mode="mode1", data={"col1": 3, "col2": 4}),
        DataverseBatchCommand(uri="uri3", mode="mode1", data={"col1": 5, "col2": 6}),
    ]
    return data


def test_chunk_data(test_data_batch_commands):
    data = test_data_batch_commands
    data_size = len(data)

    assert data_size == 3

    size = 2
    a = chunk_data(data, size=size)

    first = a.__next__()

    assert len(first) == size
    assert first[0].data == {"col1": 1, "col2": 2}
    assert first[1].data == {"col1": 3, "col2": 4}

    second = a.__next__()

    assert len(second) == data_size - len(first)
    assert second[0].data == {"col1": 5, "col2": 6}


def test_extract_key_single_str(test_data_dict):
    # Pass key as str
    data, key = extract_key(test_data_dict, key_columns="abc")

    assert key == "abc='abc'"
    assert data == {"b": 2, "c": 3, "d": "hello"}

    # Pass key as 1-element set
    data, key = extract_key(test_data_dict, key_columns={"b"})

    assert key == "b=2"
    assert data == {"abc": "abc", "c": 3, "d": "hello"}

    # Pass key as many-element set
    data, key = extract_key(test_data_dict, key_columns={"abc", "b"})

    assert all(i in key for i in ["abc='abc'", "b=2", ","])
    assert data == {"c": 3, "d": "hello"}


def test_convert_data_dict(test_data_dict):
    data = convert_data(test_data_dict)
    assert data == [test_data_dict]


def test_convert_data_list(test_data_list):
    data = convert_data(test_data_list)
    assert data == test_data_list


def test_convert_data_df(test_data_df, test_data_list):
    data = convert_data(test_data_df)
    assert data == test_data_list


def test_convert_data_error():
    data = {"a", "b"}
    with pytest.raises(DataverseError, match=r"Data seems to be .+"):
        convert_data(data)


def test_expand_headers(test_data_dict):
    additional_headers = {"abc": "foo", "q": "bar"}

    headers = expand_headers(test_data_dict, additional_headers)

    assert len(headers) == len(test_data_dict) + 1
    assert all([x in headers for x in ["abc", "b", "c", "d", "q"]])
    assert headers["abc"] == "foo"


def test_parse_orderby():
    orderby_str = "abc,123"
    orderby_lst_valid = [DataverseOrderby("abc"), DataverseOrderby("123", "desc")]
    orderby_lst_invalid = [DataverseOrderby("abc"), DataverseOrderby("wrong")]

    assert parse_orderby(orderby_str) == "abc,123"
    assert parse_orderby(orderby_lst_valid) == "abc asc,123 desc"
    assert parse_orderby(orderby_lst_valid) == "abc asc,123 desc"
    assert parse_orderby(orderby_lst_invalid) == "abc asc,wrong asc"


@pytest.fixture
def expansion():
    exp = DataverseExpand(
        table="test",
        select=["col1", "col2"],
        filter="col1 eq 'hello'",
        orderby=DataverseOrderby("col1"),
        top=2,
    )
    return exp


@pytest.fixture
def expansion_result():
    expanded_string = (
        "test("
        + "$select=col1,col2;"
        + "$filter=col1 eq 'hello';"
        + "$orderby=col1 asc;"
        + "$top=2"
        + ")"
    )
    return expanded_string


def test_parse_expand(expansion, expansion_result):
    expand_str = "hello"

    assert parse_expand(expand_str) == "hello"
    assert parse_expand(expansion) == expansion_result


def test_parse_expand_element(expansion, expansion_result):
    assert parse_expand_element(expansion) == expansion_result


@pytest.fixture
def altkeys():
    return (("abc", "abc"), ("æ", "%C3%A6"), "abc%")


def test_altkey_illegal_symbols(altkeys):
    assert _altkey_identify_illegal_symbols(altkeys[0][0]) is None
    assert _altkey_identify_illegal_symbols(altkeys[1][0]) is None

    with pytest.raises(ValueError):
        _altkey_identify_illegal_symbols(altkeys[2])


def test_altkey_encoding(altkeys):
    assert _altkey_encoding(altkeys[0][0]) == f"'{altkeys[0][1]}'"
    assert _altkey_encoding(altkeys[1][0]) == f"'{altkeys[1][1]}'"


def test_encode_altkey():
    url = "http://stuff(a='a')"
    assert encode_altkeys(url) == url

    url = "http://stuff(a='æ')"
    assert encode_altkeys(url) == url.replace("æ", "%C3%A6")

    url = "http://stuff(a='æ')/moo(b='å')"
    assert encode_altkeys(url) == url.replace("æ", "%C3%A6").replace("å", "%C3%A5")
