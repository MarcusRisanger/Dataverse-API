import logging
from dataclasses import dataclass

import pandas as pd
import pytest

from dataverse_api.dataclasses import DataverseBatchCommand
from dataverse_api.errors import DataverseError
from dataverse_api.utils import (
    chunk_data,
    convert_data,
    expand_headers,
    extract_batch_response_data,
    extract_key,
)

log = logging.getLogger()


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
