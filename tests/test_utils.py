import pandas as pd
import pytest

from dataverse_api.utils import (
    DataverseBatchCommand,
    batch_id_generator,
    chunk_data,
    convert_data,
    extract_key,
)


@pytest.fixture
def test_data_dict():
    data = {"a": "abc", "b": 2, "c": 3, "d": "hello"}
    return data


@pytest.fixture
def test_data_list(test_data_dict):
    data = [
        test_data_dict,
        {"a": "cba", "b": 4, "c": 6},
    ]
    return data


@pytest.fixture
def test_data_df(test_data_list):
    data = pd.DataFrame(test_data_list)
    return data


def test_chunk_data():
    data = [
        DataverseBatchCommand(uri="uri1", mode="mode1", data={"col1": 1, "col2": 2}),
        DataverseBatchCommand(uri="uri2", mode="mode1", data={"col1": 3, "col2": 4}),
        DataverseBatchCommand(uri="uri3", mode="mode1", data={"col1": 5, "col2": 6}),
    ]
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
    result = extract_key(test_data_dict, key_columns={"a"})

    assert result == "a='abc'"
    assert test_data_dict == {"b": 2, "c": 3, "d": "hello"}


def test_extract_key_single_int(test_data_dict):
    result = extract_key(test_data_dict, key_columns={"b"})

    assert result == "b=2"
    assert test_data_dict == {"a": "abc", "c": 3, "d": "hello"}


def test_extract_key_composite(test_data_dict):
    result = extract_key(test_data_dict, key_columns={"a", "b"})

    assert result == "a='abc',b=2"
    assert test_data_dict == {"c": 3, "d": "hello"}


def test_batch_id_generator():
    for _ in range(10):
        id = batch_id_generator()

        assert len(id) == 36
        assert str(id)[8] == "-"
        assert str(id)[13] == "-"
        assert str(id)[14] == "4"


def test_convert_data_dict(test_data_dict):
    data = convert_data(test_data_dict)

    assert data == [test_data_dict]


def test_convert_data_list(test_data_list):
    data = convert_data(test_data_list)
    assert data == test_data_list


def test_convert_data_df(test_data_df, test_data_list):
    data = convert_data(test_data_df)
    assert data == test_data_list
