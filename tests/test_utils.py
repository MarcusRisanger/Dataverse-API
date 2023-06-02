import json
import logging

import pandas as pd
import pytest

from dataverse_api.utils import (
    DataverseBatchCommand,
    DataverseColumn,
    DataverseError,
    chunk_data,
    convert_data,
    expand_headers,
    extract_batch_response_data,
    extract_key,
    find_invalid_columns,
    parse_meta_columns,
    parse_meta_entity,
    parse_meta_keys,
)

log = logging.getLogger()


@pytest.fixture
def test_data_dict():
    data = {"a": "abc", "b": 2, "c": 3, "d": "hello"}
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
        return f.read()


@pytest.fixture
def entity_validation_data_bad():
    with open("tests/sample_data/test_entity_init_bad.txt") as f:
        return f.read()


@pytest.fixture
def processed_entity_validation_data(entity_validation_data):
    output = extract_batch_response_data(entity_validation_data)
    return output


def test_entity_validation(processed_entity_validation_data):
    output = processed_entity_validation_data
    assert len(output) == 3
    assert "$entity" in output[0]
    assert "/Attributes" in output[1]
    assert "/Keys" in output[2]


def test_metadata_parse_failures(entity_validation_data_bad):
    output = extract_batch_response_data(entity_validation_data_bad)
    data = [json.loads(i) for i in output]
    matcher = r"Payload does not contain .+"

    assert len(output) == 3
    with pytest.raises(DataverseError, match=matcher):
        parse_meta_entity(data[0])

    with pytest.raises(DataverseError, match=matcher):
        parse_meta_columns(data[1])

    with pytest.raises(DataverseError, match=matcher):
        parse_meta_keys(data[2])


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
    data, key = extract_key(test_data_dict, key_columns="a")

    assert key == "a='abc'"
    assert data == {"b": 2, "c": 3, "d": "hello"}

    # Pass key as 1-element set
    data, key = extract_key(test_data_dict, key_columns={"b"})

    assert key == "b=2"
    assert data == {"a": "abc", "c": 3, "d": "hello"}

    # Pass key as many-element set
    data, key = extract_key(test_data_dict, key_columns={"a", "b"})

    assert all(i in key for i in ["a='abc'", "b=2", ","])
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
    additional_headers = {"a": "foo", "q": "bar"}

    headers = expand_headers(test_data_dict, additional_headers)

    assert len(headers) == len(test_data_dict) + 1
    assert all([x in headers for x in ["a", "b", "c", "d", "q"]])
    assert headers["a"] == "foo"


@pytest.mark.parametrize(
    "mode, failure",
    [
        ("create", r"not valid for create: statecode"),
        ("update", r"not valid for update: importsequencenumber"),
    ],
)
def test_find_invalid_cols(mode, failure, processed_entity_validation_data):
    col_data = json.loads(processed_entity_validation_data[1])
    schema_columns: dict[str, DataverseColumn] = parse_meta_columns(col_data)

    key = {"testid"}
    cols = {"testid", "statecode", "importsequencenumber"}

    with pytest.raises(DataverseError, match=failure):
        find_invalid_columns(
            key_columns=key,
            data_columns=cols,
            schema_columns=schema_columns,
            mode=mode,
        )
