import logging
from datetime import date, datetime
from textwrap import dedent

import pytest

from dataverse_api.errors import DataverseError
from dataverse_api.utils.batching import BatchCommand, RequestMethod, check_altkey_support
from dataverse_api.utils.data import serialize_json


def test_batch_command_delete():
    url = "foo"
    method = RequestMethod.DELETE
    batch_id = "123"
    api_url = "http://test.com"

    expected_output = f"""\
    --{batch_id}
    Content-Type: application/http
    Content-Transfer-Encoding: binary

    {method.name} {api_url}/{url} HTTP/1.1
    Content-Type: application/json



    """

    command = BatchCommand(url=url, method=method)
    assert command.single_col is False
    assert command.content_type == "Content-Type: application/json"
    assert command.encode(batch_id=batch_id, api_url=api_url) == dedent(expected_output)


def test_batch_command_post():
    url = "foo"
    method = RequestMethod.POST
    batch_id = "123"
    api_url = "http://test.com"
    data = {"test": 123}

    expected_output = f"""\
    --{batch_id}
    Content-Type: application/http
    Content-Transfer-Encoding: binary

    {method.name} {api_url}/{url} HTTP/1.1
    Content-Type: application/json; type=entry


    {serialize_json(data)}
    """

    command = BatchCommand(url=url, method=method, data=data)
    assert command.single_col is False
    assert command.content_type == "Content-Type: application/json; type=entry"
    assert command.encode(batch_id=batch_id, api_url=api_url) == dedent(expected_output)


def test_batch_command_patch_with_header():
    url = "foo"
    method = RequestMethod.PATCH
    batch_id = "123"
    api_url = "http://test.com"
    data = {"test": 123}
    header = {"MSCRM.SuppressDuplicateDetection": "false"}

    expected_output = f"""\
    --{batch_id}
    Content-Type: application/http
    Content-Transfer-Encoding: binary

    {method.name} {api_url}/{url} HTTP/1.1
    Content-Type: application/json
    MSCRM.SuppressDuplicateDetection: false

    {serialize_json(data)}
    """

    command = BatchCommand(url=url, method=method, data=data, headers=header)
    assert command.single_col is False
    assert command.headers == header
    assert command.content_type == "Content-Type: application/json"
    assert command.encode(batch_id=batch_id, api_url=api_url) == dedent(expected_output)


def test_batch_command_put():
    url = "foo(row_id)"
    method = RequestMethod.PUT
    batch_id = "123"
    api_url = "http://test.com"
    data = {"test": 123}

    expected_output = f"""\
    --{batch_id}
    Content-Type: application/http
    Content-Transfer-Encoding: binary

    {method.name} {api_url}/{url}/{list(data.keys())[0]} HTTP/1.1
    Content-Type: application/json


    {{{'"value"'}: {data["test"]}}}
    """

    command = BatchCommand(url=url, method=method, data=data)
    assert command.single_col is True
    assert command.content_type == "Content-Type: application/json"
    assert command.encode(batch_id=batch_id, api_url=api_url) == dedent(expected_output)


def test_batch_altkey_encoding_letters():
    url = "hello(altkey='æøå')"
    batch = BatchCommand(url=url, method=RequestMethod.GET)
    assert batch.url == "hello(altkey='%C3%A6%C3%B8%C3%A5')"


def test_batch_altkey_encoding_space():
    url = "kenobi(altkey='hello there')"
    batch = BatchCommand(url=url, method=RequestMethod.GET)
    assert batch.url == "kenobi(altkey='hello%20there')"


def test_check_altkey_support_with_valid_types():
    keys = ["name", "age", "score"]
    data = [
        {"name": "Alice", "age": 30, "score": 95.5},
        {"name": "Bob", "age": 25, "score": 87.0},
    ]
    # Should not raise any warnings or exceptions
    check_altkey_support(keys, data)


def test_check_altkey_support_with_invalid_types(caplog):
    keys = ["name", "created_date"]
    data = [
        {"name": "Alice", "created_date": date(2023, 1, 1)},
        {"name": "Bob", "created_date": datetime(2023, 2, 1)},
    ]

    with caplog.at_level(logging.WARNING):
        check_altkey_support(keys, data)

    assert "complex types" in caplog.text
    assert "created_date" in caplog.text
    assert "datetime" in caplog.text
    assert "date" in caplog.text


def test_check_altkey_support_with_mixed_types(caplog):
    keys = ["id", "tags", "metadata"]
    data = [
        {"id": 1, "tags": ["tag1", "tag2"], "metadata": None},
        {"id": 2, "tags": ["tag3"], "metadata": {"key": "value"}},
    ]

    with caplog.at_level(logging.WARNING):
        check_altkey_support(keys, data)

    assert "complex types" in caplog.text
    assert "tags" in caplog.text
    assert "metadata" in caplog.text
    assert "list" in caplog.text
    assert "dict" in caplog.text or "NoneType" in caplog.text

    def test_check_altkey_support_with_missing_keys():
        keys = ["name", "missing_key"]
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        # Should raise DataverseError when keys are missing
        with pytest.raises(DataverseError):
            check_altkey_support(keys, data)


def test_check_altkey_support_with_empty_data():
    keys = ["name", "age"]
    data = []
    # Should not raise any warnings with empty data
    check_altkey_support(keys, data)
