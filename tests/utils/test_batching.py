import logging
from datetime import date, datetime
from textwrap import dedent

import pytest

from dataverse_api.utils.batching import BatchCommand, RequestMethod, transform_upsert_data
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


def test_transform_upsert_data_with_simple_altkey_types_no_warning(caplog):
    """Test that simple types (str, int, float, bool) don't trigger warnings."""
    data = [
        {"key1": "test", "key2": 123, "key3": 45.6, "value": "data1"},
        {"key1": "test2", "key2": 456, "key3": 78.9, "value": "data2"},
    ]
    keys = ["key1", "key2", "key3"]
    
    with caplog.at_level(logging.WARNING):
        result = list(transform_upsert_data(data, keys, is_primary_id=False))
    
    # Should not log any warnings
    assert len(caplog.records) == 0
    
    # Verify the transformation still works correctly
    assert len(result) == 2
    assert result[0][0] == "key1='test',key2=123,key3=45.6"
    assert result[0][1] == {"value": "data1"}


def test_transform_upsert_data_with_boolean_altkey_no_warning(caplog):
    """Test that boolean types don't trigger warnings."""
    data = [
        {"is_active": True, "name": "test", "value": "data1"},
        {"is_active": False, "name": "test2", "value": "data2"},
    ]
    keys = ["is_active", "name"]
    
    with caplog.at_level(logging.WARNING):
        result = list(transform_upsert_data(data, keys, is_primary_id=False))
    
    # Should not log any warnings
    assert len(caplog.records) == 0
    
    # Verify the transformation still works correctly
    assert len(result) == 2
    assert result[0][0] == "is_active=True,name='test'"
    assert result[0][1] == {"value": "data1"}


def test_transform_upsert_data_with_date_altkey_logs_warning(caplog):
    """Test that date types trigger a warning."""
    data = [
        {"created_date": date(2024, 1, 1), "name": "test", "value": "data1"},
        {"created_date": date(2024, 1, 2), "name": "test2", "value": "data2"},
    ]
    keys = ["created_date", "name"]
    
    with caplog.at_level(logging.WARNING):
        result = list(transform_upsert_data(data, keys, is_primary_id=False))
    
    # Should log a warning about the date type
    assert len(caplog.records) == 1
    assert "created_date" in caplog.records[0].message
    assert "date" in caplog.records[0].message
    assert "complex types" in caplog.records[0].message
    
    # Verify the transformation still proceeds
    assert len(result) == 2


def test_transform_upsert_data_with_datetime_altkey_logs_warning(caplog):
    """Test that datetime types trigger a warning."""
    data = [
        {"timestamp": datetime(2024, 1, 1, 12, 0, 0), "name": "test", "value": "data1"},
    ]
    keys = ["timestamp"]
    
    with caplog.at_level(logging.WARNING):
        result = list(transform_upsert_data(data, keys, is_primary_id=False))
    
    # Should log a warning about the datetime type
    assert len(caplog.records) == 1
    assert "timestamp" in caplog.records[0].message
    assert "datetime" in caplog.records[0].message
    
    # Verify the transformation still proceeds
    assert len(result) == 1


def test_transform_upsert_data_with_mixed_complex_types_logs_warning(caplog):
    """Test that multiple complex types are all reported in the warning."""
    data = [
        {
            "date_key": date(2024, 1, 1),
            "dict_key": {"nested": "value"},
            "name": "test",
            "value": "data1",
        },
    ]
    keys = ["date_key", "dict_key", "name"]
    
    with caplog.at_level(logging.WARNING):
        result = list(transform_upsert_data(data, keys, is_primary_id=False))
    
    # Should log a warning mentioning both complex types
    assert len(caplog.records) == 1
    warning_message = caplog.records[0].message
    assert "date_key" in warning_message
    assert "dict_key" in warning_message
    assert "date" in warning_message
    assert "dict" in warning_message
    
    # Verify the transformation still proceeds
    assert len(result) == 1


def test_transform_upsert_data_with_none_values_no_warning(caplog):
    """Test that None values in altkeys don't trigger warnings."""
    data = [
        {"key1": None, "key2": "test", "value": "data1"},
    ]
    keys = ["key1", "key2"]
    
    with caplog.at_level(logging.WARNING):
        result = list(transform_upsert_data(data, keys, is_primary_id=False))
    
    # Should not log any warnings (None is handled specially)
    assert len(caplog.records) == 0
    
    # Verify the transformation still works
    assert len(result) == 1


def test_transform_upsert_data_with_primary_id_no_validation(caplog):
    """Test that primary ID upserts don't trigger validation."""
    data = [
        {"id": date(2024, 1, 1), "value": "data1"},  # Even with date type
    ]
    keys = ["id"]
    
    with caplog.at_level(logging.WARNING):
        result = list(transform_upsert_data(data, keys, is_primary_id=True))
    
    # Should not log any warnings when using primary ID
    assert len(caplog.records) == 0
    
    # Verify the transformation still works
    assert len(result) == 1


def test_transform_upsert_data_with_empty_data_no_error(caplog):
    """Test that empty data doesn't cause errors."""
    data = []
    keys = ["key1", "key2"]
    
    with caplog.at_level(logging.WARNING):
        result = list(transform_upsert_data(data, keys, is_primary_id=False))
    
    # Should not log any warnings or raise errors
    assert len(caplog.records) == 0
    assert len(result) == 0
