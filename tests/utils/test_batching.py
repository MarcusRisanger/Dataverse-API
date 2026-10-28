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


@pytest.mark.parametrize(
    ("headers", "expected_headers"),
    [
        pytest.param(
            {"MSCRM.SuppressDuplicateDetection": "false"},
            "MSCRM.SuppressDuplicateDetection: false",
            id="single-header",
        ),
        pytest.param(
            {"If-Match": "*", "Prefer": "return=representation"},
            "If-Match: *\nPrefer: return=representation",
            id="multiple-headers",
        ),
    ],
)
def test_batch_command_patch_with_header(headers: dict[str, str], expected_headers: str):
    url = "foo"
    method = RequestMethod.PATCH
    batch_id = "123"
    api_url = "http://test.com"
    data = {"test": 123}

    expected_output = (
        f"--{batch_id}\n"
        "Content-Type: application/http\n"
        "Content-Transfer-Encoding: binary\n\n"
        f"{method.name} {api_url}/{url} HTTP/1.1\n"
        "Content-Type: application/json\n"
        f"{expected_headers}\n\n"
        f"{serialize_json(data)}\n"
    )

    command = BatchCommand(url=url, method=method, data=data, headers=headers)
    assert command.single_col is False
    assert command.headers == headers
    assert command.content_type == "Content-Type: application/json"
    assert command.encode(batch_id=batch_id, api_url=api_url) == expected_output


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

    {method.name} {api_url}/{url}/{next(iter(data.keys()))} HTTP/1.1
    Content-Type: application/json


    {{{'"value"'}: {data["test"]}}}
    """

    command = BatchCommand(url=url, method=method, data=data)
    assert command.single_col is True
    assert command.content_type == "Content-Type: application/json"
    assert command.encode(batch_id=batch_id, api_url=api_url) == dedent(expected_output)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param("hello(altkey='æøå')", "hello(altkey='%C3%A6%C3%B8%C3%A5')", id="unicode-value"),
        pytest.param("kenobi(altkey='hello there')", "kenobi(altkey='hello%20there')", id="space-in-value"),
    ],
)
def test_batch_altkey_encoding(url: str, expected: str):
    batch = BatchCommand(url=url, method=RequestMethod.GET)
    assert batch.url == expected


@pytest.mark.parametrize(
    ("keys", "data"),
    [
        pytest.param(
            ["name", "age", "score"],
            [{"name": "Alice", "age": 30, "score": 95.5}, {"name": "Bob", "age": 25, "score": 87.0}],
            id="valid-types",
        ),
        pytest.param(["name", "age"], [], id="empty-data"),
    ],
)
def test_check_altkey_support_without_warnings(keys: list[str], data, caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.WARNING, logger="dataverse_api"):
        check_altkey_support(keys, data)

    assert not caplog.records


@pytest.mark.parametrize(
    ("keys", "data", "expected_details"),
    [
        pytest.param(
            ["name", "created_date"],
            [
                {"name": "Alice", "created_date": date(2023, 1, 1)},
                {"name": "Bob", "created_date": datetime(2023, 2, 1)},
            ],
            ("created_date", "datetime", "date"),
            id="date-and-datetime",
        ),
        pytest.param(
            ["id", "tags", "metadata"],
            [
                {"id": 1, "tags": ["tag1", "tag2"], "metadata": None},
                {"id": 2, "tags": ["tag3"], "metadata": {"key": "value"}},
            ],
            ("tags", "metadata", "list", "dict", "NoneType"),
            id="mixed-complex-types",
        ),
    ],
)
def test_check_altkey_support_with_unsupported_types(
    keys: list[str], data, expected_details: tuple[str, ...], caplog: pytest.LogCaptureFixture
):
    with caplog.at_level(logging.WARNING, logger="dataverse_api"):
        check_altkey_support(keys, data)

    assert [(record.name, record.levelno) for record in caplog.records] == [
        ("dataverse_api.utils.batching", logging.WARNING)
    ]
    assert "complex types" in caplog.text
    for detail in expected_details:
        assert detail in caplog.text


def test_check_altkey_support_with_missing_keys():
    keys = ["name", "missing_key"]
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

    with pytest.raises(DataverseError, match="Column 'missing_key' used in alternate key not found"):
        check_altkey_support(keys, data)
