from dataverse.utils.batching import BatchCommand, BatchMode
from textwrap import dedent
import json


def test_batch_command_delete():
    url = "foo"
    mode = BatchMode.DELETE
    batch_id = "123"
    api_url = "http://test.com"

    expected_output = f"""\
    --{batch_id}
    Content-Type: application/http
    Content-Transfer-Encoding: binary

    {mode.name} {api_url}/{url} HTTP/1.1
    Content-Type: application/json

    null
    """

    command = BatchCommand(url=url, mode=mode)
    assert command.single_col is False
    assert command.content_type == "Content-Type: application/json"
    assert command.encode(batch_id=batch_id, api_url=api_url) == dedent(expected_output)


def test_batch_command_post():
    url = "foo"
    mode = BatchMode.POST
    batch_id = "123"
    api_url = "http://test.com"
    data = {"test": 123}

    expected_output = f"""\
    --{batch_id}
    Content-Type: application/http
    Content-Transfer-Encoding: binary

    {mode.name} {api_url}/{url} HTTP/1.1
    Content-Type: application/json; type=entry

    {json.dumps(data)}
    """

    command = BatchCommand(url=url, mode=mode, data=data)
    assert command.single_col is False
    assert command.content_type == "Content-Type: application/json; type=entry"
    assert command.encode(batch_id=batch_id, api_url=api_url) == dedent(expected_output)


def test_batch_command_put():
    url = "foo(row_id)"
    mode = BatchMode.PUT
    batch_id = "123"
    api_url = "http://test.com"
    data = {"test": 123}

    expected_output = f"""\
    --{batch_id}
    Content-Type: application/http
    Content-Transfer-Encoding: binary

    {mode.name} {api_url}/{url}/{list(data.keys())[0]} HTTP/1.1
    Content-Type: application/json

    {{{'"value"'}: {data['test']}}}
    """

    command = BatchCommand(url=url, mode=mode, data=data, single_col=True)
    assert command.single_col is True
    assert command.content_type == "Content-Type: application/json"
    assert command.encode(batch_id=batch_id, api_url=api_url) == dedent(expected_output)
