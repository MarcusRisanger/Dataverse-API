from textwrap import dedent

from dataverse_api.utils.batching import BatchCommand, RequestMethod
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
