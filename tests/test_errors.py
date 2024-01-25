from dataverse_api.errors import DataverseError


def test_dataverse_error():
    msg = "hello123"
    error = DataverseError(message=msg, response=None)

    assert error.args[0] == msg
    assert error.response is None

    error = DataverseError(message=msg, response=123)

    assert error.response == 123
