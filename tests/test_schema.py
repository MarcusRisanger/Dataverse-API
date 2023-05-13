import os

import pytest
from pytest_mock import mocker  # noqa F401

from dataverse_api.schema import DataverseSchema


@pytest.fixture
def example_schema():
    print(os.getcwd() + "   LOLOLOL")
    file_path = "tests/test_data/test_schema.txt"
    full_path = os.path.join(os.getcwd(), file_path)
    with open(full_path) as f:
        data = f.read()
    return data


def test_dataverse_schema(mocker, example_schema):  # noqa F811
    mocker.patch.object(DataverseSchema, "_fetch_metadata", return_value=example_schema)
    schema = DataverseSchema(auth="auth", api_url="some_url")

    assert schema._api_url == "some_url"
    assert schema._auth == "auth"

    assert len(schema.entities) == 2

    for entity in schema.entities:
        assert schema.entities[entity].key in schema.entities[entity].columns
        for key in schema.entities[entity].altkeys:
            assert all(x in schema.entities[entity].columns for x in key)
