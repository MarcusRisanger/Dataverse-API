import pytest

from dataverse_api.schema import DataverseSchema


@pytest.fixture
def schema_class():
    return DataverseSchema()
