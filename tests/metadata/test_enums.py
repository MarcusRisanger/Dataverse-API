from dataverse_api.metadata.enums import BaseEnum


def test_base_enum():
    class Stuff(BaseEnum):
        FOO = 1

    a = Stuff.FOO
    assert a._get_value() == 1
