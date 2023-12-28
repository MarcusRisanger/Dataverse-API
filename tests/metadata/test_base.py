from pydantic import Field

from dataverse.metadata.base import MetadataBase


def test_base():
    test_str = "Hello"
    test_int = 123
    test_type = "Schmoo"

    class Bar(MetadataBase):
        aa_bb_cc: str = "Hello"

    class Foo(MetadataBase):
        bar: Bar = Field(default_factory=Bar)
        odata_type: str = Field(default=test_type, alias="@odata.type")
        my_str: str
        my_int: int

    inst = Foo.model_validate_dataverse({"MyStr": test_str, "MyInt": test_int, "@odata.type": test_type})

    assert inst.my_int == test_int
    assert inst.my_str == test_str
    assert inst.odata_type == test_type

    a = inst.dump_to_dataverse()

    # Check camel-casing and special case @odata.type
    assert all([i in a.keys() for i in ["MyStr", "MyInt", "@odata.type", "Bar"]])

    # Check values
    test_int, test_str, test_type == a["MyStr"], a["MyInt"], a["@odata.type"]

    # Check length
    assert len(a) == 4
    assert a["Bar"].get("AaBbCc") == "Hello"
