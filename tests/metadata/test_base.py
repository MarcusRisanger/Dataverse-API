from dataclasses import dataclass, field

from dataverse.metadata.base import MetadataBase


def test_base():
    test_str = "Hello"
    test_int = 123
    test_type = "Moo"

    @dataclass
    class Bar(MetadataBase):
        aa_bb_cc: str = "Hello"

    @dataclass
    class Foo(MetadataBase):
        bar: Bar = field(default_factory=Bar)
        _odata_type: str = test_type
        my_str: str = test_str
        my_int: int = test_int

    a = Foo().__call__()

    # Check camel-casing and special case @odata.type
    assert all([i in a.keys() for i in ["MyStr", "MyInt", "@odata.type", "Bar"]])

    # Check values
    test_int, test_str, test_type == a["MyStr"], a["MyInt"], a["@odata.type"]

    # Check length
    assert len(a) == 4
    assert a["Bar"].get("AaBbCc") == "Hello"
