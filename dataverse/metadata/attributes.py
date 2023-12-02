"""
A collection of Dataverse Attribute metadata classes.
"""

from dataclasses import dataclass, field

from dataverse.metadata.base import BASE_TYPE, MetadataBase
from dataverse.metadata.complex_properties import Label, RequiredLevel
from dataverse.metadata.enums import AttributeRequiredLevel, StringFormat


@dataclass
class AttributeMetadata(MetadataBase):
    """
    Base Metadata class for Attributes.
    """

    schema_name: str
    description: Label
    display_name: Label
    required_level: RequiredLevel = field(default_factory=lambda: RequiredLevel(value=AttributeRequiredLevel.NONE))


@dataclass(kw_only=True)
class LookupAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a Lookup column.
    """

    _attribute_type: str = field(init=False, default="Lookup")
    _attribute_type_name: dict[str, str] = field(init=False, default_factory=lambda: {"Value": "LookupType"})
    _odata_type: str = field(init=False, default=BASE_TYPE + "LookupAttributeMetadata")


@dataclass(kw_only=True)
class StringAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a String column.

    Parameters
    ----------
    schema_name : str
    description : Label
    display_name: Label
    required
    """

    is_primary_name: bool = False
    max_length: int = 100
    # format: StringFormat = field(default_factory=StringFormat.Text)
    format_name: StringFormat = field(default=StringFormat.TEXT)
    _attribute_type: str = field(init=False, default="String")
    _attribute_type_name: dict[str, str] = field(init=False, default_factory=lambda: {"Value": "StringType"})
    _odata_type: str = field(init=False, default=BASE_TYPE + "StringAttributeMetadata")


@dataclass(kw_only=True)
class AutoNumberMetadata(StringAttributeMetadata):
    """
    Attribute Metadata for an Auto-number column.
    """

    auto_number_format: str
    format_name: StringFormat = field(init=False, default=StringFormat.TEXT)
    required_level: RequiredLevel = field(
        init=False, default_factory=lambda: RequiredLevel(AttributeRequiredLevel.APPLICATION_REQUIRED)
    )
