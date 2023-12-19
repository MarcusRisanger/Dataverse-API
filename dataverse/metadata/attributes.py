"""
A collection of Dataverse Attribute metadata classes.
"""


from typing import Any

from pydantic import Field

from dataverse.metadata.base import BASE_TYPE, MetadataBase
from dataverse.metadata.complex_properties import Label, RequiredLevel, required_level_default
from dataverse.metadata.enums import AttributeType, AttributeTypeName, StringFormat


class AttributeMetadata(MetadataBase):
    """
    Base Metadata class for Attributes.
    """

    schema_name: str
    description: Label
    display_name: Label
    required_level: RequiredLevel = Field(default_factory=required_level_default)


class LookupAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a Lookup column.

    Parameters
    ----------
    schema_name : str
    description : dataverse.Label
    display_name : dataverse.Label
    required_level : dataverse.RequiredLevel
    """

    def model_post_init(self, __context: Any) -> None:
        self.attribute_type = AttributeType.LOOKUP
        self.attribute_type_name = AttributeTypeName.LOOKUP
        self.odata_type = BASE_TYPE + "LookupAttributeMetadata"
        return super().model_post_init(__context)


class StringAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a String column.

    Parameters
    ----------
    schema_name : str
    max_length : int
    description : dataverse.Label
    display_name : dataverse.Label
    required_level : dataverse.RequiredLevel
    """

    is_primary_name: bool = False
    max_length: int = 100
    format_name: StringFormat = Field(default=StringFormat.TEXT)

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "StringAttributeMetadata"
        self.attribute_type = AttributeType.STRING
        self.attribute_type_name = AttributeTypeName.STRING_TYPE
        return super().model_post_init(__context)


class AutoNumberMetadata(StringAttributeMetadata):
    """
    Attribute Metadata for an Auto-number column.

    Parameters
    ----------
    schema_name : str
    autonumber_format: str
    description : dataverse.Label
    display_name : dataverse.Label
    required_level : dataverse.RequiredLevel
    """

    auto_number_format: str

    def model_post_init(self, __context: Any) -> None:
        self.format_name = StringFormat.TEXT
        return super().model_post_init(__context)
