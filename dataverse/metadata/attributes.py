"""
A collection of Dataverse Attribute metadata classes.
"""


from typing import Any

from pydantic import Field

from dataverse.metadata.base import BASE_TYPE, MetadataBase
from dataverse.metadata.complex_properties import Label, RequiredLevel, create_label, required_level_default
from dataverse.metadata.enums import AttributeType, AttributeTypeName, StringFormat


class AttributeMetadata(MetadataBase):
    """
    Base Metadata class for Attributes.
    """

    schema_name: str
    description: Label = Field(default_factory=create_label)
    display_name: Label = Field(default_factory=create_label)
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
    is_primary_name : bool
    """

    is_primary_name: bool = Field(default=False)
    max_length: int = Field(default=100)
    format_name: StringFormat = Field(default=StringFormat.TEXT)
    auto_number_format: str | None = Field(default=None)

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "StringAttributeMetadata"
        self.attribute_type = AttributeType.STRING
        self.attribute_type_name = AttributeTypeName.STRING_TYPE
        if self.auto_number_format is not None:
            self.format_name = StringFormat.TEXT
        return super().model_post_init(__context)
