"""
A collection of Dataverse Attribute metadata classes.
"""


from typing import Any

from pydantic import Field

from dataverse_api.metadata.base import BASE_TYPE, MetadataBase
from dataverse_api.metadata.complex_properties import Label, RequiredLevel, create_label, required_level_default
from dataverse_api.metadata.enums import (
    AttributeType,
    AttributeTypeName,
    DateTimeFormat,
    IntegerFormat,
    MemoFormat,
    StringFormat,
)
from dataverse_api.utils.labels import define_label


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
    format_name : dataverse.StringFormat
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


class DecimalAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a Decimal column.

    Parameters
    ----------
    schema_name : str
    min_value : float
    max_value : float
    precision : int
    description : dataverse.Label
    display_name : dataverse.Label
    required_level : dataverse.RequiredLevel
    """

    min_value: float
    max_value: float
    precision: int = 2

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "DecimalAttributeMetadata"
        self.attribute_type = AttributeType.DECIMAL
        self.attribute_type_name = AttributeTypeName.DECIMAL
        return super().model_post_init(__context)


class IntegerAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a Integer column.

    Parameters
    ----------
    schema_name : str
    min_value : int
    max_value : int
    precision : int
    format : dataverse.IntegerFormat
    description : dataverse.Label
    display_name : dataverse.Label
    required_level : dataverse.RequiredLevel
    """

    min_value: int
    max_value: int
    precision: int = 2
    format: IntegerFormat = Field(default=IntegerFormat.NONE)

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "IntegerAttributeMetadata"
        self.attribute_type = AttributeType.INTEGER
        self.attribute_type_name = AttributeTypeName.INTEGER
        return super().model_post_init(__context)


class DateTimeAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a DateTime column.

    Parameters
    ----------
    schema_name : str
    min_value : int
    max_value : int
    precision : int
    description : dataverse.Label
    display_name : dataverse.Label
    required_level : dataverse.RequiredLevel
    """

    format: DateTimeFormat = Field(default=DateTimeFormat.DATE_AND_TIME)

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "DateTimeAttributeMetadata"
        self.attribute_type = AttributeType.DATE_TIME
        self.attribute_type_name = AttributeTypeName.DATE_TIME
        return super().model_post_init(__context)


class MemoAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a Memo column.

    Parameters
    ----------
    schema_name : str
    description : dataverse.Label
    display_name : dataverse.Label
    required_level : dataverse.RequiredLevel
    """

    format: MemoFormat = Field(default=MemoFormat.TEXT_AREA)

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "MemoAttributeMetadata"
        self.attribute_type = AttributeType.MEMO
        self.attribute_type_name = AttributeTypeName.MEMO
        return super().model_post_init(__context)


class BigIntAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a Memo column.

    Parameters
    ----------
    schema_name : str
    description : dataverse.Label
    display_name : dataverse.Label
    required_level : dataverse.RequiredLevel
    """

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "BigIntAttributeMetadata"
        self.attribute_type = AttributeType.BIG_INT
        self.attribute_type_name = AttributeTypeName.BIG_INT
        return super().model_post_init(__context)


class BoolOption(MetadataBase):
    value: int
    label: Label


def bool_opt(val: bool) -> BoolOption:
    return BoolOption(value=val, label=define_label(str(val)))


class BooleanOptionSet(MetadataBase):
    true_option: BoolOption = Field(default=bool_opt(True))
    false_option: BoolOption = Field(default=bool_opt(False))

    def model_post_init(self, __context: Any) -> None:
        self.option_set_type = "Boolean"
        return super().model_post_init(__context)


class BooleanAttributeMetadata(AttributeMetadata):
    """
    Attribute Metadata for a Boolean column.

    Parameters
    ----------
    schema_name : str
    option_set : BooleanOptionSet
    description : dataverse.Label
    display_name : dataverse.Label
    required_level : dataverse.RequiredLevel
    """

    default_value: bool = False
    option_set: BooleanOptionSet = Field(default=BooleanOptionSet())

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "BooleanAttributeMetadata"
        self.attribute_type = AttributeType.BOOLEAN
        self.attribute_type_name = AttributeTypeName.BOOLEAN
        return super().model_post_init(__context)


AttributeTypes = (
    StringAttributeMetadata
    | DecimalAttributeMetadata
    | DateTimeAttributeMetadata
    | IntegerAttributeMetadata
    | BigIntAttributeMetadata
    | BooleanAttributeMetadata
    | MemoAttributeMetadata
)
