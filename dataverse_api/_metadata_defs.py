"""
This contains some dataclasses for metadata handling.
"""

from dataclasses import dataclass
from typing import Any, Literal, Type, Union

BASE = "Microsoft.Dynamics.CRM."


@dataclass
class ManagedProperty:
    """
    Creates metadata for managed properties in Dataverse.
    """

    value: Union[str, bool]
    can_be_changed: bool
    managed_property_name: str

    def __call__(self) -> dict[str, Any]:
        return {
            "Value": self.value,
            "CanBeChanged": self.can_be_changed,
            "ManagedPropertyLogicalName": self.managed_property_name,
        }


@dataclass
class LocalizedLabel:
    """
    Create metadata for LocalizedLabel, with default
    language code for US English.
    """

    label: str
    languagecode: int

    def __call__(self) -> dict[str, Any]:
        return {
            "@odata.type": BASE + "LocalizedLabel",
            "Label": self.label,
            "LanguageCode": self.languagecode,
        }


@dataclass
class Label(LocalizedLabel):
    """
    Create metadata for Label.

    Use either a specific label and country code, or
    use the `labels` argument with a list of `LocalizedLabel`.

    Default country code is 1033 (US English).
    """

    def __call__(self) -> dict[str, Any]:
        return {
            "@odata.type": BASE + "Label",
            "LocalizedLabels": [LocalizedLabel(self.label, self.languagecode)()],
        }


@dataclass
class _BaseMetadata:
    """
    Base class for metadata containing "standard" elements.
    """

    description: Label
    display_name: Label
    schema_name: str

    def _base_metadata(self):
        return {
            "Description": self.description(),
            "DisplayName": self.display_name(),
            "SchemaName": self.schema_name,
        }


@dataclass
class AttributeMetadata(_BaseMetadata):
    """
    Create metadata for Attribute.
    Includes metadata from BaseMetadata.
    """

    required_level: ManagedProperty

    def _attr_metadata(self) -> dict[str, Any]:
        attr_metadata = {
            "RequiredLevel": self.required_level(),
        }
        attr_metadata.update(self._base_metadata())
        return attr_metadata


@dataclass
class StringAttributeMetadata(AttributeMetadata):
    """
    Create metadata for StringAttribute.
    Includes metadata from AttributeMetadata and BaseMetadata.
    """

    format_name: Literal[
        "Email",
        "Text",
        "TextArea",
        "Url",
        "TickerSymbol",
        "PhoneticGuide",
        "VersionNumber",
        "Phone",
        "Json",
        "RichText",
    ]
    is_primary: bool
    max_length: int

    def __call__(self) -> dict[str, Any]:
        base = {
            "@odata.type": BASE + "StringAttributeMetadata",
            "AttributeType": "String",
            "AttributeTypeName": {"Value": "StringType"},
            "IsPrimaryName": self.is_primary,
            "FormatName": {"Value": self.format_name},
            "MaxLength": self.max_length,
        }
        base.update(self._attr_metadata())
        return base


@dataclass
class EntityMetadata(_BaseMetadata):
    """
    Create metadata for Entity.
    """

    attributes: Union[StringAttributeMetadata, list[Type[AttributeMetadata]]]
    display_collection_name: Label
    ownership_type: str
    is_activity: bool
    has_activities: bool
    has_notes: bool

    def __call__(self):
        entity_metadata = {
            "@odata.type": BASE + "EntityMetadata",
            "Attributes": [x() for x in self.attributes],
            "DisplayCollectionName": self.display_collection_name(),
            "IsActivity": self.is_activity,
            "HasActivities": self.has_activities,
            "HasNotes": self.has_notes,
            "OwnershipType": self.ownership_type,
        }
        entity_metadata.update(self._base_metadata())
        return entity_metadata
