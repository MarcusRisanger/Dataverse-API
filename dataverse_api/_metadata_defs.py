"""
This contains some dataclasses for metadata handling.
"""

from dataclasses import dataclass
from typing import Any, Optional, Type

BASE = "Microsoft.Dynamics.CRM."


@dataclass
class LocalizedLabel:
    """
    Create metadata for LocalizedLabel, with default
    language code for US English.
    """

    label: str
    languagecode: int

    def __call__(self) -> dict:
        return {
            "@odata.type": BASE + "LocalizedLabel",
            "Label": self.label,
            "LanguageCode": self.languagecode,
        }


@dataclass
class Label:
    """
    Create metadata for Label.

    Use either a specific label and country code, or
    use the `labels` argument with a list of `LocalizedLabel`.

    Default country code is 1033 (US English).
    """

    label: Optional[str] = None
    country_code: Optional[int] = 1033
    labels: Optional[list[LocalizedLabel]] = None

    def __call__(self):
        if self.labels is not None:
            self.localized_labels = self.labels
        else:
            self.localized_labels = [LocalizedLabel(self.label, self.country_code)]
        return {
            "@odata.type": BASE + "Label",
            "LocalizedLabels": [x() for x in self.localized_labels],
        }


@dataclass
class _BaseMetadata:
    """
    Base class for metadata containing "standard" elements.
    """

    description: Label
    display_name: Label
    schema_name: str


@dataclass
class RequiredLevel:
    level: str = "None"
    can_be_changed: bool = True

    def __call__(self):
        return {
            "Value": self.level,
            "CanBeChanged": self.can_be_changed,
            "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
        }


@dataclass
class AttributeMetadata(_BaseMetadata):
    """
    Create metadata for Attribute.
    """

    required_level: RequiredLevel


@dataclass
class StringAttributeMetadata(AttributeMetadata):
    max_length: int
    format_name: str
    is_primary: bool = False

    def __call__(self) -> Any:
        return {
            "@odata.type": BASE + "StringAttributeMetadata",
            "AttributeType": "String",
            "AttributeTypeName": {"Value": "StringType"},
            "Description": self.description(),
            "DisplayName": self.display_name(),
            "IsPrimaryName": self.is_primary,
            "RequiredLevel": self.required_level(),
            "SchemaName": self.schema_name,
            "FormatName": self.format_name,
            "MaxLength": self.max_length,
        }


@dataclass
class EntityMetadata(_BaseMetadata):
    """
    Create metadata for Entity.

    Only one Attribute must have a IsPrimaryName = True property.
    """

    display_collection_name: Label
    primary_attribute: StringAttributeMetadata
    ownership_type: str = "TeamOwned"
    attributes: Optional[list[Type[AttributeMetadata]]] = None
    has_activities: bool = False
    has_notes: bool = False
    is_activity: bool = False

    def __call__(self):
        if self.attributes is not None:
            self.attributes.append(self.primary_attribute)
        else:
            self.attributes = list()
            self.attributes.append(self.primary_attribute)

        return {
            "@odata.type": BASE + "EntityMetadata",
            "Attributes": [x() for x in self.attributes],
            "Description": self.description(),
            "DisplayCollectionName": self.display_collection_name(),
            "DisplayName": self.display_name(),
            "HasActivities": self.has_activities,
            "HasNotes": self.has_notes,
            "IsActivity": self.is_activity,
            "OwnershipType": self.ownership_type,
            "SchemaName": self.schema_name,
        }
