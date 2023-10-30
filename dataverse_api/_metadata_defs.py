"""
This contains some dataclasses for metadata handling.
"""

from dataclasses import dataclass
from typing import Any, Literal, Optional, Type, Union

from box import Box as RawMetadata  # noqa

BASE = "Microsoft.Dynamics.CRM."


@dataclass
class ManagedProperty:
    """
    Creates metadata for managed properties in Dataverse.
    """

    value: bool
    can_be_changed: bool
    managed_property_name: str

    def __call__(self) -> dict[str, Any]:
        return {
            "Value": self.value,
            "CanBeChanged": self.can_be_changed,
            "ManagedPropertyLogicalName": self.managed_property_name,
        }


@dataclass(frozen=True)
class RequiredLevel:
    value: Literal["None", "ApplicationRequired", "Recommended"] = "None"
    can_be_changed: bool = True

    def __call__(self) -> dict[str, Any]:
        return {
            "Value": self.value,
            "CanBeChanged": self.can_be_changed,
            "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
        }


@dataclass(frozen=True)
class LocalizedLabel:
    """
    Create metadata for LocalizedLabel, with default
    language code for US English.
    """

    label: str
    languagecode: int = 1033

    def __call__(self) -> dict[str, Any]:
        return {
            "@odata.type": BASE + "LocalizedLabel",
            "Label": self.label,
            "LanguageCode": self.languagecode,
        }


@dataclass(frozen=True)
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


class AttributeMetadata(_BaseMetadata):
    """
    To have a common descendant for Attributes.
    """

    pass


@dataclass
class AutoNumberMetadata(AttributeMetadata):
    """
    Create metadata for StringAttribute with autonumbering.
    Includes metadata from `AttributeMetadata` and `BaseMetadata`.
    """

    auto_number_format: str
    is_primary: bool = False
    max_length: int = 100
    required_level: RequiredLevel = RequiredLevel()

    def __call__(self) -> dict[str, Any]:
        base = {
            "@odata.type": BASE + "StringAttributeMetadata",
            "AttributeType": "String",
            "AttributeTypeName": {"Value": "StringType"},
            "FormatName": {"Value": "Text"},
            "IsPrimaryName": self.is_primary,
            "MaxLength": self.max_length,
            "AutoNumberFormat": self.auto_number_format,
            "RequiredLevel": self.required_level(),
        }
        base.update(self._base_metadata())
        return base


@dataclass
class StringAttributeMetadata(AttributeMetadata):
    """
    Create metadata for StringAttribute.
    Includes metadata from `AttributeMetadata` and `BaseMetadata`.
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
    ] = "Text"
    max_length: int = 100
    is_primary: bool = False
    required_level: RequiredLevel = RequiredLevel()

    def __call__(self) -> dict[str, Any]:
        base = {
            "@odata.type": BASE + "StringAttributeMetadata",
            "AttributeType": "String",
            "AttributeTypeName": {"Value": "StringType"},
            "IsPrimaryName": self.is_primary,
            "FormatName": {"Value": self.format_name},
            "MaxLength": self.max_length,
            "RequiredLevel": self.required_level(),
        }
        base.update(self._base_metadata())
        return base


@dataclass
class LookupAttributeMetadata(AttributeMetadata):
    """
    Create metadata for Lookup attribute.
    Includes metadata from `AttributeMetadata` and `BaseMetadata`.
    """

    required_level: RequiredLevel = RequiredLevel()

    def __call__(self) -> dict[str, Any]:
        base = {
            "@odata.type": BASE + "LookupAttributeMetadata",
            "AttributeType": "Lookup",
            "AttributeTypeName": {"Value": "LookupType"},
            "RequiredLevel": self.required_level(),
        }
        base.update(self._base_metadata())
        return base


@dataclass
class DecimalAttributeMetadata(AttributeMetadata):
    """
    Create metadata for Decimal attribute.
    Includes metadata for `AttributeMetadata` and `BaseMetadata`.
    """

    min_value: float
    max_value: float
    precision: int = 2
    required_level: RequiredLevel = RequiredLevel()

    def __call__(self) -> dict[str, Any]:
        base = {
            "@odata.type": BASE + "DecimalAttributeMetadata",
            "AttributeType": "Decimal",
            "AttributeTypeName": {"Value": "DecimalType"},
            "MinValue": self.min_value,
            "MaxValue": self.max_value,
            "Precision": self.precision,
            "RequiredLevel": self.required_level(),
        }
        base.update(self._base_metadata())
        return base


@dataclass
class IntegerAttributeMetadata(AttributeMetadata):
    """
    Create metadata for Integer attribute.
    Includes metadata for `AttributeMetadata` and `BaseMetadata`.
    """

    min_value: int
    max_value: int
    format: Literal["None", "Duration", "TimeZone", "Language", "Locale"] = "None"
    required_level: RequiredLevel = RequiredLevel()

    def __call__(self) -> dict[str, Any]:
        base = {
            "@odata.type": BASE + "IntegerAttributeMetadata",
            "AttributeType": "Decimal",
            "AttributeTypeName": {"Value": "DecimalType"},
            "MinValue": self.min_value,
            "MaxValue": self.max_value,
            "Format": self.format,
            "RequiredLevel": self.required_level(),
        }
        base.update(self._base_metadata())
        return base


@dataclass
class DateTimeAttributeMetadata(AttributeMetadata):
    """
    Create metadata for DateTime attribute.
    Includes metadata for `AttributeMetadata` and `BaseMetadata`.
    """

    format: Literal["DateOnly", "DateAndTime"] = "DateAndTime"
    required_level: RequiredLevel = RequiredLevel()

    def __call__(self) -> dict[str, Any]:
        base = {
            "@odata.type": BASE + "DateTimeAttributeMetadata",
            "AttributeType": "DateTime",
            "AttributeTypeName": {"Value": "DateTimeType"},
            "Format": self.format,
            "RequiredLevel": self.required_level(),
        }
        base.update(self._base_metadata())
        return base


@dataclass
class MemoAttributeMetadata(AttributeMetadata):
    """
    Create metadata for Memo attribute.
    Includes metadata for `AttributeMetadata` and `BaseMetadata`.
    """

    max_length: int
    required_level: RequiredLevel = RequiredLevel()

    def __call__(self) -> dict[str, Any]:
        base = {
            "@odata.type": BASE + "MemoAttributeMetadata",
            "AttributeType": "Memo",
            "AttributeTypeName": {"Value": "MemoType"},
            "Format": "TextArea",
            "MaxLength": self.max_length,
            "RequiredLevel": self.required_level(),
        }
        base.update(self._base_metadata())
        return base


CascadeType = Literal[
    "NoCascade",
    "Cascade",
    "Active",
    "UserOwned",
    "RemoveLink",
    "Restrict",
]


@dataclass
class CascadeConfiguration:
    """
    Complex Enum for Cascade Configuration.
    """

    assign: CascadeType = "Cascade"
    delete: CascadeType = "Cascade"
    merge: CascadeType = "Cascade"
    reparent: CascadeType = "Cascade"
    share: CascadeType = "Cascade"
    unshare: CascadeType = "Cascade"

    def __call__(self) -> dict[str, str]:
        return {
            "Assign": self.assign,
            "Delete": self.delete,
            "Merge": self.merge,
            "Reparent": self.reparent,
            "Share": self.share,
            "Unshare": self.unshare,
        }


@dataclass
class AssociatedMenuConfiguration:
    """
    Complex Enum for Associated Menu Config.
    """

    behavior: Literal[
        "UseCollectionName", "UseLabel", "DoNotDisplay"
    ] = "UseCollectionName"
    group: Literal["Details", "Sales", "Service", "Marketing"] = "Details"
    order: int = 10000
    label: Optional[Label] = Label("")

    def __call__(self) -> dict[str, Any]:
        return {
            "Behavior": self.behavior,
            "Group": self.group,
            "Label": self.label(),
            "Order": self.order,
        }


@dataclass
class _RelationshipMetadataBase:
    """
    Base metadata for RelationshipEntity
    """

    schema_name: str
    cascade_configuration: CascadeConfiguration

    def _base_metadata(self) -> dict[str, Any]:
        return {
            "SchemaName": self.schema_name,
            "CascadeConfiguration": self.cascade_configuration(),
        }


@dataclass
class OneToManyRelationshipMetadata(_RelationshipMetadataBase):
    """
    Create medatada for OneToManyRelationship.
    """

    referenced_entity: str
    referencing_entity: str
    lookup: LookupAttributeMetadata
    associated_menu_config: AssociatedMenuConfiguration

    def __call__(self) -> dict[str, Any]:
        relationship_metadata = {
            "@odata.type": "Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
            "SchemaName": self.schema_name,
            "CascadeConfiguration": self.cascade_configuration(),
            "AssociatedMenuConfiguration": self.associated_menu_config(),
            "ReferencedEntity": self.referenced_entity,
            "ReferencingEntity": self.referencing_entity,
            "Lookup": self.lookup(),
        }
        relationship_metadata.update(self._base_metadata())
        return relationship_metadata


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


@dataclass
class EntityKeyMetadata:
    """
    Create medatata for EntityKey.
    """

    schema_name: str
    display_name: Label
    key_attributes: list[str]

    def __call__(self) -> Any:
        return {
            "SchemaName": self.schema_name,
            "DisplayName": self.display_name(),
            "KeyAttributes": self.key_attributes,
        }
