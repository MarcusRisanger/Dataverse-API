"""
A collection of Dataverse Relationship metadata classes.
"""


from typing import Any

from pydantic import Field

from dataverse_api.metadata.attributes import LookupAttributeMetadata
from dataverse_api.metadata.base import BASE_TYPE, MetadataBase
from dataverse_api.metadata.complex_properties import AssociatedMenuConfiguration, CascadeConfiguration, Label
from dataverse_api.utils.labels import define_label


class RelationshipMetadata(MetadataBase):
    """
    Base Metadata class for Relationships.
    """

    schema_name: str
    is_valid_for_advanced_find: bool = True
    description: Label | None = Field(default=None)
    display_name: Label | None = Field(default=None)


class OneToManyRelationshipMetadata(RelationshipMetadata):
    """
    Metadata for a One-to-Many relationship between Entities.
    """

    referenced_entity: str
    referencing_entity: str
    lookup: LookupAttributeMetadata
    referenced_attribute: str | None = Field(default=None)
    cascade_configuration: CascadeConfiguration = Field(default_factory=CascadeConfiguration)
    associated_menu_configuration: AssociatedMenuConfiguration = Field(default_factory=AssociatedMenuConfiguration)

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "OneToManyRelationshipMetadata"
        return super().model_post_init(__context)


def define_relationship(
    schema_name: str,
    referencing_entity: str,
    referenced_entity: str,
    lookup: str | LookupAttributeMetadata,
    *,
    referenced_attribute: str | None = None,
    description: str | Label | None = None,
    display_name: str | Label | None = None,
    is_valid_for_advanced_find: bool = True,
) -> OneToManyRelationshipMetadata:
    """
    Defines a RelationshipMetadata between the referencing Entity (on the many-side)
    and the referenced Entity (on the one-side).

    Parameters
    ----------
    schema_name : str
        The Schema name for the Relationship.
    referencing_entity: str
        The Entity on the many-side of the Relationship.
    referenced_entity: str
        The Entity on the one-side of the Relationship.
    referenced_attribute:
        The attribute referenced by the Relationship.
    lookup : str or LookupAttributeMetadata
        Either a fully defined LookupAttributeMetadata or the
        schema name of the lookup column that will be created
        in the referencing Entity.
    description : str or `Label`
        Defines the description of the Relationship.
    display_name: str or `Label`
        The display name of the Relationship.
    """
    _name = " ".join(schema_name.split(" ")[1:])

    description = define_label(label=description, override="")
    display_name = define_label(label=display_name, override=_name)

    if isinstance(lookup, str):
        lookup_label = define_label(f"Relationship between {referencing_entity} and {referenced_entity}.")

        lookup = LookupAttributeMetadata(
            schema_name=f"rel_{referenced_entity[:41]}",
            description=lookup_label,
            display_name=lookup_label,
        )

    return OneToManyRelationshipMetadata(
        schema_name=schema_name,
        description=description,
        display_name=display_name,
        is_valid_for_advanced_find=is_valid_for_advanced_find,
        referenced_attribute=referenced_attribute,
        referenced_entity=referenced_entity,
        referencing_entity=referencing_entity,
        lookup=lookup,
    )
