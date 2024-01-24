"""
The Dataverse Entity metadata class, and a function to define it using as little
data as possible for ease of use.
"""


from collections.abc import Sequence
from typing import Any

from dataverse.metadata.attributes import StringAttributeMetadata
from dataverse.metadata.base import BASE_TYPE, MetadataBase
from dataverse.metadata.complex_properties import Label
from dataverse.metadata.enums import OwnershipType
from dataverse.utils.labels import define_label

# Should be union of all AttributeTypes
AttributeTypes = StringAttributeMetadata


class EntityMetadata(MetadataBase):
    """
    Entity Metadata for Dataverse.
    """

    schema_name: str
    description: Label
    display_name: Label
    display_collection_name: Label
    attributes: Sequence[AttributeTypes] | None = None
    ownership_type: OwnershipType
    is_activity: bool
    has_activities: bool
    has_notes: bool

    def model_post_init(self, __context: Any) -> None:
        self.odata_type = BASE_TYPE + "EntityMetadata"
        return super().model_post_init(__context)


def define_entity(
    schema_name: str,
    attributes: Sequence[AttributeTypes],
    description: str | Label | None = None,
    display_name: str | Label | None = None,
    display_collection_name: str | Label | None = None,
    ownership_type: OwnershipType = OwnershipType.NONE,
    is_activity: bool = False,
    has_activities: bool = False,
    has_notes: bool = False,
) -> EntityMetadata:
    """
    Defining an EntityMetadata instance.

    Parameters
    ----------
    schema_name : str
        Schema name of the Entity.
    attributes : list of AttributeMetadata
        List of `AttributeMetadata` where exactly one element must
        represent the primary attribute for the Entity.
    description: Optional string or `Label`
        A short description of the Entity. Defaults to empty string.
    display_name: Optional string or `Label`
        The display name of the Entity.
    display_collection_name:
        The display name of the Entity Collection (plural).
    """

    _name = " ".join(schema_name.split("_")[1:])

    # Handle labels
    description = define_label(description)
    display_name = define_label(display_name, _name)
    display_collection_name = define_label(display_collection_name, _name + "s")

    return EntityMetadata(
        schema_name=schema_name,
        attributes=attributes,
        description=description,
        display_name=display_name,
        display_collection_name=display_collection_name,
        ownership_type=ownership_type,
        is_activity=is_activity,
        has_activities=has_activities,
        has_notes=has_notes,
    )
