"""
Contains the `DataverseClient` class, used to instantiate a connection
to the Dataverse Web API and to spawn `DataverseEntity` objects for
interacting with tables.

Author: Marcus Risanger
"""


import logging
from typing import Optional, Type, Union

from box import BoxError
from msal_requests_auth.auth import ClientCredentialAuth, DeviceCodeAuth

from dataverse_api._api import DataverseAPI
from dataverse_api._metadata_defs import (
    AssociatedMenuConfiguration,
    AttributeMetadata,
    AutoNumberMetadata,
    CascadeConfiguration,
    EntityMetadata,
    Label,
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    RawMetadata,
    RequiredLevel,
    StringAttributeMetadata,
)
from dataverse_api.dataclasses import DataverseAuth
from dataverse_api.entity import DataverseEntity
from dataverse_api.errors import DataverseError

log = logging.getLogger("dataverse-api")


class DataverseClient(DataverseAPI):
    """
    Base class used to instantiate Entities for manipulation.

    Args:
      - resource: Dynamics url for Dataverse environment
      - auth: Authorization callable for Dataverse resource
    """

    def __init__(
        self,
        resource: str,
        auth: Union[ClientCredentialAuth, DeviceCodeAuth],
    ):
        self._credentials = DataverseAuth(resource=resource, auth=auth)
        super().__init__(auth=self._credentials)
        self._entity_cache: dict[str, DataverseEntity] = {}

    def entity(
        self, logical_name: str, validate: Optional[bool] = False
    ) -> DataverseEntity:
        """
        Returns an Entity class capable of interacting with the given Dataverse Entity.

        Args:
          - entity_logical_name: The logical name of the Dataverse Entity. Use to
            enable query-based validation against the API to check column names etc.
            prior to running queries.
          - entity_set_name: The "API queryable" name of the Dataverse Entity. Use
            to bypass query-based validation, if your script arguments are already
            validated. This saves some API calls during initialization.

        Returns:
          - `DataverseEntity` readily instantiated.
        """
        if logical_name not in self._entity_cache:
            self._entity_cache[logical_name] = DataverseEntity(
                auth=self._credentials,
                logical_name=logical_name,
                validate=validate,
            )

        return self._entity_cache[logical_name]

    def create_entity(
        self,
        schema_name: str,
        description: Union[str, Label],
        display_name: Union[str, Label],
        attributes: Union[str, StringAttributeMetadata, list[Type[AttributeMetadata]]],
        display_collection_name: Optional[Union[str, Label]] = None,
        solution_name: Optional[str] = None,
        ownership_type: str = "None",
        has_activities: bool = False,
        has_notes: bool = False,
        is_activity: bool = False,
    ):
        """
        Creates a new Entity in the dataverse.

        Args:
          - schema_name: Entity schema name
          - attributes: Either a str (schema name of primary attr),
            `StringAttributeMetadata` for the primary attribute, or a list of
            `AttributeMetadata` descendants.
            contain at least one `StringAttributeMetadata` tagged as primary attribute.
          - description: String (or `Label`) describing the Entity.
          - display_name: String (or `Label`) with the Entity display name.
        Optional:
          - solution_name: Unique name of solution Entity should belong to
          - display_collection_name: String (or `Label`) with the Entity display name.
          - has_activities: Whether activities are associated with this Entity.
          - has_notes: Whether notes are associated with this Entity.
          - is_activity: Whether the Entity is an activity.
        """

        if isinstance(attributes, str):
            attributes = StringAttributeMetadata(
                schema_name=attributes,
                description=Label(f"Primary attribute for {schema_name}."),
                display_name=Label("Primary Attribute"),
                is_primary=True,
            )

        if isinstance(description, str):
            description = Label(description)

        if isinstance(display_name, str):
            display_name = Label(display_name)

        if display_collection_name is None:
            display_collection_name = display_name
        elif isinstance(display_collection_name, str):
            display_collection_name = Label(display_collection_name)

        if isinstance(attributes, StringAttributeMetadata):
            attributes = [attributes]

        # Controlling number of primary attributes to be == 1
        primary = 0
        for attr in attributes:
            if isinstance(attr, (StringAttributeMetadata, AutoNumberMetadata)):
                if attr.is_primary:
                    primary += 1
        if primary == 0:
            raise DataverseError("No primary attribute given.")
        elif primary > 1:
            raise DataverseError("Too many primary attributes given.")

        # Assigning Entity to specific solution, needs header
        if solution_name is not None:
            additional_headers = {"MSCRM.SolutionUniqueName": solution_name}
        else:
            additional_headers = None

        # Entity Definition
        table = EntityMetadata(
            schema_name=schema_name,
            description=description,
            display_name=display_name,
            display_collection_name=display_collection_name,
            ownership_type=ownership_type,
            has_activities=has_activities,
            has_notes=has_notes,
            is_activity=is_activity,
            attributes=attributes,
        )

        self._post(
            url="EntityDefinitions",
            additional_headers=additional_headers,
            json=table(),
        )

    def delete_entity(self, logical_name: str):
        """
        Deletes the targeted entity in Dataverse.

        Args:
          - logical_name: Logical name of Entity to delete.
        """

        self._delete(f"EntityDefinitions(LogicalName='{logical_name}')")

    def update_entity(
        self,
        entity_definition: RawMetadata,
        merge_labels: bool = False,
        solution_name: Optional[str] = None,
    ):
        """
        Updates Dataverse with the supplied EntityMetadata definition.

        Args:
          - entity_definition: `RawMetadata` (as returned by the
            `get_entity_metadata` method)
          - marge_labels: Whether the existing `LocalizedLabels`
            that are not overwritten by the new definition will be
            preserved or removed. Default behavior is `False`.
        """

        try:
            logical_name = RawMetadata.LogicalName
        except BoxError:
            raise DataverseError("Logical name not found in EntityMetadata.")

        additional_headers = dict()
        if merge_labels:
            additional_headers = {"MSCRM.MergeLabels": True}
        if solution_name:
            additional_headers = {"MSCRM.SolutionUniqueName": solution_name}
        if not additional_headers:
            additional_headers = None

        self._put(
            f"EntityDefinitions(LogicalName='{logical_name}')",
            additional_headers=additional_headers,
            json=entity_definition.to_json(),
        )

    def get_entity_metadata(self, logical_name: str) -> RawMetadata:
        """
        Retrieves the EntityMetadata from Dataverse encapsulated in a `Box`.

        Can be used to update the metadata of an entity using the
        `update_entity` method.

        Args:
          - logical_name: Logical name of Entity to retrieve Metadata for.
        """

        meta = self._get(f"EntityDefinitions(LogicalName='{logical_name}')").json()

        return RawMetadata(meta)

    def relate_entities_one_many(
        self,
        relationship_display_name: Union[str, Label],
        relationship_column_schema_name: str,
        one_side_entity_logical_name: str,
        many_side_entity_logical_name: str,
        relationship_schema_name: Optional[str] = None,
        cascade_configuration: Optional[CascadeConfiguration] = None,
        associated_menu_config: Optional[AssociatedMenuConfiguration] = None,
    ) -> None:
        """
        Establishes a new one-to-many relationship in Dataverse
        between tagged entities.

        Args:
          - relationship_display_name: Display name of relationship and lookup
          - relationship_column_schema_name: Lookup column schema name
          - one_side_entity_logical_name: Name of Entity on one-side
          - many_side_entity_logical_name: Name of Entity on many-side
          - cascade_configuration: Optional cascade config override.
          - associated_menu_config: Optonal associated menu config override.
        """
        if isinstance(relationship_display_name, str):
            relationship_display_name = Label(relationship_display_name)

        if relationship_schema_name is None:
            relationship_schema_name = (
                f"rel_{many_side_entity_logical_name}"
                + f"_{one_side_entity_logical_name}"
            )[:41]

        if cascade_configuration is None:
            cascade_configuration = CascadeConfiguration()

        if associated_menu_config is None:
            associated_menu_config = AssociatedMenuConfiguration()

        lookup = LookupAttributeMetadata(
            description=relationship_display_name,
            display_name=relationship_display_name,
            schema_name=relationship_column_schema_name,
            required_level=RequiredLevel(),
        )

        rel = OneToManyRelationshipMetadata(
            schema_name=relationship_schema_name,
            cascade_configuration=cascade_configuration,
            associated_menu_config=associated_menu_config,
            referenced_entity=one_side_entity_logical_name,
            referencing_entity=many_side_entity_logical_name,
            lookup=lookup,
        )

        self._post(url="RelationshipDefinitions", json=rel())
