"""
Contains the `DataverseClient` class, used to instantiate a connection
to the Dataverse Web API and to spawn `DataverseEntity` objects for
interacting with tables.

Author: Marcus Risanger
"""


import logging
from typing import Optional, Type, Union

from msal_requests_auth.auth import ClientCredentialAuth, DeviceCodeAuth

from dataverse_api._api import DataverseAPI
from dataverse_api._metadata_defs import (
    AttributeMetadata,
    EntityMetadata,
    Label,
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
        attributes: Union[StringAttributeMetadata, list[Type[AttributeMetadata]]],
        description: Union[str, Label],
        display_name: Union[str, Label],
        display_collection_name: Optional[Union[str, Label]] = None,
        ownership_type: str = "None",
        has_activities: bool = False,
        has_notes: bool = False,
        is_activity: bool = False,
    ):
        """
        Creates a new Entity in the dataverse.

        Args:
          - schema_name: Entity schema name
          - attributes: Entity attribute(s). If a list is submitted, the list must
            contain at least one `StringAttributeMetadata` tagged as primary attribute.
          - description
        """
        if type(description) == str:
            description = Label(description)

        if type(display_name) == str:
            display_name = Label(display_name)

        if display_collection_name is None:
            display_collection_name = display_name
        elif type(display_collection_name) == str:
            display_collection_name = Label(display_collection_name)

        if type(attributes) == StringAttributeMetadata:
            attributes = [attributes]

        # Too dumb right now to think of something more elegant
        primary = 0
        for attr in attributes:
            if type(attr) == StringAttributeMetadata:
                if attr.is_primary:
                    primary += 1
        if primary == 0 or primary > 1:
            raise DataverseError("No primary attribute given.")

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

        self._post(url="EntityDefinitions", json=table())
