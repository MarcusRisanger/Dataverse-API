import logging
from typing import Optional
from urllib.parse import urljoin

from dataverse_api.dataclasses import DataverseAuth
from dataverse_api.entity import DataverseEntity

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)


class DataverseClient:
    """
    Base class used to instantiate Entities for manipulation.

    Args:
      - auth: Contains authorization callable and related Dataverse resource
    """

    def __init__(self, auth: DataverseAuth):
        self._auth = auth
        self._api_url = urljoin(auth.resource, "/api/data/v9.2/")
        self._entity_cache: dict[str, DataverseEntity] = {}
        self._default_headers = {
            "Accept": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Content-Type": "application/json",
        }

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
                auth=self._auth, logical_name=logical_name, validate=validate
            )

        return self._entity_cache[logical_name]
