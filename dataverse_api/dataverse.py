"""
Trying out new things..
"""

from collections.abc import Callable

import requests

from dataverse_api._api import Dataverse
from dataverse_api.entity import DataverseEntity
from dataverse_api.metadata.base import MetadataDumper
from dataverse_api.metadata.entity import EntityMetadata
from dataverse_api.metadata.helpers import Publisher, Solution
from dataverse_api.metadata.relationships import RelationshipMetadata
from dataverse_api.utils.batching import BatchCommand, RequestMethod


class DataverseClient(Dataverse):
    """
    The main entrypoint for communicating with a given Dataverse Environment.

    Parameters
    ----------
    session: requests.Session
        The authenticated session used to communicate with the Web API.
    environment_url : str
        The environment URL that is used as a base for all API calls.
    """

    def __init__(self, session: requests.Session, environment_url: str):
        super().__init__(session=session, environment_url=environment_url)

    def entity(self, logical_name: str) -> DataverseEntity:
        """
        Create interface for Entity.

        Parameters
        ----------
        logical_name : str
            The logical name of the Entity.

        Returns
        -------
        DataverseEntity
            A class with methods for working with a specific
            Entity in Dataverse.
        """
        return DataverseEntity(
            session=self._session,
            environment_url=self._environment_url,
            logical_name=logical_name,
        )

    def create_entity(
        self,
        entity_definition: MetadataDumper,
        *,
        solution_name: str | None = None,
        return_representation: bool = False,
    ) -> requests.Response:
        """
        Create new Entity.

        Parameters
        ----------
        entity_definition : `EntityMetadata``
            The full Entity definition.
        solution_name : str
            The unique solution name, if the Entity is to be
            created within such a scope.
        return_representation : bool
            Whether to include the metadata representation after update
            in the response from server.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        headers: dict[str, str] = dict()
        if return_representation:
            headers["Prefer"] = "return=representation"
        if solution_name:
            headers["MSCRM.SolutionName"] = solution_name

        return self._api_call(
            method=RequestMethod.POST,
            url="EntityDefinitions",
            headers=headers,
            json=entity_definition.dump_to_dataverse(),
        )

    def get_language_codes(self) -> list[int]:
        """
        Retrieves the language codes that are enabled for the Dataverse organization.
        Dataverse will only recognize `LocalizedLabels` with the LCID codes returned
        by this function.

        Returns
        -------
        list of int
            The language code IDs enabled for the Dataverse Organization.
        """
        resp = self._api_call(
            method=RequestMethod.GET,
            url="RetrieveAvailableLanguages",
        )
        return resp.json()["LocaleIds"]

    def get_entity_definition(self, logical_name: str) -> EntityMetadata:
        """
        Retrieves the Entity metadata definition from Dataverse.

        Parameters
        ----------
        logical_name : str
            The logical name of the Entity to fetch Metadata for.

        Returns
        -------
        EntityMetadata
            Required as basis for updating Entity metadata for an
            existing Entity in Dataverse.
        """
        resp = self._api_call(
            method=RequestMethod.GET,
            url=f"EntityDefinitions(LogicalName='{logical_name}')",
        )
        return EntityMetadata.model_validate_dataverse(resp.json())

    def update_entity(
        self,
        entity: MetadataDumper,
        *,
        solution_name: str | None = None,
        preserve_localized_labels: bool = False,
        return_representation: bool = False,
    ) -> requests.Response:
        """
        Updates the Entity definition in Dataverse.

        Parameters
        ----------
        entity : MetadataDumper
            The updated Entity metadata to be persisted in Dataverse.
        solution_name : str
            The Solution Name with which to associate the Entity.
        preserve_localized_labels : bool
            Whether to preserve localized labels that exist in Dataverse
            but are not part of the submitted EntityMetadata.
        return_representation : bool
            Whether to include the metadata representation after update
            in the response from server.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        headers: dict[str, str] = dict()
        if solution_name:
            headers["MSCRM.SolutionUniqueName"] = solution_name
        if preserve_localized_labels:
            headers["MSCRM.Mergelabels"] = "true"
        if return_representation:
            headers["Prefer"] = "return=representation"

        return self._api_call(
            method=RequestMethod.PUT,
            url=f"EntityDefinitions(LogicalName='{entity.schema_name.lower()}')",
            headers=headers,
            json=entity.dump_to_dataverse(),
        )

    def delete_entity(
        self,
        logical_name: str,
    ) -> requests.Response:
        """
        Delete Entity.

        Parameters
        ----------
        logical_name : str
            The logical name of the Entity to delete.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        return self._api_call(
            method=RequestMethod.DELETE,
            url=f"EntityDefinitions(LogicalName='{logical_name}')",
        )

    def create_relationship(
        self, relationship_definition: MetadataDumper, return_representation: bool = False
    ) -> requests.Response:
        """
        Relate Entities.

        Parameters
        ----------
        relationship_definition : MetadataDumper
            The relationship definition to establish in Dataverse.
        return_representation : bool
            Whether to include the metadata representation after creation
            in the response from server.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        headers: dict[str, str] = dict()
        if return_representation:
            headers["Prefer"] = "return=representation"

        return self._api_call(
            method=RequestMethod.POST,
            url="RelationshipDefinitions",
            headers=headers,
            json=relationship_definition.dump_to_dataverse(),
        )

    def get_relationship_definition(self, schema_name: str) -> RelationshipMetadata:
        """
        Retrieves the Relationship metadata definition from Dataverse.

        Parameters
        ----------
        schema_name : str
            The schema name of the Relationship to fetch Metadata for.

        Returns
        -------
        EntityMetadata
            Required as basis for updating Entity metadata for an
            existing Entity in Dataverse.
        """
        resp = self._api_call(
            method=RequestMethod.GET,
            url=f"RelationshipDefinitions(SchemaName='{schema_name}')",
        )
        return RelationshipMetadata.model_validate_dataverse(resp.json())

    def update_relationship(
        self,
        relationship: MetadataDumper,
        *,
        preserve_localized_labels: bool = False,
    ) -> requests.Response:
        """
        Updates the Relationship definition in Dataverse.

        Parameters
        ----------
        entity : MetadataDumper
            The updated metadata to be persisted in Dataverse.
        preserve_localized_labels : bool
            Whether to preserve localized labels that exist in Dataverse
            but are not part of the submitted metadata.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        headers: dict[str, str] = dict()
        if preserve_localized_labels:
            headers["MSCRM.Mergelabels"] = "true"

        return self._api_call(
            method=RequestMethod.PUT,
            url=f"RelationshipDefinitions(SchemaName='{relationship.schema_name}')",
            headers=headers,
            json=relationship.dump_to_dataverse(),
        )

    def delete_relationship(
        self,
        logical_name: str,
    ) -> requests.Response:
        """
        Delete relationship between Entities.

        Parameters
        ----------
        logial_name : str
            The logical name of the relationship that is to be deleted.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        return self._api_call(
            method=RequestMethod.DELETE,
            url=f"RelationshipDefinitions(LogicalName='{logical_name}')",
        )

    def create_publisher(
        self,
        publisher_definition: Publisher,
    ) -> requests.Response:
        """
        Create a new publisher in Dataverse.

        Parameters
        ----------
        publisher_definition : Publisher
            The Publisher to establish in Dataverse.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        return self._api_call(
            method=RequestMethod.POST,
            url="publishers",
            json=publisher_definition(),
        )

    def create_solution(
        self,
        solution_definition: Solution,
    ) -> requests.Response:
        """
        Create a solution related to a publisher.

        Parameters
        ----------
        solution_definition : Solution
            Describing the new solution to be added.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        return self._api_call(
            method=RequestMethod.POST,
            url="solutions",
            json=solution_definition(),
        )

    def submit_batch(
        self,
        batch_data: list[BatchCommand],
        batch_id_generator: Callable[[], str] | None = None,
    ) -> list[requests.Response]:
        """
        Submits a custom batch for processing.

        Parameters
        ----------
        batch_data : list of `dataverse.utils.batching.BatchCommand`
            The individual batching commands to be submitted to the Dataverse Environment.
        batch_id_generator : callable
            An optional function for providing a unique batch ID.

        Returns
        -------
        list of requests.Response
            The response from the server.
        """
        return self._batch_api_call(batch_commands=batch_data, id_generator=batch_id_generator)
