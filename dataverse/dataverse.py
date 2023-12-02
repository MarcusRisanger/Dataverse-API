"""
Trying out new things..
"""


import requests

from dataverse._api import Dataverse
from dataverse.entity import DataverseEntity
from dataverse.metadata.entity import EntityMetadata
from dataverse.metadata.helpers import Publisher, Solution
from dataverse.metadata.relationships import RelationshipMetadata


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
        entity_definition: EntityMetadata,
        solution_name: str | None = None,
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

        Returns
        -------
        requests.Response
            The response from the server.
        """

        if solution_name:
            headers = {"MSCRM.SolutionName": solution_name}
        else:
            headers = None

        return self._api_call(
            method="post",
            url="EntityDefinitions",
            headers=headers,
            json=entity_definition(),
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
            method="delete",
            url=f"EntityDefinitions(LogicalName='{logical_name}')",
        )

    def create_relationship(
        self,
        relationship_definition: RelationshipMetadata,
    ) -> requests.Response:
        """
        Relate Entities.

        Parameters
        ----------
        relationship_definition : RelationshipMetadata
            The relationship definition to establish in Dataverse.

        Returns
        -------
        requests.Response
            The response from the server.
        """

        return self._api_call(
            method="post",
            url="RelationshipDefinitions",
            json=relationship_definition(),
        )

    def delete_relationship(
        self,
        logical_name: str,
    ) -> None:
        """
        Delete relationship between Entities.
        """

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
            method="post",
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
            method="post",
            url="solutions",
            json=solution_definition(),
        )
