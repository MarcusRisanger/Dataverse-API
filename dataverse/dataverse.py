"""
Trying out new things..

"""

from typing import Any, Callable
from urllib.parse import urljoin

import requests

from dataverse.errors import DataverseError
from dataverse.metadata.entity import EntityMetadata
from dataverse.metadata.helpers import Publisher, Solution
from dataverse.metadata.relationships import RelationshipMetadata


class Dataverse:
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
        self.__api = session
        self._endpoint = urljoin(environment_url, "api/data/v9.2/")

    def __api_call(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> requests.Response:
        """
        Send API call to Dataverse.

        Parameters
        ----------
        method : str
            Request method.
        url : str
            URL added to endpoint.
        headers : dict
            Optional request headers. Will replace defaults.
        data : dict
            String payload.
        json : str
            Serializable JSON payload.

        Returns
        -------
        requests.Response
            Response from API call.

        Raises
        ------
        requests.exceptions.HTTPError
        """
        request_url = urljoin(self._endpoint, url)

        default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "OData-Version": "4.0",
            "OData-MaxVersion": "4.0",
        }

        if headers:
            for k, v in headers.items():
                default_headers[k] = v

        try:
            resp = self.__api.request(
                method=method,
                url=request_url,
                headers=default_headers,
                data=data,
                json=json,
                timeout=120,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise DataverseError(
                message=(
                    f"Error with GET request: {e.args[0]}"
                    + f"{'// Response body: '+ e.response.text if e.response else ''}"
                ),
                response=e.response,
            ) from e

        return resp

    def entity(self) -> None:
        """
        Create interface for Entity.
        """

    def create_entity(
        self,
        entity_definition: Callable | EntityMetadata,
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
        """

        if solution_name:
            headers = {"MSCRM.SolutionName": solution_name}
        else:
            headers = None

        return self.__api_call(
            method="post",
            url="EntityDefinitions",
            headers=headers,
            json=entity_definition(),
        )

    def delete_entity(self, logical_name: str) -> requests.Response:
        """
        Delete Entity.
        """
        return self.__api_call(
            method="delete",
            url=f"EntityDefinitions(LogicalName='{logical_name}')",
        )

    def create_relationship(self, relationship_definition: Callable | RelationshipMetadata) -> requests.Response:
        """
        Relate Entities.
        """

        return self.__api_call(
            method="post",
            url="RelationshipDefinitions",
            json=relationship_definition(),
        )

    def delete_relationship(self, logical_name: str) -> None:
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
        publisher_definition :
        """
        return self.__api_call(
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
        """
        return self.__api_call(
            method="post",
            url="solutions",
            json=solution_definition(),
        )
