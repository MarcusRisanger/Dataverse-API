import requests

from dataverse._api import Dataverse


class DataverseEntity(Dataverse):
    def __init__(self, session: requests.Session, environment_url: str, logical_name: str):
        super().__init__(session=session, environment_url=environment_url)

        self._logical_name = logical_name
        self._entity_set_name = self.__get_entity_set_name()

    @property
    def logical_name(self) -> str:
        return self._logical_name

    @property
    def entity_set_name(self) -> str:
        return self._entity_set_name

    def __get_entity_set_name(self) -> str:
        """
        To fetch the Entity Set Name of the Entity, used
        instead of Logical Name as API endpoint.
        """
        resp = self._api_call(
            method="GET",
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')",
            params={"$select": "EntitySetName"},
        )
        return resp.json()["EntitySetName"]
