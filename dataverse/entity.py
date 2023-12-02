import requests

from dataverse._api import Dataverse


class DataverseEntity(Dataverse):
    def __init__(self, session: requests.Session, environment_url: str, logical_name: str):
        super().__init__(session=session, environment_url=environment_url)

        self._logical_name = logical_name

    @property
    def logical_name(self) -> str:
        return self._logical_name
