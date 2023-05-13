import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, List, Set
from urllib.parse import urljoin

import requests
from msal_requests_auth.auth import ClientCredentialAuth

from dataverse_api.utils import DataverseError


@dataclass
class DataverseTableSchema:
    key: str
    columns: Set[str]
    altkeys: List[Set[str]]


class DataverseSchema:
    def __init__(
        self,
        auth: ClientCredentialAuth,
        api_url: str,
    ):
        self._api_url = api_url
        self._auth = auth
        self.entities: Dict[str, DataverseTableSchema] = {}

        raw_schema = self._fetch_metadata()
        self._parse_metadata(raw_schema)

    def _fetch_metadata(self) -> str:
        metadata_url = urljoin(self._api_url, "$metadata")
        headers = {"Accept": "application/xml"}

        try:
            response = requests.get(
                url=metadata_url,
                headers=headers,
                auth=self._auth,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error fetching metadata: {e}", response=e.response)

        return response.text

    def _parse_metadata(self, raw_schema: str) -> None:
        schema = ET.fromstring(raw_schema)
        for table in schema.findall(".//{*}EntityType"):
            # Get key
            key = table.find(".//{*}PropertyRef")
            if key is None:  # Some special entities have no key attribute
                continue
            else:
                key = key.attrib["Name"]

            table_name = table.attrib["Name"] + "s"
            columns: Set[str] = set()
            altkeys: List[Set[str]] = list()

            # Get all column names
            for column in table.findall(".//{*}Property"):
                columns.add(column.attrib["Name"])

            # Get alternate key column combinations, if any
            for altkey in table.findall(".//{*}Record[@Type='Keys.AlternateKey']"):
                key_columns = set()
                for key_part in altkey.findall(".//{*}PropertyValue"):
                    if key_part.attrib["Property"] == "Name":
                        key_columns.add(key_part.attrib["PropertyPath"])
                altkeys.append(key_columns)

            # Write to schema
            self.entities[table_name] = DataverseTableSchema(
                key=key, columns=columns, altkeys=altkeys
            )
