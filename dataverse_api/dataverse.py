from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime as dt
from textwrap import dedent
from typing import Any, Dict, List, Literal, Optional, Set, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from msal_requests_auth.auth import ClientCredentialAuth
from requests_toolbelt.utils import dump

from dataverse_api.schema import DataverseSchema
from dataverse_api.utils import DataverseError, chunk_data, expand_headers

log = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)


class DataverseClient:
    """
    Base class used to establish authorization and certain default parameters
    for connecting to Dataverse environment.

    Args:
      - dynamics_url: Base environment url
      - authorization: `ClientCredentialAuth` from `msal_requests_auth` providing
      the necessary app registration to interact with Dataverse
      - validate: Whether to retrieve Dataverse schema and apply validation rules
    """

    def __init__(
        self,
        dynamics_url: str,
        authorization: ClientCredentialAuth,
        validate: bool = False,
    ):
        self.api_url = urljoin(dynamics_url, "/api/data/v9.2/")
        self._auth = authorization
        self._validate = validate

        if self._validate:
            self.schema = DataverseSchema(self._auth, self.api_url)
        self._entity_cache: Dict[str, DataverseEntity] = {}
        self._default_headers = {
            "Accept": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Content-Type": "application/json",
        }

    def entity(self, entity_name: str) -> DataverseEntity:
        """
        Returns an Entity class capable of interacting with the given Dataverse Entity.
        """
        if entity_name not in self._entity_cache:
            self._entity_cache[entity_name] = DataverseEntity(
                client=self, entity_name=entity_name
            )

        return self._entity_cache[entity_name]

    def _post(
        self, url: str, data: Any, additional_headers: Optional[dict] = None
    ) -> requests.Response:
        """
        Post HTTP call to Dataverse.

        Args:
          - url: Appended to API endpoint
          - data: Request payload
          - headers: Headers to overwrite default headers
        """
        headers = expand_headers(self._default_headers, additional_headers)

        if headers["Content-Type"] == "application/json":
            data_payload = None
            json_payload = data
        else:
            data_payload = data
            json_payload = None

        url = urljoin(self.api_url, url)

        return requests.post(
            url=url,
            headers=headers,
            auth=self._auth,
            data=data_payload,
            json=json_payload,
        )

    def _put(self, url: str, data: Any, additional_headers: Optional[dict] = None):
        headers = expand_headers(self._default_headers, additional_headers)

        url = urljoin(self.api_url, url)

        return requests.put(url=url, headers=headers)


class DataverseEntity:
    def __init__(self, client: DataverseClient, entity_name: str):
        self._client = client
        self.entity_name = entity_name
        if client._validate:
            try:
                self.schema = client.schema.entities[entity_name]
            except KeyError:
                raise DataverseError("Entity %s not found in schema." % entity_name)

    def upsert(
        self,
        data: Union[dict, List[dict], pd.DataFrame],
        key_columns: Optional[Set[str]] = None,
    ) -> None:
        log.info(f"Performing upsert of {len(data)} elements into {self.entity_name}.")

        if self._client._validate:
            key_columns = self._validate_payload(data, mode="upsert")
            log.info("Data validation completed.")

        if key_columns is None and not self._client._validate:
            raise DataverseError("Key column(s) must be specified.")

        self._batch_operation(mode="PATCH", data=data, key_columns=key_columns)

    def _batch_operation(
        self,
        mode: Literal["PATCH", "POST", "PUT", "DELETE"],
        data: Union[dict, List[dict], pd.DataFrame],
        key_columns: Optional[Set[str]] = None,
    ):
        """
        Prepares passed data for batch operations against the Dataverse.

        Args:
           - mode: POST or PATCH for creating or upserting data.
           - data: Accepts a dict, list of dicts or a `pd.DataFrame` containing data.
           - key_columns: For PATCH mode, requires key column or alternate key columns
             for updating data.

        Raises:
           - DataverseError if no key columns are passed in PATCH mode.
           - DataverseError if batch operation fails.
        """
        if mode == "PATCH" and key_columns is None:
            raise DataverseError("PATCH mode requires key columns to be passed.")

        log.info(f"Preparing data for batch operation towards {self.entity_name}.")

        # Get into useable format
        if isinstance(data, dict):
            data = [data]
        elif isinstance(data, pd.DataFrame):
            data = [
                {k: v for k, v in m.items() if pd.notnull(v)}
                for m in data.to_dict("records")
            ]

        # Dataverse operates with max batch operation commands of 1000
        for data_chunk in chunk_data(data, size=1000):
            batch_id = "batch_%s" % uuid.uuid4().hex
            batch_data = ""

            for row in data_chunk:
                # Row key
                row_key = ""
                if mode == "PATCH":
                    key_elements = []
                    for col in key_columns:
                        key_elements.append(f"{col}={row.pop(col).__repr__()}")
                    row_key = ",".join(key_elements)

                # Each batch operation must be appended to the command string
                row_command = f"""\
                --{batch_id}
                Content-Type: application/http
                Content-Transfer-Encoding: binary
                
                {mode} {self._client.api_url}{self.entity_name}{"("+row_key+")" if mode=="PATCH" else""} HTTP/1.1
                Content-Type: application/json{'; type=entry' if mode=="POST" else""}

                {json.dumps(row)}
                """
                batch_data += dedent(row_command)

            # To comply with payload structure,
            # the final newline requires one space
            batch_data += f"\n\n--{batch_id}--\n "

            batch_headers = {
                "Content-Type": f'multipart/mixed; boundary="{batch_id}"',
                "If-None-Match": "null",
            }

            log.info(
                f"Sending batch ID {batch_id} containing {len(data_chunk)} rows for upsert into {self.entity_name}."
            )

            try:
                response = self._client._post(
                    url="$batch", additional_headers=batch_headers, data=batch_data
                )
                print(dump.dump_all(response).decode("utf-8"))
                response.raise_for_status()
                log.info(
                    f"Successfully upserted {len(data_chunk)} rows into {self.entity_name}."
                )
            except requests.RequestException as e:
                raise DataverseError(f"Failed to perform batch operation: {e}")

    def _validate_payload(
        self,
        data: Union[dict, List[dict], pd.DataFrame],
        mode: Literal["upsert", "create"],
    ) -> Optional[Set[str]]:
        """
        Can be used to validate write/update/upsert data payload
        against the parsed Entity schema.

        Returns a set of key column(s) to use if succesful.

        Raises DataverseError if:
          - Column names in payload are not found in schema
          - No key or alternate key can be formed from columns
        """
        if isinstance(data, dict):
            data_columns = data.keys()
            complete_columns = [k for k, v in data.items() if v is not None]

        elif isinstance(data, list):
            data_columns = set()
            for row in data:
                data_columns.update(row.keys())

            # Looping through dicts to get the intersection of keys
            complete_columns = self.schema.columns.copy()
            for row in data:
                contains_values = {k for k, v in row.items() if v is not None}
                complete_columns = complete_columns.intersection(contains_values)

        elif isinstance(data, pd.DataFrame):
            data_columns = data.columns
            complete_columns = data.columns[~data.isnull().any()]
        else:
            raise TypeError("Wrong type passed for data.")

        data_columns, complete_columns = set(data_columns), set(complete_columns)

        # Checking column names against schema
        if not data_columns.issubset(self.schema.columns):
            bad_columns = list(data_columns.difference(self.schema.columns))
            raise DataverseError(
                f"Payload columns not in schema: {' '.join(bad_columns)}"
            )

        if mode not in ["upsert"]:
            return None

        # Checking for available keys against schema
        if self.schema.key in complete_columns:
            return set(self.schema.key)
        elif self.schema.altkeys:
            # Checking if any valid altkeys can be formed from columns
            for altkey in sorted(self.schema.altkeys, key=len):
                if altkey.issubset(complete_columns):
                    return altkey
        else:
            raise DataverseError(
                "No columns in payload to form primary key or any alternate key."
            )
