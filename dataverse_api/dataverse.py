from __future__ import annotations

import json
import logging
import uuid
from textwrap import dedent
from typing import Any, Dict, List, Optional, Set, Union, Callable
from urllib.parse import urljoin

import pandas as pd
import requests
from msal import ConfidentialClientApplication
from msal_requests_auth.auth import ClientCredentialAuth

# from requests_toolbelt.utils import dump
# print(dump.dump_all(response).decode("utf-8"))
from dataverse_api.schema import DataverseSchema
from dataverse_api.utils import (
    DataverseBatchCommand,
    DataverseError,
    chunk_data,
    convert_data,
    expand_headers,
    extract_key,
    batch_id_generator
)

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)


class DataverseClient:
    """
    Base class used to establish authorization and certain default parameters
    for connecting to Dataverse environment.

    Args:
      - dynamics_url: Base environment url
      - authorization: `ClientCredentialAuth` from `msal_requests_auth`
      providing the necessary app registration to interact with Dataverse
      - validate: Whether to retrieve Dataverse schema and apply validation rules
    """

    def __init__(
        self,
        app_id: str,
        client_secret: str,
        authority_url: str,
        dynamics_url: str,
        scopes: list[str],
        validate: bool = False,
    ):
        self.api_url = urljoin(dynamics_url, "/api/data/v9.2/")

        app = ConfidentialClientApplication(
            client_id=app_id, authority=authority_url, client_credential=client_secret
        )

        self._auth = ClientCredentialAuth(client=app, scopes=scopes)
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
        self, url: str, additional_headers: Optional[dict] = None, **kwargs
    ) -> requests.Response:
        """
        POST is used to write new data or send a batch request to Dataverse.

        Args:
          - url: Appended to API endpoint
          - data: Request payload (str, bytes etc.)
          - json: Request JSON serializable payload
          - headers: Headers to overwrite default headers
        """
        headers = expand_headers(self._default_headers, additional_headers)
        url = urljoin(self.api_url, url)

        return requests.post(
            url=url,
            auth=self._auth,
            headers=headers,
            data=kwargs.get("data"),
            json=kwargs.get("json"),
        )

    def _put(self, entity_name: str, key: str, data: Dict[str, Any]) -> bool:
        """
        PUT is used to update a single column value for a single record.

        Args:
          - entity_name: Table where record exists
          - key: Either primary key or alternate key of record, appropriately formatted
          - column:
        """
        column, value = list(data.items())[0]

        if self._validate and column not in self.schema.entities[entity_name].columns:
            raise DataverseError(f"Column {column} not found in {entity_name} schema.")

        url = f"{urljoin(self.api_url,entity_name)}({key})/{column}"

        try:
            response = requests.put(
                url=url,
                auth=self._auth,
                headers=self._default_headers,
                json={"value": value},
            )
            response.raise_for_status()
            return response.status_code == 204
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error with PUT request: {e}", response=e.response)

    def _patch(
        self,
        url: str,
        data: Dict[str, Any],
        additional_headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        PATCH is used to update several values for a single record.

        Args:
          - url: Postfix of API endpoint to isolate unique record
          - additional_headers: If it is required to overwrite default
            or add new header elements
          - data: JSON serializable dictionary containing data payload.
        """
        headers = expand_headers(self._default_headers, additional_headers)
        url = urljoin(self.api_url, url)

        try:
            response = requests.patch(
                url=url,
                auth=self._auth,
                headers=headers,
                json=data,
            )
            response.raise_for_status()
            return response.status_code == 204
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error with PATCH request: {e}", response=e.response)

    def _delete(
        self, url: str, additional_headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        DELETE is used to either purge whole records or a specific
        column value for a particular record.

        Args:
          - url: Postfix of API endpoint to isolate unique record
            or record + column
          - additional_headers: If it is required to overwrite default
            or add new header elements
        """
        headers = expand_headers(self._default_headers, additional_headers)
        url = urljoin(self.api_url, url)

        try:
            response = requests.delete(
                url=url,
                auth=self._auth,
                headers=headers,
            )
            response.raise_for_status()
            return response.status_code == 204
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error with DELETE request: {e}", response=e.response)

    def batch_operation(self, data: List[DataverseBatchCommand], batch_id_generator: Callable[...,str] = batch_id_generator):
        """
        Generalized function to run batch commands against Dataverse.

        Data containing either a list of DataverseBatchCommands containing
        the relevant data for submission, where each dict or table row
        contains necessary information for one single batch command.

        DataverseBatchCommands contain:
          - uri: The postfix after API endpoint to form the full command URI.
          - mode: The mode used by the singular batch command.
          - data: The data to be transmitted related to the the command.

        Example data:

        [
            {
                "__uri__": "accounts(key_column1='abc',key_column2=3)",
                "__mode__": "PATCH",
                "account_name": "Dataverse",
                "account_number": 123
            },
            {
                "__uri__": "employees(key_column1=3,key_column2=4)/name" ,
                "__mode__": "PUT",
                "value": "Barack Obama"
            }
        ]
        """

        for chunk in chunk_data(data, size=1000):
            batch_id = "batch_%s" % batch_id_generator()

            # Preparing batch data
            batch_data = ""
            for row in chunk:
                row_command = f"""\
                --{batch_id}
                Content-Type: application/http
                Content-Transfer-Encoding: binary

                {row.mode} {self.api_url}{row.uri} HTTP/1.1
                Content-Type: application/json{'; type=entry' if row.mode=="POST" else""}

                {json.dumps(row.data)}
                """

                batch_data += dedent(row_command)
            batch_data += f"\n\n--{batch_id}--\n "

            # Preparing batch-specific headers
            additional_headers = {
                "Content-Type": f'multipart/mixed; boundary="{batch_id}"',
                "If-None-Match": "null",
            }

            log.info(
                f"Sending batch ID {batch_id} containing {len(chunk)} "
                + "rows for upsert into Dataverse."
            )

            try:
                response = self._post(
                    url="$batch", additional_headers=additional_headers, data=batch_data
                )
                response.raise_for_status()
                log.info(f"Successfully completed {len(chunk)} batch command chunk.")

            except requests.RequestException as e:
                if response.status_code == 412:
                    raise DataverseError(
                        (
                            "Failed to perform batch operation, most likely"
                            + f"  due to existing keys in insertion data: {e}"
                        ),
                        response=e.response,
                    )
                raise DataverseError(
                    f"Failed to perform batch operation: {e}", response=e.response
                )

        log.info("Successfully completed all batch commands.")
        return True


class DataverseEntity:
    """
    Class that controls interaction with a specific Dataverse Entity.

    Allows for simple table insertion or upsert of large amounts of data.

    >>> table = client.entity("tablename")
    """

    def __init__(self, client: DataverseClient, entity_name: str):
        self._client = client
        self.entity_name = entity_name
        if client._validate:
            try:
                self.schema = client.schema.entities[entity_name]
            except KeyError:
                raise DataverseError("Entity %s not found in schema." % entity_name)

    def update_single_value(
        self, data: Dict[str, Any], key_columns: Optional[Set[str]] = None
    ) -> None:
        """
        Updates singular column value for a specific row in Entity.

        Args:
          - data: Data that forms the basis for update into Dataverse.
          - key_columns: If validation is not enabled, provide primary column or
            columns that form an alternate key

        >>> data={"col1":"abc", "col2":"dac", "col3":69}
        >>> table.update_single_value(data, key_columns={"col1","col2"})
        """
        key_columns = self._validate_payload(data, write_mode=True)

        if key_columns is None and not self._client._validate:
            raise DataverseError("Key column(s) must be specified.")

        key = extract_key(data=data, key_columns=key_columns)

        if len(data) > 1:
            raise DataverseError("Can only update a single column using this function.")

        response = self._client._put(entity_name=self.entity_name, key=key, data=data)
        if response:
            log.info(f"Successfully updated {key} in {self.entity_name}.")

    # def update_single_column(
    #     self, data: Dict[str, Any], key_columns: Optional[Set[str]] = None
    # ) -> None:
    #     key_columns = self._validate_payload(data, write_mode=True)

    #     if key_columns is None and not self._client._validate:
    #         raise DataverseError("Key column(s) must be specified.")

    def insert(
        self,
        data: Union[dict, List[dict], pd.DataFrame],
    ) -> None:
        """
        Inserts data into the selected Entity.

        Args:
          - data: Data that forms the basis for insert into Dataverse.
          - key_columns: If validation is not enabled, provide primary column or
            columns that form an alternate key, to ensure data can be inserted.

        >>> data={"col1":"abc", "col2":"dac", "col3":69, "col4":"Foo"}
        >>> table.upsert(data, key_columns={"col1","col2"})
        """
        data = convert_data(data)
        mode = "POST"

        log.info(f"Performing insert of {len(data)} elements into {self.entity_name}.")

        self._validate_payload(data, write_mode=True)

        # Converting to Batch Commands
        batch_data = [
            DataverseBatchCommand(uri=self.entity_name, mode=mode, data=row)
            for row in data
        ]

        if self._client.batch_operation(batch_data):
            log.info(
                f"Successfully inserted {len(batch_data)} rows to {self.entity_name}."
            )

    def upsert(
        self,
        data: Union[dict, List[dict], pd.DataFrame],
        key_columns: Optional[Union[str, Set[str]]] = None,
    ) -> None:
        """
        Upserts data into the selected Entity.

        Args:
          - data: Data that forms the basis for upsert into Dataverse.
          - key_columns: If validation is not enabled, provide primary column or
            columns that form an alternate key for identifying rows uniquely.

        >>> data={"col1":"abc", "col2":"dac", "col3":69, "col4":"Foo"}
        >>> table.upsert(data, key_columns={"col1","col2"})
        """
        data = convert_data(data)
        mode = "PATCH"

        log.info(f"Performing upsert of {len(data)} elements into {self.entity_name}.")

        if self._client._validate:
            key_columns = self._validate_payload(data, write_mode=True)
            log.info("Data validation completed.")

        if key_columns is None and not self._client._validate:
            raise DataverseError("Key column(s) must be specified.")

        batch_data: List[DataverseBatchCommand] = []

        for row in data:
            # Splitting out key column(s) from data - should not be present in body
            key_elements = []
            for col in set(key_columns):
                key_elements.append(f"{col}={row.pop(col).__repr__()}")
            row_key = ",".join(key_elements)

            batch_data.append(
                DataverseBatchCommand(
                    uri=f"{self.entity_name}({row_key})", mode=mode, data=row
                )
            )

        if self._client.batch_operation(batch_data):
            log.info(
                f"Successfully upserted {len(batch_data)} rows to {self.entity_name}."
            )

    def _validate_payload(
        self,
        data: Union[dict, List[dict], pd.DataFrame],
        write_mode: Optional[bool] = False,
    ) -> Optional[Set[str]]:
        """
        Can be used to validate write/update/upsert data payload
        against the parsed Entity schema.

        Returns a set of key column(s) to use if succesful.

        Raises DataverseError if:
          - Column names in payload are not found in schema
          - No key or alternate key can be formed from columns (if write_mode = True)
        """
        if not self._client._validate:
            log.info("Data validation not performed.")
            return None

        if isinstance(data, dict):
            data_columns = set(data.keys())
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
                (
                    "Found bad payload columns not present "
                    + f"in table schema: {' '.join(bad_columns)}"
                )
            )

        if not write_mode:
            log.info(
                "Data validation completed - all columns valid according to schema."
            )
            return None

        # Checking for available keys against schema
        if self.schema.key in complete_columns:
            log.info("Data validation completed. Key column present in all rows.")
            return set(self.schema.key)
        elif self.schema.altkeys:
            # Checking if any valid altkeys can be formed from columns
            for altkey in sorted(self.schema.altkeys, key=len):
                if altkey.issubset(complete_columns):
                    log.info(
                        (
                            "Data validation completed. A consistent"
                            + " alternate key can be formed from all rows."
                        )
                    )
                    return altkey
        else:
            raise DataverseError(
                "No columns in payload to form primary key or any alternate key."
            )
