from __future__ import annotations

import json
import logging
from collections.abc import Callable
from textwrap import dedent
from typing import Any, Literal, Optional, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from msal import ConfidentialClientApplication
from msal_requests_auth.auth import ClientCredentialAuth

# from requests_toolbelt.utils import dump
# print(dump.dump_all(response).decode("utf-8"))
from dataverse_api.utils import (
    DataverseBatchCommand,
    DataverseEntitySet,
    DataverseError,
    DataverseTableSchema,
    batch_id_generator,
    chunk_data,
    convert_data,
    expand_headers,
    extract_key,
    parse_meta_columns,
    parse_meta_keys,
)

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)


class DataverseClient:
    """
    Base class used to establish authorization and certain default
    parameters for connecting to Dataverse environment, along with
    base methods for interacting with the Web API.

    Args:
      - app_id: Azure App Registration ID
      - client_secret: Secret for App registration
      - authority_url: Authority URL for App registration
      - dynamics_url: Base environment url
      - scopes: The App registration scope
    """

    def __init__(
        self,
        app_id: str,
        client_secret: str,
        authority_url: str,
        dynamics_url: str,
        scopes: list[str],
    ):
        self.api_url = urljoin(dynamics_url, "/api/data/v9.2/")
        self._auth = self._authenticate(
            app_id=app_id,
            client_secret=client_secret,
            authority_url=authority_url,
            scopes=scopes,
        )
        self._entity_cache: dict[str, DataverseEntity] = {}
        self._default_headers = {
            "Accept": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Content-Type": "application/json",
        }

    def _authenticate(
        self,
        app_id: str,
        client_secret: str,
        authority_url: str,
        scopes: list[str],
    ) -> ClientCredentialAuth:
        app = ConfidentialClientApplication(
            client_id=app_id, authority=authority_url, client_credential=client_secret
        )
        return ClientCredentialAuth(client=app, scopes=scopes)

    def entity(
        self,
        logical_name: Optional[str] = None,
        entity_set_name: Optional[str] = None,
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
        if (logical_name is None) ^ (entity_set_name is None):  # XOR
            name = logical_name or entity_set_name

            if name not in self._entity_cache:
                self._entity_cache[name] = DataverseEntity(
                    client=self,
                    logical_name=logical_name,
                    entity_set_name=entity_set_name,
                )

            return self._entity_cache[name]

    def _get(
        self, url: str, additional_headers: Optional[dict] = None, **kwargs
    ) -> requests.Response:
        """
        GET is used to retrieve data from Dataverse.

        Args:
          - url: Appended to API endpoint
          - additional_headers: Headers to overwrite default headers
          - data: Request payload (str, bytes etc.)
          - json: Request JSON serializable payload
          - params: Relevant request parameters
        """
        headers = expand_headers(self._default_headers, additional_headers)
        url = urljoin(self.api_url, url)

        try:
            response = requests.get(
                url=url,
                auth=self._auth,
                headers=headers,
                data=kwargs.get("data"),
                json=kwargs.get("json"),
                params=kwargs.get("params"),
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error with GET request: {e}", response=e.response)

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

        try:
            response = requests.post(
                url=url,
                auth=self._auth,
                headers=headers,
                data=kwargs.get("data"),
                json=kwargs.get("json"),
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error with POST request: {e}", response=e.response)

    def _put(
        self, entity_name: str, key: str, data: dict[str, Any]
    ) -> requests.Response:
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
            return response
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error with PUT request: {e}", response=e.response)

    def _patch(
        self,
        url: str,
        data: dict[str, Any],
        additional_headers: Optional[dict] = None,
    ) -> requests.Response:
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
            return response
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error with PATCH request: {e}", response=e.response)

    def _delete(
        self, url: str, additional_headers: Optional[dict] = None
    ) -> requests.Response:
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
            return response
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error with DELETE request: {e}", response=e.response)

    def _batch_operation(
        self,
        data: list[DataverseBatchCommand],
        batch_id_generator: Callable[..., str] = batch_id_generator,
    ):
        """
        Generalized function to run batch commands against Dataverse.

        Data containing either a list of DataverseBatchCommands containing
        the relevant data for submission, where each dict or table row
        contains necessary information for one single batch command.

        DataverseBatchCommands have the following attributes:
          - uri: The postfix after API endpoint to form the full command URI.
          - mode: The mode used by the singular batch command.
          - data: The data to be transmitted related to the the command.

        Args:
          - data: A list of `DataverseBatchCommand` to be executed
          - batch_id_generator: An optional function call for overriding
            default unique batch ID generation.
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

    Can be instantiated with the EntitySetName (if known) of the Entity
    to run without validation, or using the LogicalName of the
    Entity to apply validation.

    >>> table = client.entity(logical_name="tablename")  # Validates

    or

    >>> table = client.entity(entity_set_name="tablename")  # No validation
    """

    def __init__(
        self,
        client: DataverseClient,
        logical_name: Optional[str] = None,
        entity_set_name: Optional[str] = None,
    ):
        self._client = client
        self._entity_set_name = entity_set_name

        if logical_name is not None:
            self._validate = True
            entity_set_data = self._fetch_entity_data(logical_name=logical_name)
            self.schema = DataverseTableSchema(
                name=entity_set_data.entity_set_name,
                key=entity_set_data.entity_set_key,
            )

            col_response = self._fetch_entity_columns(logical_name)
            key_response = self._fetch_entity_altkeys(logical_name)
            self.schema.columns = parse_meta_columns(col_response)
            self.schema.altkeys = parse_meta_keys(key_response)

        elif entity_set_name is not None:
            self._validate = False
            self.schema = DataverseTableSchema(name=entity_set_name)

    def _fetch_entity_data(self, logical_name) -> DataverseEntitySet:
        """
        Required for initialization using logical name of Dataverse Entity.

        Returns:
          - `DataverseEntitySet` containing attrs `entity_set_name` and `key`
        """
        url = f"EntityDefinitions(LogicalName='{logical_name}')"
        parameters = ["EntitySetName", "PrimaryIdAttribute"]
        params = {"$select": ",".join(parameters)}

        response = self._client._get(url=url, params=params).json()

        entity_set = DataverseEntitySet(
            entity_set_name=response["EntitySetName"],
            entity_set_key=response["PrimaryIdAttribute"],
        )

        return entity_set

    def _fetch_entity_columns(self, logical_name) -> requests.Response:
        """
        Required for initialization using logical name of Dataverse Entity.
        """
        url = f"EntityDefinitions(LogicalName='{logical_name}')/Attributes"
        parameters = [
            "LogicalName",
            "SchemaName",
            "AttributeType",
            "IsValidForCreate",
            "IsValidForUpdate",
        ]
        params = {
            "$select": ",".join(parameters),
            "$filter": "IsValidODataAttribute eq true",
        }

        response = self._client._get(url=url, params=params)

        return response

    def _fetch_entity_altkeys(self, logical_name) -> requests.Response:
        """
        Required for initialization using logical name of Dataverse Entity.
        """
        url = f"EntityDefinitions(LogicalName='{logical_name}')/Keys"
        parameters = ["KeyAttributes"]
        params = {"$select": ",".join(parameters)}

        response = self._client._get(url=url, params=params)

        return response

    def update_single_value(
        self, data: dict[str, Any], key_columns: Optional[set[str]] = None
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
        key_columns = key_columns or self._validate_payload(data, write_mode=True)

        if key_columns is None and not self._validate:
            raise DataverseError("Key column(s) must be specified.")

        data, row_key = extract_key(data=data, key_columns=key_columns)

        if len(data) > 1:
            raise DataverseError("Can only update a single column using this function.")

        response = self._client._put(
            entity_name=self.schema.name, key=row_key, data=data
        )
        if response:
            log.info(f"Successfully updated {row_key} in {self.schema.name}.")

    def update_single_column(
        self,
        data: Union[dict, list[dict], pd.DataFrame],
        key_columns: Optional[Union[str, set[str]]] = None,
        **kwargs,
    ) -> None:
        """
        Updates the values of a single column for multiple rows in Entity.

        Args:
          - data: Data that forms the basis for update into Dataverse.
          - key_columns: If validation is not enabled, provide primary column or
            columns that form an alternate key, to identify unique row.

        kwargs:
          - liberal: If set to True, this will allow for different columns to be
            updated on a per-row basis, but still just one column per row.
            Default behavior is that each update points to one singular
            column in Dataverse, across all rows.

        Raises:
          - DataverseError if no key column is supplied, or none can be found
            in validation step.
          - DataverseError if more than one data column is passed per row.

        >>> data=[{"col1":"foo", "col2":2}, {"col1":"bar", "col2":3}]
        >>> table.upsert_single_column(data, key_columns="col1")

        Alternatively, updating different columns per data row:

        >>> data=[{"col1":"foo", "col2":2}, {"col1":"bar", "col3":5}]
        >>> table.upsert_single_column(data, key_columns="col1", liberal=True)
        """
        data = convert_data(data)
        key_columns = key_columns or self._validate_payload(data, write_mode=True)

        if key_columns is None and not self._validate:
            raise DataverseError("Key column(s) must be specified.")

        if not all(len(row) == 1 for row in data):
            raise DataverseError(
                "Only one data column can be passed. Use `upsert` instead."
            )

        if kwargs.get("liberal", False) is False:
            key = list(data[0].keys())[0]
            for row in data[1:]:
                if list(row.keys())[0] != key:
                    raise DataverseError(
                        "Only one data column may be passed. Use `liberal=True` instead."
                    )

        batch_data = self._prepare_batch_data(
            data=data,
            mode="PUT",
            key_columns=key_columns,
        )

        if self._client._batch_operation(batch_data):
            log.info(
                f"Successfully updated {len(batch_data)} rows in {self.schema.name}."
            )

    def insert(
        self,
        data: Union[dict, list[dict], pd.DataFrame],
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
        # Validation just run to make sure appropriate keys are present
        self._validate_payload(data, write_mode=True)

        log.info(f"Performing insert of {len(data)} elements into {self.schema.name}.")

        # Converting to Batch Commands
        batch_data = self._prepare_batch_data(data=data, mode="POST")

        if self._client._batch_operation(batch_data):
            log.info(
                f"Successfully inserted {len(batch_data)} rows to {self.schema.name}."
            )

    def upsert(
        self,
        data: Union[dict, list[dict], pd.DataFrame],
        key_columns: Optional[Union[str, set[str]]] = None,
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
        key_columns = key_columns or self._validate_payload(data, write_mode=True)

        log.info(f"Performing upsert of {len(data)} elements into {self.schema.name}.")

        if key_columns is None and not self._validate:
            raise DataverseError("Key column(s) must be specified.")

        batch_data = self._prepare_batch_data(
            data=data,
            mode="PATCH",
            key_columns=key_columns,
        )

        if self._client._batch_operation(batch_data):
            log.info(
                f"Successfully upserted {len(batch_data)} rows to {self.schema.name}."
            )

    def _prepare_batch_data(
        self,
        data: list[dict],
        mode: Literal["PATCH", "POST", "PUT", "DELETE"],
        key_columns: Optional[set[str]] = None,
    ) -> list[DataverseBatchCommand]:
        """
        Transforms submitted data to Batch Operations commands.

        For modes where row ID matters (PATCH, PUT, DELETE), key_column
        is required.

        Args:
          - data: The data to be prepared into batch commands
          - mode: The request mode to be carried out for each command
          - key_columns: Optional set of key columns

        Returns:
          - List of `DataverseBatchCommand` objects for passing into the
            `DataverseClient.batch_operation` method.

        Raises:
          - DataverseError if mode is not `POST` and no `key_column` is specified.
        """
        output: list[DataverseBatchCommand] = []

        if mode != "POST" and key_columns is None:
            raise DataverseError("Mode requires key column to be passed as argument.")

        for row in data:
            if mode in ["PATCH", "PUT", "DELETE"]:
                row_data, row_key = extract_key(data=row, key_columns=key_columns)
                uri = f"{self.schema.name}({row_key})"
            else:
                uri = f"{self.schema.name}"
            output.append(DataverseBatchCommand(uri=uri, mode=mode, data=row_data))

        return output

    def _validate_payload(
        self,
        data: list[dict],
        write_mode: Optional[bool] = False,
    ) -> Optional[set[str]]:
        """
        Used to validate write/update/upsert data payload
        against the parsed Entity schema. If validation is set to False,
        it will not return a key or alter the supplied data.

        Returns a set of key column(s) to use if succesful.

        Raises DataverseError if:
          - Column names in payload are not found in schema
          - No key or alternate key can be formed from columns (if write_mode = True)
        """
        if not self._validate:
            log.info("Data validation not performed.")
            return None

        # Getting a set of all columns supplied in data
        data_columns = set()
        for row in data:
            data_columns.update(row.keys())

        # Getting a set of all columns present in all rows of data
        complete_columns = {k for k in self.schema.columns.keys()}
        for row in data:
            contains_values = {k for k, v in row.items() if v is not None}
            complete_columns = complete_columns.intersection(contains_values)

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
            # Preferring shortest possible altkeys
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

    def read(self, select: Optional[list[str]] = None, page_size: Optional[int] = None):
        """
        Reads entity.

        TODO: Support advanced query params: select, expand, filter, orderby, top
        TODO: Support paging if server responds with
        """

        additional_headers = dict()
        if page_size is not None:
            additional_headers["Prefer"] = "odata.maxpagesize=%s" % page_size

        params = dict()
        if select is not None:
            params["$select"] = ",".join(select)

        return self._client._get(
            url=self.schema.name,
            additional_headers=additional_headers,
            params=params,
        )
