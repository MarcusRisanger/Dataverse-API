from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Literal, Optional, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from msal_requests_auth.auth import ClientCredentialAuth, DeviceCodeAuth

from dataverse_api.utils import (
    DataverseBatchCommand,
    DataverseError,
    DataverseTableSchema,
    batch_command,
    batch_id_generator,
    chunk_data,
    convert_data,
    expand_headers,
    extract_batch_response_data,
    extract_key,
    find_invalid_columns,
    parse_entity_metadata,
)

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)


ENTITY_SET_PARAMS = ["EntitySetName", "PrimaryIdAttribute"]
ENTITY_KEY_PARAMS = ["KeyAttributes"]
ENTITY_ATTR_PARAMS = [
    "LogicalName",
    "SchemaName",
    "AttributeType",
    "IsValidForCreate",
    "IsValidForUpdate",
    "IsValidODataAttribute",
]


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
      - scopes: The App registration scope names
    """

    def __init__(
        self,
        resource: str,
        auth: Union[ClientCredentialAuth, DeviceCodeAuth],
    ):
        self.api_url = urljoin(resource, "/api/data/v9.2/")
        self._auth = auth
        self._entity_cache: dict[str, DataverseEntity] = {}
        self._default_headers = {
            "Accept": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Content-Type": "application/json",
        }

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
        if (entity_set_name is not None) and (logical_name is not None):
            log.warning("Using entity set name. Entity will not be validated.")
            logical_name = None

        name = entity_set_name or logical_name

        if name is None:
            raise DataverseError("Please provide valid input.")

        if name not in self._entity_cache:
            self._entity_cache[name] = DataverseEntity(
                client=self,
                logical_name=logical_name,
                entity_set_name=entity_set_name,
            )

        return self._entity_cache[name]

    def get(
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

    def post(
        self,
        url: str,
        additional_headers: Optional[dict] = None,
        data: Optional[str] = None,
        json: Optional[dict] = None,
    ) -> requests.Response:
        """
        POST is used to write new data or send a batch request to Dataverse.

        Args:
          - url: Appended to API endpoint
          - additional_headers: Headers to overwrite default headers
          - data: Request payload (str, bytes etc.)
          - json: Request JSON serializable payload
        """
        headers = expand_headers(self._default_headers, additional_headers)
        url = urljoin(self.api_url, url)

        try:
            response = requests.post(
                url=url,
                auth=self._auth,
                headers=headers,
                data=data,
                json=json,
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            raise DataverseError(f"Error with POST request: {e}", response=e.response)

    def put(
        self, entity_name: str, key: str, column: str, value: Any
    ) -> requests.Response:
        """
        PUT is used to update a single column value for a single record.

        Args:
          - entity_name: Table where record exists
          - key: Either primary key or alternate key of record, appropriately formatted
          - column: The column to access for changing value
          - value: The value to be persisted to Dataverse in column
        """
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

    def patch(
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

    def delete(
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

    def batch_operation(
        self,
        data: list[DataverseBatchCommand],
        batch_id_generator: Callable[..., str] = batch_id_generator,
    ) -> requests.Response:
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
            batch_id = f"batch_{batch_id_generator()}"

            # Preparing batch data
            batch_data = ""
            for row in chunk:
                batch_data += batch_command(
                    batch_id=batch_id, api_url=self.api_url, row=row
                )

            # Note: Trailing space in final line is crucial
            # Request fails to meet specification without it
            batch_data += f"\n\n--{batch_id}--\n "

            # Preparing batch-specific headers
            additional_headers = {
                "Content-Type": f'multipart/mixed; boundary="{batch_id}"',
                "If-None-Match": "null",
            }

            log.debug(
                f"Sending batch ID {batch_id} containing {len(chunk)} "
                + "commands for processing in Dataverse."
            )

            response = self.post(
                url="$batch", additional_headers=additional_headers, data=batch_data
            )
            log.debug(f"Successfully completed {len(chunk)} batch command chunk.")

        log.debug("Successfully completed all batch commands.")
        return response


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
            metadata = self._get_entity_metadata(
                logical_name=logical_name,
                entity_params=ENTITY_SET_PARAMS,
                attr_params=ENTITY_ATTR_PARAMS,
                key_params=ENTITY_KEY_PARAMS,
            )
            self.schema = parse_entity_metadata(metadata)

        elif entity_set_name is not None:
            self._validate = False
            self.schema = DataverseTableSchema(name=entity_set_name)

    def _get_entity_metadata(
        self,
        logical_name: str,
        entity_params: list[str],
        attr_params: list[str],
        key_params: list[str],
    ) -> list[dict]:
        """
        Required for initialization using logical name of Dataverse Entity.

        Returns:
          - `DataverseEntitySet` containing attrs `entity_set_name` and `key`
        """
        url = f"EntityDefinitions(LogicalName='{logical_name}')"
        data = [
            DataverseBatchCommand(f"{url}?$select={','.join(entity_params)}", "GET"),
            DataverseBatchCommand(
                f"{url}/Attributes?$select={','.join(attr_params)}", "GET"
            ),
            DataverseBatchCommand(f"{url}/Keys?$select={','.join(key_params)}", "GET"),
        ]

        response = self._client.batch_operation(data)
        return extract_batch_response_data(response.text)

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
        key_columns = key_columns or self._validate_payload(data, mode="update")

        if key_columns is None and not self._validate:
            raise DataverseError("Key column(s) must be specified.")

        data, row_key = extract_key(data=data, key_columns=key_columns)

        if len(data) > 1:
            raise DataverseError("Can only update a single column using this function.")

        # Extracting single key/value pair from dict
        column, value = list(data.items())[0]

        response = self._client.put(
            entity_name=self.schema.name, key=row_key, column=column, value=value
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
        key_columns = key_columns or self._validate_payload(data, mode="update")

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

        if self._client.batch_operation(batch_data):
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
        self._validate_payload(data, mode="create")

        log.info(f"Performing insert of {len(data)} elements into {self.schema.name}.")

        # Converting to Batch Commands
        batch_data = self._prepare_batch_data(data=data, mode="POST")

        self._client.batch_operation(batch_data)
        log.info(f"Successfully inserted {len(batch_data)} rows to {self.schema.name}.")

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
        key_columns = key_columns or self._validate_payload(data, mode="upsert")

        log.info(f"Performing upsert of {len(data)} elements into {self.schema.name}.")

        if key_columns is None and not self._validate:
            raise DataverseError("Key column(s) must be specified.")

        batch_data = self._prepare_batch_data(
            data=data,
            mode="PATCH",
            key_columns=key_columns,
        )

        self._client.batch_operation(batch_data)
        log.info(f"Successfully upserted {len(batch_data)} rows to {self.schema.name}.")

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
                output.append(DataverseBatchCommand(uri=uri, mode=mode, data=row_data))
            else:
                uri = f"{self.schema.name}"
                output.append(DataverseBatchCommand(uri=uri, mode=mode, data=row))

        return output

    def _validate_payload(
        self,
        data: list[dict],
        mode: Optional[Literal["insert", "update", "upsert"]] = None,
    ) -> Optional[set[str]]:
        """
        Used to validate write/update/upsert data payload
        against the parsed Entity schema. If validation is set to False,
        it will not return a key or alter the supplied data.

        Returns a set of key column(s) to use if succesful.

        Args:
          - data: The data that will be validated according to Schema.
          - mode: The optional validation mode parameter that must be set
            to `"create"` if writing new data to Dataverse and `"update"`
            if performing updates to Dataverse.

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

        # Checking column names against schema
        if not data_columns.issubset(self.schema.columns):
            bad_columns = list(data_columns.difference(self.schema.columns))
            raise DataverseError(
                (
                    "Found bad payload columns not present "
                    + f"in table schema: {' '.join(bad_columns)}"
                )
            )

        if mode is None:
            log.info(
                "Data validation completed - all columns valid according to schema."
            )
            return None

        # Checking for available keys against schema
        if self.schema.key in complete_columns:
            log.info("Key column present in all rows, using as key.")
            key = {self.schema.key}
        elif self.schema.altkeys:
            # Checking if any valid altkeys can be formed from columns
            # Preferring shortest possible altkeys
            for altkey in sorted(self.schema.altkeys, key=len):
                if altkey.issubset(complete_columns):
                    log.info(
                        ("A consistent alternate key can be formed from all rows.")
                    )
                    key = altkey
        else:
            raise DataverseError(
                "No columns in payload to form consistent primary or alternate key."
            )

        find_invalid_columns(
            key_columns=key,
            data_columns=data_columns,
            schema_columns=self.schema.columns,
            mode=mode,
        )

        return key

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

        return self._client.get(
            url=self.schema.name,
            additional_headers=additional_headers,
            params=params,
        )
