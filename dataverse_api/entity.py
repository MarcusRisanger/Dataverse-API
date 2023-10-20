"""
Contains the DataverseEntity class, the main interface
to use with the Dataverse Web API.

Author: Marcus Risanger
"""

from __future__ import annotations

import logging
from typing import Any, Literal, Optional, Union

import pandas as pd

from dataverse_api._api import DataverseAPI
from dataverse_api._metadata_defs import EntityKeyMetadata, Label, ManagedProperty
from dataverse_api.dataclasses import (
    DataverseAuth,
    DataverseBatchCommand,
    DataverseEntitySchema,
    DataverseExpand,
    DataverseFile,
    DataverseOrderby,
)
from dataverse_api.errors import DataverseError
from dataverse_api.schema import DataverseSchema
from dataverse_api.utils import (
    convert_data,
    extract_key,
    find_invalid_columns,
    parse_expand,
    parse_orderby,
)

log = logging.getLogger("dataverse-api")


class DataverseEntity(DataverseAPI):
    """
    Class that controls interaction with a specific Dataverse Entity.

    Is instantiated with the Entity logical name, with an optional
    argument to apply data validation.

    >>> table = client.entity(logical_name="tablename", validate=True)  # Validates

    or

    >>> table = client.entity(logical_name="tablename")  # No validation
    """

    def __init__(
        self,
        logical_name: str,
        auth: DataverseAuth,
        validate: bool = False,
    ) -> None:
        super().__init__(auth=auth)
        self._validate = validate
        self.logical_name = logical_name
        self.schema: DataverseEntitySchema = DataverseSchema(
            auth=auth, logical_name=logical_name, validate=validate
        ).fetch()

    def update_single_value(
        self, data: dict[str, Any], key_columns: Optional[Union[str, set[str]]] = None
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
        key_columns = key_columns or self._validate_payload([data], mode="update")

        if key_columns is None and not self._validate:
            raise DataverseError("Key column(s) must be specified.")

        data, row_key = extract_key(data=data, key_columns=key_columns)

        if len(data) > 1:
            raise DataverseError("Can only update a single column using this function.")

        # Extracting single key/value pair from dict
        column, value = list(data.items())[0]

        response = self._put(
            url=f"{self.schema.entity.name}({row_key})/{column}",
            json={"value": value},
        )
        if response:
            log.info(f"Successfully updated {row_key} in {self.schema.entity.name}.")

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
            column in Dataverse for update, across all rows.

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

        liberal: bool = kwargs.get("liberal", False)

        if key_columns is None and not self._validate:
            raise DataverseError("Key column(s) must be specified.")

        if not all(len(row) == 1 for row in data) and not liberal:
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

        if self._batch_operation(batch_data):
            log.info(
                f"Successfully updated {len(batch_data)} rows"
                + f"in {self.schema.entity.name}."
            )

    def insert_row(
        self,
        data: Optional[str] = None,
        json: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Inserts one row of data into the selected Entity.

        Args:
          - data: Data string for entry.
          - json: JSON serializable object for entry.
        """
        if data is None and json is None:
            raise DataverseError("Needs either JSON or data payload!")

        url = self.schema.entity.name
        self._post(url=url, json=json, data=data)

    def insert(
        self,
        data: Union[dict, list[dict], pd.DataFrame],
    ) -> None:
        """
        Inserts data into the selected Entity.

        Args:
          - data: Data that forms the basis for insert into Dataverse.

        >>> data={"col1":"abc", "col2":"dac", "col3":69, "col4":"Foo"}
        >>> table.upsert(data)
        """
        data = convert_data(data)
        # Validation just run to make sure appropriate keys are present
        self._validate_payload(data, mode="create")

        log.debug(
            f"Performing insert of {len(data)} elements into {self.schema.entity.name}."
        )

        # Converting to Batch Commands
        batch_data = self._prepare_batch_data(data=data, mode="POST")

        self._batch_operation(batch_data)
        log.debug(
            f"Successfully inserted {len(batch_data)} rows to {self.schema.entity.name}."
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
        key_columns = key_columns or self._validate_payload(data, mode="upsert")

        log.debug(
            f"Performing upsert of {len(data)} elements into {self.schema.entity.name}."
        )

        if key_columns is None and not self._validate:
            raise DataverseError("Key column(s) must be specified.")

        batch_data = self._prepare_batch_data(
            data=data,
            mode="PATCH",
            key_columns=key_columns,
        )

        self._batch_operation(batch_data)
        log.debug(
            f"Successfully upserted {len(batch_data)} rows to {self.schema.entity.name}."
        )

    def _prepare_batch_data(
        self,
        data: list[dict],
        mode: Literal["PATCH", "POST", "PUT", "DELETE"],
        key_columns: Optional[Union[str, set[str]]] = None,
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
                uri = f"{self.schema.entity.name}({row_key})"
                output.append(DataverseBatchCommand(uri=uri, mode=mode, data=row_data))
            else:
                uri = f"{self.schema.entity.name}"
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

        # Getting a set of all columns supplied in data,
        # and a set of columns that is present in every row of data
        supplied_columns = set()
        complete_columns = {k for k in self.schema.attributes.keys()}

        for row in data:
            # Updating set of supplied columns
            supplied_columns.update(row.keys())

            # Updating set of columns present in ALL rows
            row_keys = {k for k, v in row.items() if v is not None}
            complete_columns = complete_columns.intersection(row_keys)

        # Checking column names against schema
        if not supplied_columns.issubset(self.schema.attributes):
            bad_columns = list(supplied_columns.difference(self.schema.attributes))
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
        if self.schema.entity.primary_attr in complete_columns:
            log.debug("Key column present in all rows, using as key.")
            key = {self.schema.entity.primary_attr}
        elif self.schema.altkeys:
            # Checking if any valid altkeys can be formed from columns
            # Preferring shortest possible altkeys
            for altkey in sorted(self.schema.altkeys, key=len):
                if altkey.issubset(complete_columns):
                    log.debug(
                        ("A consistent alternate key can be formed from all rows.")
                    )
                    key = altkey
        else:
            raise DataverseError(
                "No columns in payload to form consistent primary or alternate key."
            )

        find_invalid_columns(
            key_columns=key,
            data_columns=supplied_columns,
            schema_columns=self.schema.attributes,
            mode=mode,
        )

        return key

    def read(
        self,
        select: Optional[Union[str, list[str]]] = None,
        filter: Optional[str] = None,
        expand: Optional[Union[str, list[DataverseExpand]]] = None,
        orderby: Optional[Union[str, list[DataverseOrderby]]] = None,
        top: Optional[int] = None,
        apply: Optional[str] = None,
        page_size: Optional[int] = None,
    ):
        """
        Reads entity.

        Optional querying args:
          - select: A single column or list of columns to return from the
            current entity.
          - filter: A fully qualified filtering string.
          - expand: A fully qualified expand string or a list of
            `DataverseExpand` objects.
          - orderby: A fully qualified order_by string or a list of
            `DataverseOrderby` objects.
          - top: Optional limit on returned records.
          - apply: A fully qualified string describing aggregation
            and grouping of returned records.
          - page_size: Optional parameter limiting number of records
            retrieved per API call.

        """

        additional_headers = dict()
        if page_size is not None:
            additional_headers["Prefer"] = f"odata.maxpagesize={page_size}"

        params = dict()
        if select is not None:
            params["$select"] = ",".join(select)
        if filter is not None:
            params["$filter"] = filter
        if expand is not None:
            params["$expand"] = parse_expand(expand)
        if orderby is not None:
            params["$orderby"] = parse_orderby(orderby)
        if top is not None:
            params["$top"] = top
        if apply is not None:
            params["$apply"] = apply

        output = []
        url = self.schema.entity.name

        # Looping through pages
        while url:
            response: dict = self._get(
                url=url, additional_headers=additional_headers, params=params
            ).json()
            output.extend(response["value"])
            url = response.get("@odata.nextLink")

        return output

    def upload_file(
        self,
        file_name: str,
        file_content: bytes,
        file_column: str,
        row: dict[str, Any],
        key_columns: Optional[Union[str, set[str]]] = None,
    ) -> None:
        """
        Uploads image to the Dataverse entity.

        Args:
          - file_name: Name of image name and byte payload
          - file_content: Image payload, bytes
          - file_column: Optional override if image is to be uploaded to
            a non-primary file column
          - row: Dict containing row key information
          - key_columns: Optional set of key columns found in data
        """
        file = DataverseFile(file_name=file_name, payload=file_content)

        key_columns = key_columns or self._validate_payload([row], mode="insert")
        _, row_key = extract_key(data=row, key_columns=key_columns)

        if len(file.payload) > 134217728:
            log.debug("File too large for single API request. Chunking.")
            self._upload_large_file(file=file, row_key=row_key, file_column=file_column)

        url = f"{self.schema.entity.name}({row_key})/{file_column}"
        additional_headers = {
            "Content-Type": "application/octet-stream",
            "x-ms-file-name": file.file_name,
            "Content-Length": str(len(file.payload)),
        }

        self._patch(url=url, additional_headers=additional_headers, data=file.payload)

        # If file is less than 128 MB, upload in single chunk

        """
        PATCH [Organization Uri]/api/data/v9.2/accounts
          (<accountid>)/sample_filecolumn HTTP/1.1
        OData-MaxVersion: 4.0
        OData-Version: 4.0
        If-None-Match: null
        Accept: application/json
        Content-Type: application/octet-stream
        x-ms-file-name: 4094kb.txt
        Content-Length: 4191273

        < binary content removed for brevity>
        """

        # Else break up in chunks

    def upload_image(
        self,
        file_name: str,
        file_content: bytes,
        image_column: str,
        row: dict[str, Any],
        key_columns: Optional[Union[str, set[str]]] = None,
    ) -> None:
        """
        Uploads image to the Dataverse entity.

        Args:
          - file_name: Name of image name and byte payload
          - file_content: Image payload, bytes
          - image_column: Target image column
          - row: Dict containing row key information
          - key_columns: Optional set of key columns found in data
        """
        image = DataverseFile(file_name=file_name, payload=file_content)

        self.upload_file(
            file_name=image.file_name,
            file_content=image.payload,
            row=row,
            key_columns=key_columns,
            file_column=image_column,
        )

    def _upload_large_file(
        self,
        file: DataverseFile,
        row_key: str,
        file_column: str,
    ):
        raise NotImplementedError("Sorry!")

        # Needs to implement chunking

        url = f"{self.schema.entity.name}({row_key})/{file_column}"
        additional_headers = {
            "x-ms-transfer-mode": "chunked",
            "x-ms-file-name": file.file_name,
        }

        while url:
            self._patch(url, additional_headers=additional_headers)

    def add_alternate_key(
        self,
        schema_name: str,
        key_attributes: list[str],
        display_name: Label,
        is_customizable: Optional[ManagedProperty] = None,
    ) -> None:
        """
        Method for adding an alternate key to the Entity.

        Args:
          - schema_name: Schema name for key. Will also be Logical name (lowercased).
          - key_attributes: List of key attributes that comprise the alternate key.
          - display_name: The display name of the alternate key.
        """

        if is_customizable is None:
            is_customizable = ManagedProperty(
                value=True, can_be_changed=True, managed_property_name="iscustomizable"
            )

        meta = EntityKeyMetadata(
            display_name=display_name,
            schema_name=schema_name,
            key_attributes=key_attributes,
        )

        self._post(
            url=(f"EntityDefinitions(LogicalName='{self.logical_name}')" + "/Keys"),
            json=meta(),
        )

    def remove_alternate_key(self, logical_name: str) -> None:
        """
        Method for removing a given alternate key.

        Args:
          - logical_name: Logical name of key to be deleted for Entity.
        """
        self._delete(
            url=(
                f"EntityDefinitions(LogicalName='{self.logical_name}')"
                + f"/Keys(LogicalName='{logical_name}')"
            )
        )
