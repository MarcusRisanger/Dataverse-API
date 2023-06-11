"""
Contains the DataverseEntity class, the main interface
to use with the Dataverse Web API.

Author: Marcus Risanger
"""


import logging
from typing import Any, Literal, Optional, Union

import pandas as pd

from dataverse_api._api import DataverseAPI
from dataverse_api.dataclasses import (
    DataverseAuth,
    DataverseBatchCommand,
    DataverseEntitySchema,
    DataverseExpand,
    DataverseImageFile,
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
    ):
        super().__init__(auth=auth)
        self._validate = validate
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
            entity_name=self.schema.name, key=row_key, column=column, value=value
        )
        if response:
            logging.info(f"Successfully updated {row_key} in {self.schema.name}.")

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
            logging.info(
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

        logging.info(
            f"Performing insert of {len(data)} elements into {self.schema.name}."
        )

        # Converting to Batch Commands
        batch_data = self._prepare_batch_data(data=data, mode="POST")

        self._batch_operation(batch_data)
        logging.info(
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
        key_columns = key_columns or self._validate_payload(data, mode="upsert")

        logging.info(
            f"Performing upsert of {len(data)} elements into {self.schema.name}."
        )

        if key_columns is None and not self._validate:
            raise DataverseError("Key column(s) must be specified.")

        batch_data = self._prepare_batch_data(
            data=data,
            mode="PATCH",
            key_columns=key_columns,
        )

        self._batch_operation(batch_data)
        logging.info(
            f"Successfully upserted {len(batch_data)} rows to {self.schema.name}."
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
            logging.info("Data validation not performed.")
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
            logging.info(
                "Data validation completed - all columns valid according to schema."
            )
            return None

        # Checking for available keys against schema
        if self.schema.key in complete_columns:
            logging.info("Key column present in all rows, using as key.")
            key = {self.schema.key}
        elif self.schema.altkeys:
            # Checking if any valid altkeys can be formed from columns
            # Preferring shortest possible altkeys
            for altkey in sorted(self.schema.altkeys, key=len):
                if altkey.issubset(complete_columns):
                    logging.info(
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

    def read(
        self,
        select: Optional[list[str]] = None,
        filter: Optional[str] = None,
        expand: Optional[Union[str, list[DataverseExpand]]] = None,
        orderby: Optional[Union[str, list[DataverseOrderby]]] = None,
        top: Optional[int] = None,
        apply: Optional[str] = None,
        page_size: Optional[int] = None,
    ):
        """
        Reads entity.

        Optional args:
          - select: List of columns to return from the table.
          - filter: A fully qualified filtering string.
          - expand: A fully qualified expand string or a dict where
            each key corresponds to a related table, and each value is
            a list of related columns to select.
          - orderby: A fully qualified order_by string or a list of
            two-element tuples with column name first and asc/desc designation.

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
            params["$expand"] = parse_expand(expand, self.schema.relationships)
        if orderby is not None:
            params["$orderby"] = parse_orderby(orderby, self.schema.relationships)
        if top is not None:
            params["$top"] = top
        if apply is not None:
            params["$apply"] = apply

        output = []
        url = self.schema.name

        # Looping through pages
        while url:
            response: dict = self._get(
                url=url, additional_headers=additional_headers, params=params
            ).json()
            output.extend(response["value"])
            url = response.get("@odata.nextLink")

        return output

    def upload_image(
        self,
        image: DataverseImageFile,
        image_column: str,
        data: dict[str, Any],
        key_columns: Optional[Union[str, set[str]]] = None,
    ):
        extension = image.file_name.split(".")[1]
        if self._validate and extension in self.schema.illegal_extensions:
            raise DataverseError(
                f"Image extension '{extension}' blocked by organization."
            )

        key_columns = key_columns or self._validate_payload([data], mode="insert")
        _, row_key = extract_key(data=data, key_columns=key_columns)

        additional_headers = {
            "Content-Type": "application/octet-stream",
            "x-ms-file-name": image.file_name,
            "Content-Length": str(len(image.payload)),
        }

        url = f"{self.schema.name}({row_key})/{image_column}"

        self._patch(url=url, additional_headers=additional_headers, data=image.payload)
