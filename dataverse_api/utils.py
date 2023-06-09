"""
Contains various utils, to simplify code in the main modules.

Author: Marcus Risanger
"""


import json
import logging
from collections.abc import Iterator
from datetime import datetime as dt
from textwrap import dedent
from typing import Any, Literal, Optional, Union
from uuid import uuid4

import pandas as pd
import requests

from dataverse_api.dataclasses import DataverseBatchCommand, DataverseColumn
from dataverse_api.errors import DataverseError


def get_val(col: dict, attr: Literal["Min", "Max"]) -> Union[dt, int, float]:
    """
    Used to parse column metadata to clean up code in schema module, to
    handle both datetime min/max and regular int/float min/max attributes.

    Min/Max values are yielded from the API with different attribute names,
    and are in different formats. We cast time formates to datetime, otherwise
    pass the JSON parsed value directly. Both support <, > operations for
    validating that columnar values are within their limits.

    Args:
      - col: The dictionary representing the column definition from the API
      - attr: Whether the function should grab the min or max value.

    Returns:
      - Respective column value limit. `datetime` for DateTime type columns,
        otherwise `int` or `float` for numerical columns, else `None`.
    """
    try:
        val = dt.fromisoformat(str(col.get(f"{attr}SupportedValue"))[:-1] + "+00:00")
    except ValueError:
        val = col.get(f"{attr}Value")
    return val


def extract_batch_response_data(response: requests.Response) -> list[dict]:
    """
    Retrieves the data contained in a batch request return Response.
    Order of arguments is based on order of batch commands.

    Args:
      - The text string returned by the request.

    Returns:
      - List of dicts containing data in request return. Ready for
        `parse_entity_metadata` function.
    """
    response_text = response.text
    out = []
    for row in response_text.splitlines():
        if len(row) == 0:
            continue
        if row[0] == "{":
            out.append(json.loads(row))
    return out


def parse_meta_columns(
    attribute_metadata: dict[Any],
) -> dict[str, DataverseColumn]:
    """
    Parses the available columns based on the AttributeMetadata EntityType,
    given in response from the EntityDefinitions()/Attribute endpoint.

    Required properties in Response body:
      - LogicalName
      - SchemaName
      - IsValidForCreate
      - IsValidForUpdate

    Optional properties in Response body:
      - IsValidODataAttribute

    Returns:
      - Dict with column names as key and `DataverseColumn` as values.
    """
    columns = dict()
    items: list[dict] = attribute_metadata["value"]
    for item in items:
        try:
            if item.get("IsValidODataAttribute", True) and (
                item["IsValidForCreate"] or item["IsValidForUpdate"]
            ):
                columns[item["LogicalName"]] = DataverseColumn(
                    schema_name=item["SchemaName"],
                    can_create=item["IsValidForCreate"],
                    can_update=item["IsValidForUpdate"],
                )
        except KeyError:
            raise DataverseError("Payload does not contain the necessary columns.")

    return columns


def chunk_data(
    data: list[DataverseBatchCommand], size: int
) -> Iterator[list[DataverseBatchCommand]]:
    """
    Simple function to chunk a list into a maximum number of
    elements per chunk.
    """
    for i in range(0, len(data), size):
        yield data[i : i + size]  # noqa E203


def expand_headers(
    headers: dict[str, str], additional_headers: Optional[dict[str, str]] = None
) -> dict[str, str]:
    """
    Overwrites a set of (default) headers with alternate headers.

    Args:
      - headers: Default header dict
      - additional_headers: Headers with which to add or overwrite defaults.

    Returns:
      - New dict with replaced headers.
    """
    new_headers = headers.copy()
    if additional_headers is not None:
        for header in additional_headers:
            new_headers[header] = additional_headers[header]
    return new_headers


def convert_data(data: Union[dict, list[dict], pd.DataFrame]) -> list[dict]:
    """
    Normalizes data to a list of dicts, ready to be validated
    and processed into DataverseBatchCommands.

    Args:
      - data: Data payload as either a single dict, list of dicts
        or a `pd.DataFrame`.

    Returns:
      - Data payload as list of dicts.

    Raises:
      - DataverseError if arguments are of incorrect type.
    """
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return [data]
    elif isinstance(data, pd.DataFrame):
        return [
            {k: v for k, v in row.items() if pd.notnull(v)}
            for row in data.to_dict("records")
        ]
    else:
        raise DataverseError(
            "Data seems to be of a not supported type: " + f"{data.__class__.__name__}."
        )


def extract_key(
    data: dict[str, Any], key_columns: Union[str, set[str]]
) -> tuple[dict[str, Any], str]:
    """
    Extracts key from the given dictionary. The key identifies a
    row in Dataverse based on column name and corresponding value,
    and can consist of several columns, comma separated. E.g.:

    >>> key = "col1=123,col2='abc'"
    >>> requests.get(f"api_endpoint/tests({key})")

    Args:
      - data: A dict containing all columns
      - key_columns: A set containing all key columns

    Returns a tuple with two elements:
      - 0: A modified data dict where any key columns are removed
      - 1: A string that contains the Dataverse specification
        row identifier
    """
    data = data.copy()
    key_elements = []
    if type(key_columns) == str:
        key_columns = {key_columns}
    for col in key_columns:
        key_value = data.pop(col).__repr__()  # Need repr to capture strings properly
        key_elements.append(f"{col}={key_value}")
    return (data, ",".join(key_elements))


def batch_id_generator() -> str:
    """Simply creates a unique string."""
    return str(uuid4())


def batch_command(batch_id: str, api_url: str, row: DataverseBatchCommand) -> str:
    """
    Translates a batch command to the actual request string payload.

    Args:
      - batch_id: Unique-ish string that delinates the batch
      - api_url: The Dataverse Resource API endpoint
      - row: The associated batch command data
    """

    uri = api_url + row.uri
    data = row.data

    if row.mode == "PUT":
        col, value = list(row.data.items())[0]
        uri += f"/{col}"
        data = {"value": value}

    row_command = f"""\
    --{batch_id}
    Content-Type: application/http
    Content-Transfer-Encoding: binary

    {row.mode} {uri} HTTP/1.1
    Content-Type: application/json{'; type=entry' if row.mode=="POST" else""}

    {json.dumps(data)}
    """
    return dedent(row_command)


def find_invalid_columns(
    key_columns: set[str],
    data_columns: set[str],
    schema_columns: dict[str, DataverseColumn],
    mode: Literal["create", "update", "upsert"],
):
    """
    Simple function to isolate columns passed in data as attributes
    invalid for passing in create or update.

    Args:
      - key_columns: Set of key column(s) to be ignored
      - data_columns: The data columns passed from user
      - schema_columns: The data columns available in schema
    """
    baddies = set()
    for col in data_columns:
        if col in key_columns:
            continue

        create = schema_columns[col].can_create
        update = schema_columns[col].can_update

        if not create and mode == "create":
            baddies.add(col)
        elif not update and mode == "update":
            baddies.add(col)
        elif (create ^ update) and mode == "upsert":  # XOR: if only one is true
            baddies.add(col)

    if baddies and (mode in ["create", "update"]):
        cols = ", ".join(sorted(baddies))
        raise DataverseError(f"Found columns not valid for {mode}: {cols}")

    if baddies and mode == "upsert":
        logging.warning(f"Found columns that may throw errors in upsert: {cols}")
