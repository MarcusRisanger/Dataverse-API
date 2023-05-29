import json
from collections.abc import Iterator
from dataclasses import dataclass
from textwrap import dedent
from typing import Any, Optional, Union
from uuid import uuid4

import pandas as pd


@dataclass
class DataverseBatchCommand:
    uri: str
    mode: str
    data: Optional[dict[str, Any]] = None


@dataclass
class DataverseEntitySet:
    entity_set_name: str
    entity_set_key: str


@dataclass
class DataverseColumn:
    schema_name: str
    can_create: bool
    can_update: bool


@dataclass
class DataverseTableSchema:
    name: str
    key: Optional[str] = None
    columns: Optional[dict[str, DataverseColumn]] = None
    altkeys: Optional[list[set[str]]] = None


class DataverseError(Exception):
    def __init__(self, message: str, status_code=None, response=None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = response


def extract_batch_response_data(response_text: str) -> list[dict]:
    """
    Retrieves the data contained in a batch request return string.

    Args:
      - The text string returned by the request.

    Returns:
      - List of strings containing data in request return. Ready for
        `parse_entity_metadata` function.
    """

    out = []
    for row in response_text.splitlines():
        if len(row) == 0:
            continue
        if row[0] == "{":
            out.append(row)
    return out


def parse_entity_metadata(metadata: list[str]) -> DataverseTableSchema:
    """
    Parses entity metadata from list of dicts.

    Args:
      - A list containing responses from
    """
    for item in metadata:
        data = json.loads(item)
        if "$entity" in item:
            name = data["EntitySetName"]
            key = data["PrimaryIdAttribute"]
        elif "/Attributes" in item:
            columns = parse_meta_columns(data)
        elif "/Keys" in item:
            altkeys = parse_meta_keys(data)

    return DataverseTableSchema(name=name, key=key, columns=columns, altkeys=altkeys)


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


def parse_meta_keys(
    keys_metadata: list[Any],
) -> list[set[str]]:
    """
    Parses the available alternate keys based on the EntityKeyMetadata EntityType,
    given in response from the EntityDefinitions()/Keys endpoint.

    Required properties in Response body:
      - KeyAttributes

    Optional properties in Response body:
      - None

    Returns:
      - List of alternate key column combinations as sets.
    """
    keys: list[set] = list()

    items: list[dict] = keys_metadata["value"]
    for item in items:
        try:
            keys.append(set(item["KeyAttributes"]))
        except KeyError:
            raise DataverseError("Payload does not contain the necessary columns.")

    return keys


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
    for col in set(key_columns):
        key_value = data.pop(col).__repr__()  # Need repr to capture strings properly
        key_elements.append(f"{col}={key_value}")
    return (data, ",".join(key_elements))


def batch_id_generator() -> str:
    """Simply creates a unique string."""
    return str(uuid4())


def batch_command(batch_id: str, api_url: str, row: DataverseBatchCommand) -> str:
    if row.mode == "GET":
        row_command = f"""\
        --{batch_id}
        Content-Type: application/http
        Content-Transfer-Encoding: binary

        {row.mode} {api_url}{row.uri} HTTP/1.1

        """
    else:
        row_command = f"""\
        --{batch_id}
        Content-Type: application/http
        Content-Transfer-Encoding: binary

        {row.mode} {api_url}{row.uri} HTTP/1.1
        Content-Type: application/json{'; type=entry' if row.mode=="POST" else""}

        {json.dumps(row.data)}
        """

    return dedent(row_command)
