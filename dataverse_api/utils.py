import xml.etree.ElementTree as ET
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Optional, Union
from uuid import uuid4

import pandas as pd
import requests


@dataclass
class DataverseBatchCommand:
    uri: str
    mode: str
    data: dict[str, Any]


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


def parse_meta_columns(
    attribute_metadata: requests.Response,
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
    """
    columns = dict()
    items: list[dict] = attribute_metadata.json()["value"]
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
        except KeyError as e:
            raise DataverseError(
                f"Payload does not contain the necessary columns. {e}", message=e
            )

    return columns


def parse_meta_keys(
    keys_metadata: requests.Response,
) -> list[set[str]]:
    """
    Parses the available alternate keys based on the EntityKeyMetadata EntityType,
    given in response from the EntityDefinitions()/Keys endpoint.

    Required properties in Response body:
      - KeyAttributes

    Optional properties in Response body:
      - None
    """
    keys: list[set] = list()

    items: list[dict] = keys_metadata.json()["value"]
    for item in items:
        try:
            keys.append(set(item["KeyAttributes"]))
        except KeyError as e:
            raise DataverseError(
                f"Payload does not contain the ncessary columns. {e}", message=e
            )

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
    Extracts key from the given dictionary.

    Args:
      - data: A dict containing all columns
      - key_columns: A set containing all key columns

    Returns a tuple with two elements:
      - 0: A modified dictionary where any key columns are removed
      - 1: A string that contains the Dataverse specification
        row identifier
    """
    data = data.copy()
    key_elements = []
    for col in set(key_columns):
        key_elements.append(f"{col}={data.pop(col).__repr__()}")
    return (data, ",".join(key_elements))


def batch_id_generator() -> str:
    """Simply creates a unique string."""
    return str(uuid4())


def parse_metadata(raw_schema: str) -> dict[str, DataverseTableSchema]:
    entities: dict[str, DataverseTableSchema] = {}
    schema = ET.fromstring(raw_schema)
    for table in schema.findall(".//{*}EntityType"):
        # Get key
        key = table.find(".//{*}PropertyRef")
        if key is None:  # Some special entities have no key attribute
            continue
        else:
            key = key.attrib["Name"]

        table_name = table.attrib["Name"] + "s"
        columns: set[str] = set()
        altkeys: list[set[str]] = list()

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
        entities[table_name] = DataverseTableSchema(
            key=key, columns=columns, altkeys=altkeys
        )

    return entities
