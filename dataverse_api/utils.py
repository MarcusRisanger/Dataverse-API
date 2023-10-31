"""
Contains various utils, to simplify code in the main modules.

Author: Marcus Risanger
"""


import json
import logging
import re
from collections.abc import Iterator
from datetime import datetime as dt
from textwrap import dedent
from typing import Any, Literal, Optional, Union
from urllib.parse import quote
from uuid import uuid4

import pandas as pd
import requests

from dataverse_api.dataclasses import (
    DataverseBatchCommand,
    DataverseEntityAttribute,
    DataverseExpand,
    DataverseOrderby,
)
from dataverse_api.errors import DataverseError, DataverseValidationError

log = logging.getLogger("dataverse-api")


def _altkey_identify_illegal_symbols(part: str) -> None:
    """
    Identifies symbols used in strings that are illegal
    in altkeys for PATCH and GET operations.

    Parameters
    ----------
    part : str
        The capturing group of a match part corresponding to
        a string literal for an alternate key used in a Dataverse
        API call, including enclosing ticks to denote a string literal.

    Raises
    ------
    ValueError
        If the matched part includes a string that is not
        possible to use within an altkey.
    """
    illegals = ["/", "<", ">", "*", "%", "&", ":", "?", "+"]
    if any([i in rf"{part}" for i in illegals]):
        raise ValueError(f"Illegal symbol in alternate key url string literal: {part}.")


def _altkey_encoding(part: str) -> str:
    """
    Encoding function for alternate keys in URLs to handle
    non-English letters like æøå.

    Parameters
    ----------
    part: str
        The capturing group of a match part corresponding to
        a string literal for an alternate key used in a Dataverse
        API call, including enclosing ticks to denote a string literal.

    Returns
    -------
    str
        Encoded string literal, including enclosing ticks.
    """
    return "'" + quote(part) + "'"


def _parse_altkey(part: re.Match):
    """
    Function to be called in re.sub() to handle
    validation and encoding of alternate key part.

    Parameters
    ----------
    part : re.Match
        The match from a Dataverse API call URL, containing a
        capture group with an alternate key string literal.

    Returns
    -------
    str
        The encoded string literal, including enclosing ticks.
    """

    capture_group = part.group(1)

    _altkey_identify_illegal_symbols(capture_group)
    return _altkey_encoding(capture_group)


def encode_altkeys(url: str) -> str:
    """
    Function used to encode altkeys in Dataverse API calls.

    Parameters
    ----------
    url : str
        The API call URL that is to be encoded.

    Returns
    -------
    str
        The encoded URL.
    """
    pat = re.compile(r"\'([^\']*)\'")
    return re.sub(pat, _parse_altkey, url)


def get_val(col: dict, attr: Literal["Min", "Max"]) -> Union[dt, int, float]:
    """
    Used to parse column metadata to clean up code in schema module, to
    handle both datetime min/max and regular int/float min/max attributes.

    Min/Max values are yielded from the API with different attribute names,
    and are in different formats. We cast time formates to datetime, otherwise
    pass the JSON parsed value directly. Both support <, > operations for
    validating that columnar values are within their limits.

    Parameters
    ----------
    col: dict
        The dictionary representing the column definition from the API
    attr: str
        Whether the function should grab the min or max value.

    Returns
    -------
    datetime, int, float or None
        Respective column value limit. `datetime` for DateTime type columns,
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

    Parameters
    ----------
    response : requests.Response
        The response returned by the batch request.

    Returns
    -------
    list of dicts
        List of dictionaries containing data in request return.
        Ready for `parse_entity_metadata` function.
    """
    response_text = response.text
    out = []
    for row in response_text.splitlines():
        if len(row) == 0:
            continue
        if row[0] == "{":
            out.append(json.loads(row))
    return out


def chunk_data(
    data: list[DataverseBatchCommand], size: int = 500
) -> Iterator[list[DataverseBatchCommand]]:
    """
    Simple function to chunk a list into a maximum number of
    elements per chunk.

    Parameters
    ----------
    data : list of `DataverseBatchCommand`
        List containing all commands to be chunked.
    size: int, optional
        Chunking size.

    Yields
    ------
    list of `DataverseBatchCommand`
    """
    for i in range(0, len(data), size):
        yield data[i : i + size]  # noqa E203


def expand_headers(
    headers: dict[str, str], additional_headers: Optional[dict[str, str]] = None
) -> dict[str, str]:
    """
    Overwrites a set of (default) headers with alternate headers.

    Parameters
    ----------
    headers: dict
        The base headers.
    additional_headers: dict
        Additional headers to include. Duplicates in original header will
        be overwritten.

    Returns
    -------
    dict
        New dict with replaced headers.
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

    Parameters
    ----------
    data: dict, list of dicts or `pd.DataFrame`
        Data payload to be normalized into a list of dicts.

    Returns
    -------
    list of dicts
        Data payload ready for validation/processing.
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

    Parameters
    ----------
    data: dict
        A dict containing all columns
    key_columns: set
        A set containing all key columns

    Returns
    -------
    tuple
        1st element: A modified data dict where any key columns are removed
        2nd element: A string that contains the Dataverse specification
        row identifier
    """
    data = data.copy()
    key_elements = []
    if isinstance(key_columns, str):
        key_columns = {key_columns}
    for col in key_columns:
        key_value = data.pop(col).__repr__()  # Need repr to capture strings properly
        key_elements.append(f"{col}={key_value}")
    return (data, ",".join(key_elements))


def batch_id_generator() -> str:
    """
    Simply creates a unique string for batch IDs.

    Returns
    -------
    str
        UUID4 string.
    """
    return str(uuid4())


def batch_command(batch_id: str, api_url: str, row: DataverseBatchCommand) -> str:
    """
    Translates a batch command to the actual request string payload.

    Parameters
    ----------
    batch_id: str
        Unique-ish string that delinates the batch.
    api_url: str
        The Dataverse Resource API endpoint.
    row: `DataverseBatchCommand`
        The associated batching command data.

    Returns
    -------
    str
        The string representation of the batch command.
    """

    uri = api_url + row.uri
    data = row.data

    if row.mode == "PUT":
        col, value = list(row.data.items())[0]
        uri += f"/{col}"
        data = {"value": value}

    uri = encode_altkeys(uri)

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
    schema_columns: dict[str, DataverseEntityAttribute],
    mode: Literal["create", "update", "upsert"],
):
    """
    Simple function to isolate columns passed in data as attributes
    invalid for passing in create or update.

    Parameters
    ----------
    key_columns: set of str
        Set of key column(s) to be ignored.
    data_columns: set of str
        The data columns passed from user.
    schema_columns: dict of `DataverseEntityAttribute`
        The data columns available in schema.
    mode: str
        The type of operation to be carried out.
    """
    invalid_cols = set()
    for col in data_columns:
        if col in key_columns:
            continue

        create = schema_columns[col].can_create
        update = schema_columns[col].can_update

        if not create and mode == "create":
            invalid_cols.add(col)
        elif not update and mode == "update":
            invalid_cols.add(col)
        elif (create ^ update) and mode == "upsert":  # XOR: if only one is true
            invalid_cols.add(col)

    if invalid_cols and (mode in ["create", "update"]):
        cols = ", ".join(sorted(invalid_cols))
        raise DataverseError(f"Found columns not valid for {mode}: {cols}")

    if invalid_cols and mode == "upsert":
        log.warning(f"Found columns that may throw errors in upsert: {cols}")


def parse_expand(
    expand: Union[str, DataverseExpand, list[DataverseExpand]],
) -> str:
    """
    Parses an expand clause and returns an appropriate string for
    querying the Dataverse entity.

    Parameters
    ----------
    expand : str, `DataverseExpand` or list of `DataverseExpand`
        Either a manually written expand clause, an instance
        of `DataverseExpand` or a list of `DataverseExpand` objects
        that describe the full set of expansions to apply.

    Returns
    -------
    str
        String representation of the expansion clause.
    """
    if isinstance(expand, str):
        return expand

    if not isinstance(expand, list):
        expand = [expand]

    output = []
    for rule in expand:
        output.append(parse_expand_element(rule))

    return ",".join(output)


def parse_expand_element(expand: DataverseExpand) -> str:
    """
    Parses an expansion rule and returns an appropriate string for
    querying the Dataverse entity.

    Parameters
    ----------
    expand: `DataverseExpand`
        An instance of `DataverseExpand` that describes the
        expansion rules to apply.

    Raises
    ------
    `DataverseValidationError`
        If an expand clause contains a nested expand
        clause and either of these specify an orderby clause.
    """

    # Some validation rules
    if expand.expand and (expand.orderby or expand.expand.orderby):
        raise DataverseValidationError("Cannot use orderby with nested expand.")

    # Parsing
    elements = []
    if expand.select:
        elements.append(f"$select={','.join(expand.select)}")
    if expand.filter:
        elements.append(f"$filter={expand.filter}")
    if expand.orderby:
        ordering = parse_orderby(expand.orderby)
        elements.append(f"$orderby={ordering}")
    if expand.top:
        elements.append(f"$top={expand.top}")
    if expand.expand:
        elements.append(f"$expand={parse_expand(expand.expand)})")
    return f"{expand.table}({';'.join(elements)})"


def parse_orderby(
    orderby: Union[str, list[DataverseOrderby]],
):
    """
    Parses the orderby argument for Dataverse querying.

    Parameters
    ----------
    orderby: str or list of `DataverseOrderBy`
        Either a string assumed to be fully qualified for query
        or a list of `DataverseOrderBy` objects for parsing.

    Returns
    -------
    str
        A fully qualified Order By string for use in query.
    """
    if isinstance(orderby, str):
        return orderby

    if not isinstance(orderby, list):
        orderby = [orderby]

    ordering = []
    for order in orderby:
        ordering.append(f"{order.attr} {order.direction}")
    return ",".join(ordering)


def assign_expected_type(dataverse_type: str) -> type:
    """
    Returns the expected data type based on the column
    attribute type string.

    Parameters
    ----------
    dataverse_type: str
        The type designation of a Dataverse Attribute.

    Returns
    -------
    type
        The corresponding Python data type for the Attribute.
    """
    dates = ["DateTimeType"]
    bools = ["BooleanType"]
    floats = ["MoneyType", "DoubleType", "DecimalType"]
    ints = ["BigIntType", "IntegerType"]
    octets = ["ImageType", "FileType"]
    strings = [
        "PicklistType",
        "StringType",
        "MemoType",
        "UniqueidentifierType",
    ]

    if dataverse_type in floats:
        return float
    elif dataverse_type in ints:
        return int
    elif dataverse_type in strings:
        return str
    elif dataverse_type in dates:
        return dt
    elif dataverse_type in octets:
        return bytes
    elif dataverse_type in bools:
        return bool
