from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Set, Union
from uuid import uuid4

import pandas as pd


@dataclass
class DataverseBatchCommand:
    uri: str
    mode: str
    data: Dict[str, Any]


def chunk_data(
    data: List[DataverseBatchCommand], size: int
) -> Iterator[List[DataverseBatchCommand]]:
    # looping till length size
    for i in range(0, len(data), size):
        yield data[i : i + size]  # noqa E203


class DataverseError(Exception):
    def __init__(self, message: str, status_code=None, response=None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = response


def expand_headers(
    headers: Dict[str, str], additional_headers: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """
    Overwrites a set of (default) headers with alternate headers.

    Args:
      - headers: Default header dict
      - additional_headers: Headers with which to add or overwrite defaults.

    Returns:
      - New dict with
    """
    new_headers = headers.copy()
    if additional_headers is not None:
        for header in additional_headers:
            new_headers[header] = additional_headers[header]
    return new_headers


def convert_data(data: Union[dict, List[dict], pd.DataFrame]) -> List[dict]:
    """
    Normalizes data to a list of dicts, ready to be
    processed into DataverseBatchCommands.
    """
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        return [data]
    elif isinstance(data, pd.DataFrame):
        return [
            {k: v for k, v in m.items() if pd.notnull(v)}
            for m in data.to_dict("records")
        ]
    else:
        raise DataverseError(
            "Data seems to be of a not supported type: " + f"{data.__class__.__name__}."
        )


def extract_key(data: Dict[str, Any], key_columns: Set[str]) -> str:
    """
    Extracts key from the given dictionary.

    Note: Dict will mutate.
    """
    key_elements = []
    for col in key_columns:
        key_elements.append(f"{col}={data.pop(col).__repr__()}")
    return ",".join(key_elements)


def batch_id_generator() -> str:
    """Creates a unique string"""
    return str(uuid4())
