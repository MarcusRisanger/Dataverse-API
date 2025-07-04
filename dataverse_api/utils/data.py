import json
from collections.abc import Collection, Mapping
from typing import Any

import narwhals as nw
from narwhals.typing import IntoFrameT
from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class TimeType(Protocol):
    def isoformat(self) -> str: ...


def is_not_none(value: Any) -> bool:
    """Checks if a value is not None or NaN."""
    return value == value and value is not None


def dict_of_lists_to_list_of_dicts(data: dict[str, list[Any]]) -> list[dict[str, Any]]:
    """
    Converts a dictionary of lists into a list of dictionaries.
    Example:
        {'A': [1, 2], 'B': [3, 4]} -> [{'A': 1, 'B': 3}, {'A': 2, 'B': 4}]

    Parameters
    ----------
    data : dict[str, list[Any]]
        The dictionary to convert.

    Returns
    -------
    list[dict[str, Any]]
        A list of dictionaries where each dictionary corresponds to a row in the original data.
    """

    keys = list(data.keys())
    values = zip(*data.values())
    zipped = [dict(zip(keys, v)) for v in values]
    return [{k: v for k, v in row.items() if v == v and v is not None} for row in zipped]


def convert_dataframe_to_dict(data: IntoFrameT) -> list[dict[str, Any]]:
    """
    Converts DataFrame to narwhals dict and drops NaNs.

    Parameters
    ----------
    data : IntoFrameT
        The data to convert, which can be a DataFrame or similar structure.

    Returns
    -------
    list[dict[str, Any]]
        A list of dictionaries where each dictionary corresponds to a row in the DataFrame.
    """
    df = nw.from_native(data)
    data_dict = df.to_dict(as_series=False)

    return dict_of_lists_to_list_of_dicts(data_dict)


def coerce_timestamps(obj: object) -> str:
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, TimeType):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def serialize_json(obj: Mapping[str, Any] | None) -> str:
    if obj is None:
        return ""
    return json.dumps(obj, default=coerce_timestamps)


def extract_collection_valued_relationships(data: Collection[dict[str, Any]], entity_logical_name: str) -> list[str]:
    """
    Extracts collection valued relationships from a dict.

    Ignores relationships where the referencing attribute is in the `ignore` list.
    """
    target_col = "ReferencedEntityNavigationPropertyName"
    filter_col = "ReferencingEntityNavigationPropertyName"
    ignore = [f"objectid_{entity_logical_name}", f"regardingobjectid_{entity_logical_name}"]

    return [row[target_col] for row in data if row[filter_col] not in ignore]


def extract_single_valued_relationships(data: Collection[dict[str, Any]]) -> list[str]:
    """
    Extracts collection valued relationships from a dict.

    Not sure how to firm this up to ignore "irrelevant" relationships.
    """
    target_col = "ReferencingEntityNavigationPropertyName"

    return [row[target_col] for row in data]
