import json
import logging
from collections import Counter
from collections.abc import Collection, Iterable, Mapping
from typing import Any

import narwhals.stable.v2 as nw
from narwhals.stable.v2.typing import IntoDataFrame
from requests import Response
from typing_extensions import Protocol, runtime_checkable

from .text import parse_status_codes_in_batch

logger = logging.getLogger(__name__)


@runtime_checkable
class TimeType(Protocol):
    def isoformat(self) -> str: ...


def is_not_none(value: Any) -> bool:
    """Checks if a value is not None or NaN."""
    return value == value and value is not None  # noqa: PLR0124


def dict_of_lists_to_list_of_dicts(data: Mapping[str, Iterable[Any]]) -> list[dict[str, Any]]:
    """
    Converts a dictionary of lists into a list of dictionaries.
    Example:
        {'A': [1, 2], 'B': [3, 4]} -> [{'A': 1, 'B': 3}, {'A': 2, 'B': 4}]

    Parameters
    ----------
    data : Mapping[str, Iterable[Any]]
        The dictionary to convert. Each key maps to an iterable of values.

    Returns
    -------
    list[dict[str, Any]]
        A list of dictionaries where each dictionary corresponds to a row in the original data.
    """

    keys = list(data.keys())
    values = zip(*data.values())
    zipped = [dict(zip(keys, v)) for v in values]
    return [{k: v for k, v in row.items() if is_not_none(v)} for row in zipped]


def convert_dataframe_to_dict(data: IntoDataFrame) -> list[dict[str, Any]]:
    """
    Converts DataFrame to narwhals dict and drops NaNs.

    Parameters
    ----------
    data : IntoDataFrame
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
    raise TypeError(f"Type {type(obj)} not serializable")


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
    Extracts single valued relationships from a dict.

    Not sure how to firm this up to ignore "irrelevant" relationships.
    """
    target_col = "ReferencingEntityNavigationPropertyName"

    return [row[target_col] for row in data]


def check_batch_operations(data: Collection[Response]) -> None:
    counter = Counter()
    for response in data:
        status_codes = parse_status_codes_in_batch(response.text)
        counter.update(status_codes)
    successes = sum(counter.get(code, 0) for code in counter if code >= 200 and code < 300)
    total = sum(counter.values())
    failures = total - successes
    if failures > 0:
        logger.warning(
            "Batch operation had %d failures out of %d total operations.",
            failures,
            total,
        )
    else:
        logger.info("All %d batch operations succeeded.", total)
