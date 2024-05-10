import json
from collections.abc import Collection, Mapping
from datetime import date, datetime
from typing import Any

import pandas as pd


def convert_dataframe_to_dict(data: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Converts to dict and drops NaNs.
    """
    return [{k: v for k, v in m.items() if v == v and v is not None} for m in data.to_dict(orient="records")]  # type: ignore


def coerce_timestamps(obj: object) -> str:
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date, pd.Timestamp)):
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
