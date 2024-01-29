import json
from collections.abc import Mapping
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
