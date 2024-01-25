from datetime import date, datetime
from typing import Any

import pandas as pd


def convert_dataframe_to_dict(data: pd.DataFrame) -> list[dict[str, Any]]:
    return data.to_dict(orient="records")  # type: ignore


def coerce_timestamps(obj: object) -> str:
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date, pd.Timestamp)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))
