from typing import Any

import pandas as pd


def convert_dataframe_to_dict(data: pd.DataFrame) -> list[dict[str, Any]]:
    return data.to_dict(orient="records")  # type: ignore
