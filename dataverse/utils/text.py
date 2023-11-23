from functools import lru_cache
from typing import Any


@lru_cache(maxsize=128)
def snake_to_title(snek: str) -> str:
    """
    Convert a string from snake_case to TitleCase.

    Parameters
    ----------
    snek : str
        Snake case string for conversion.
    """
    components = snek.split("_")
    return "".join([x.title() for x in components])


def convert_meta_keys_to_title_case(arg: dict[str, Any]) -> dict[str, Any]:
    """
    Converts dictionary keys from snake_case to TitleCase
    recursively.

    Parameters
    ----------
    obj : string or dict

    """
    out: dict[str, Any] = dict()
    for k, v in arg.items():
        if k == "_odata_type":
            out["@odata.type"] = v  # Corresponding value is always a string
        elif isinstance(v, list):
            out[snake_to_title(k)] = [d() for d in v]  # type: ignore
        elif callable(v):
            out[snake_to_title(k)] = v()
        else:
            out[snake_to_title(k)] = v

    return dict(sorted(out.items()))  # Needs sort to ensure @odata tag first!
