import re
from functools import lru_cache
from typing import Any
from urllib.parse import quote


@lru_cache
def snake_to_title(snek: str) -> str:
    """
    Convert a string from snake_case to TitleCase.

    Parameters
    ----------
    snek : str
        snake_case string for conversion to TitleCase.
    """
    components = snek.split("_")
    return "".join([x.title() for x in components])


@lru_cache
def title_to_snake(title: str) -> str:
    """
    Convert a string from snake_case to TitleCase.

    Parameters
    ----------
    title : str
        TitleCase string for conversion to snake_case
    """
    return re.sub(r"(?<!^)(?=[A-Z])", "_", title).lower()


def convert_dict_keys_to_snake(arg: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively converts dict keys from snake_case to TitleCase.

    Parameters
    ----------
    arg : dict
    """
    out: dict[str, Any] = dict()
    for k, v in arg.items():
        key = title_to_snake(k)
        if k == "@odata.type":
            out["odata_type"] = v  # Corresponding value is always a string
        elif isinstance(v, dict):
            out[key] = convert_dict_keys_to_snake(v)  # type: ignore
        elif isinstance(v, list):
            out[key] = [convert_dict_keys_to_snake(e) for e in v]  # type: ignore
        else:
            out[key] = v
    return out


def convert_dict_keys_to_title(arg: dict[str, Any]) -> dict[str, Any]:
    """
    Converts dictionary keys from snake_case to TitleCase
    recursively.

    Parameters
    ----------
    arg : dict
    """
    out: dict[str, Any] = dict()
    for k, v in arg.items():
        if k == "odata_type":
            out["@odata.type"] = v  # Corresponding value is always a string
        elif k[0] == "@":
            out[k] = v
        elif isinstance(v, list):
            out[snake_to_title(k)] = [convert_dict_keys_to_title(d) for d in v]  # type: ignore
        elif isinstance(v, dict):
            out[snake_to_title(k)] = convert_dict_keys_to_title(v)  # type: ignore
        else:
            out[snake_to_title(k)] = v

    return dict(sorted(out.items()))  # Needs sort to ensure @odata tag first!


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

    def parse(part: re.Match) -> str:  # type: ignore
        return "'" + quote(part.group(1)) + "'"  # type: ignore

    pat = re.compile(r"\'([^\']*)\'")
    return re.sub(pat, parse, url)  # type: ignore
