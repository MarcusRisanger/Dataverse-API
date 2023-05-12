from typing import Iterator, List, Dict


def chunk_data(data: List[dict], size: int) -> Iterator[List[dict]]:
    # looping till length size
    for i in range(0, len(data), size):
        yield data[i : i + size]


class DataverseError(Exception):
    def __init__(self, message: str, status_code=None, response=None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = response


def expand_headers(
    headers: Dict[str, str], additional_headers: Dict[str, str]
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
