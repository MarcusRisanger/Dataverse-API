"""
Collection of Error classes specific to Dataverse API.
"""

from requests import Response


class DataverseError(Exception):
    """
    Basic Error class for Dataverse module.
    """

    def __init__(self, message: str, response: Response | None = None) -> None:
        super().__init__(message)
        self.response = response


class DataverseAPIError(Exception):
    """
    Error thrown for API communications.
    """

    def __init__(self, message: str, response: Response) -> None:
        super().__init__(message)
        self.response = response


class DataverseModeError(DataverseError):
    """
    Error thrown if wrong Mode is specified.
    """

    def __init__(self, mode: str, *valid: str):
        valids = "', '".join(valid)
        message = f"Mode '{mode}' is not supported. Options: '{valids}."
        super().__init__(message)
