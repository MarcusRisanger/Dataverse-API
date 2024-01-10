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
