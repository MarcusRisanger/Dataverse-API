"""
Collection of Error classes specific to Dataverse API.

Author: Marcus Risanger
"""


class DataverseError(Exception):
    def __init__(self, message: str, status_code=None, response=None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class DataverseValidationError(DataverseError):
    def __init__(self, message: str, status_code=None, response=None) -> None:
        super().__init__(message, status_code, response)
