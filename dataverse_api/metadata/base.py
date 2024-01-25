"""
The base Metadata class for Dataverse.
"""
from typing import Any, Protocol, Self

from pydantic import BaseModel, ConfigDict

from dataverse_api.utils.text import convert_dict_keys_to_snake, convert_dict_keys_to_title

BASE_TYPE = "Microsoft.Dynamics.CRM."


class MetadataDumper(Protocol):
    @property
    def schema_name(self) -> str:
        ...

    def dump_to_dataverse(self) -> dict[str, Any]:
        ...


class MetadataBase(BaseModel):
    """
    Defines the base of all Metadata dataclasses, and a few key
    methods used to serialize an API response and prepare an API request.
    """

    model_config = ConfigDict(extra="allow", validate_assignment=True)

    @classmethod
    def model_validate_dataverse(cls, arg: dict[str, Any]) -> Self:
        """
        Converts and validates a received deserialized JSON payload
        into the appropriate Metadata object.

        Parameters
        ----------
        arg : dict
            To be converted into a validated object.
        """

        converted = convert_dict_keys_to_snake(arg)
        return cls.model_validate(converted)

    def dump_to_dataverse(self, dropna: bool = True) -> dict[str, Any]:
        """
        When called, dumps the vars dictionary as TitleCase,
        needed to pass payloads to  Dataverse Web API.

        Returns
        -------
        dict
            A dictionary using Dataverse-friendly Keys,
            appropriately sorted.
        """
        if dropna:
            dump = self.model_dump(mode="json", exclude_none=True)
        else:
            dump = self.model_dump(mode="json")

        return convert_dict_keys_to_title(dump)
