"""
The base Metadata class for Dataverse.
"""
from dataclasses import dataclass, fields
from typing import Any

from dataverse.utils.text import convert_meta_keys_to_title_case

BASE_TYPE = "Microsoft.Dynamics.CRM."


@dataclass
class MetadataBase:
    """
    Defines the base of all Metadata dataclasses.
    """

    def __call__(self) -> dict[str, Any]:
        """
        When called, dumps the vars dictionary as TitleCase,
        needed to pass payloads to  Dataverse Web API.
        """
        return convert_meta_keys_to_title_case(self.__dict__)

    def __post_init__(self) -> None:
        """
        Ensure that all dataclass fields show up in vars, so that
        any non-initialized defaults show up in e.g. .__dict__
        """
        for field in fields(self):
            if field.name not in vars(self):
                setattr(self, field.name, getattr(self, field.name))
