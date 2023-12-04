from dataclasses import dataclass, field
from datetime import datetime as dt
from typing import Any, Optional, Union

import requests

from dataverse._api import Dataverse


@dataclass
class DataverseEntityAttribute:
    """
    For describing an Entity Attribute (table column).
    """

    schema_name: str
    can_create: bool
    can_update: bool
    attr_type: str
    data_type: Any
    max_height: Optional[int] = None
    max_length: Optional[int] = None
    max_size_kb: Optional[int] = None
    max_width: Optional[int] = None
    max_value: Optional[Union[dt, int, float]] = None
    min_value: Optional[Union[dt, int, float]] = None
    choices: Optional[dict[str, int]] = None


@dataclass
class DataverseEntityData:
    """
    Basic attributes for Entity.
    """

    name: str
    primary_attr: str
    primary_img: str
    language_code: int
    illegal_extensions: list[str] = field(default_factory=list)


@dataclass
class DataverseRelationships:
    """
    For describing relationships for Entity.
    """

    single_valued: list[str]
    collection_valued: list[str]

    def __call__(self) -> Any:
        return self.single_valued + self.collection_valued


class DataverseEntitySchema(Dataverse):
    """
    For describing the schema of an Entity.
    """

    def __init__(self, session: requests.Session, environment_url: str):
        super().__init__(session=session, environment_url=environment_url)
        self.entity: DataverseEntityData
        # self.attributes: dict[str, DataverseEntityAttribute]
        # self.relationships: DataverseRelationships
        # self.altkeys: list[set[str]]

    def _get_entity(self) -> int:
        return 1
