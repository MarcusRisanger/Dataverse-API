"""
Contains the different dataclasses used to handle data structures
in the other modules in the package.

Author: Marcus Risanger
"""


from dataclasses import dataclass
from datetime import datetime as dt
from typing import Any, Literal, Optional, Union

from msal_requests_auth.auth import ClientCredentialAuth, DeviceCodeAuth


@dataclass
class DataverseAuth:
    """
    For encapsulating Dataverse authentication parameters.
    """

    resource: str
    auth: Union[ClientCredentialAuth, DeviceCodeAuth]


@dataclass
class DataverseBatchCommand:
    """
    For encapsulating a singular Dataverse batch command.
    """

    uri: str
    mode: str = "GET"
    data: Optional[dict[str, Any]] = None


@dataclass
class DataverseOrderby:
    """
    For structuring $orderby Dataverse query clauses.
    """

    attr: str
    direction: Literal["asc", "desc"] = "asc"


@dataclass
class DataverseExpand:
    """
    For structuring $expand Dataverse query clauses.
    """

    table: str
    select: list[str]
    filter: Optional[str] = None
    orderby: Optional[Union[str, list[DataverseOrderby]]] = None
    top: Optional[int] = None
    expand: Optional["DataverseExpand"] = None


@dataclass
class DataverseFile:
    """
    For encapsulating image data for uploading to Dataverse.
    """

    file_name: str
    payload: bytes


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
    language_code: Optional[int] = None
    illegal_extensions: Optional[list[str]] = None


@dataclass
class DataverseRelationships:
    """
    For describing relationships for Entity.
    """

    single_valued: list[str]
    collection_valued: list[str]

    def __call__(self) -> Any:
        return self.single_valued + self.collection_valued


@dataclass
class DataverseEntitySchema:
    """
    For describing the schema of an Entity.
    """

    entity: Optional[DataverseEntityData] = None
    attributes: Optional[dict[str, DataverseEntityAttribute]] = None
    altkeys: Optional[list[set[str]]] = None
    relationships: Optional[DataverseRelationships] = None


@dataclass
class DataverseRawSchema:
    """
    For storing the raw schema data API responses.
    """

    entity_data: Optional[dict] = None
    organization_data: Optional[int] = None
    attribute_data: Optional[dict] = None
    altkey_data: Optional[dict] = None
    one_many_data: Optional[dict] = None
    many_one_data: Optional[dict] = None
    choice_data: Optional[dict] = None
