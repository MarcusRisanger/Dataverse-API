"""
Contains the different dataclasses used to handle data structures
in the other modules in the package.

Author: Marcus Risanger
"""


import logging
from dataclasses import dataclass
from datetime import datetime as dt
from typing import Any, Optional, Union

from msal_requests_auth.auth import ClientCredentialAuth, DeviceCodeAuth

log = logging.getLogger()


@dataclass
class DataverseAuth:
    resource: str
    auth: Union[ClientCredentialAuth, DeviceCodeAuth]


@dataclass
class DataverseBatchCommand:
    uri: str
    mode: str
    data: Optional[dict[str, Any]] = None


@dataclass
class DataverseEntitySet:
    entity_set_name: str
    entity_set_key: str


@dataclass
class DataverseColumn:
    schema_name: str
    can_create: bool
    can_update: bool
    attr_type: str
    max_height: Optional[int] = None
    max_length: Optional[int] = None
    max_size: Optional[int] = None
    max_width: Optional[int] = None
    max_value: Optional[Union[dt, int, float]] = None
    min_value: Optional[Union[dt, int, float]] = None
    choices: Optional[dict[Any, Any]] = None


@dataclass
class DataverseEntitySchema:
    name: Optional[str] = None
    key: Optional[str] = None
    language_code: Optional[int] = None
    columns: Optional[dict[str, DataverseColumn]] = None
    altkeys: Optional[list[set[str]]] = None


@dataclass
class DataverseRawSchema:
    entity_data: Optional[dict] = None
    column_data: Optional[dict] = None
    altkey_data: Optional[dict] = None
    language_data: Optional[int] = None
    choice_data: Optional[dict] = None
