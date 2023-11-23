"""
A collection of Dataverse Enum metadata classes.
"""
from enum import Enum, auto

from dataverse.utils.text import snake_to_title


class BaseEnum(Enum):
    """
    Base Enum class as callable. Used for standard Enums that return only a string.
    Note that the API payload requires the TitleCased Enum names and not the
    integer counterparts listed in Microsofts resource pages.
    """

    def __call__(self) -> str:
        return snake_to_title(self.name)


class StringFormat(Enum):
    """
    Enum for String Format options.
    """

    EMAIL = auto()
    TEXT = auto()
    TEXT_AREA = auto()
    URL = auto()
    TICKER_SYMBOL = auto()
    PHONETIC_GUIDE = auto()
    VERSION_NUMBER = auto()
    PHONE = auto()
    JSON = auto()
    RICH_TEXT = auto()

    def __call__(self) -> dict[str, str]:
        return {"Value": snake_to_title(self.name)}


class AssociatedMenuBehavior(BaseEnum):
    """
    Enum for Associated Menu Behavior for Relationships.
    """

    USE_COLLECTION_NAME = auto()
    USE_LABEL = auto()
    DO_NOT_DISPLAY = auto()


class AssociatedMenuGroup(BaseEnum):
    """
    Enum for Associated Menu Group for Relationships.
    """

    DETAILS = auto()
    SALES = auto()
    SERVICE = auto()
    MARKETING = auto()


class CascadeType(BaseEnum):
    """
    Enum for Cascade Types.
    """

    NO_CASCADE = auto()
    CASCADE = auto()
    ACTIVE = auto()
    USER_OWNED = auto()
    REMOVE_LINK = auto()
    RESTRICT = auto()


class OwnershipType(BaseEnum):
    """
    Enum for Ownership Types for Entities.
    """

    NONE = auto()
    USER_OWNED = auto()
    ORGANIZATION_OWNED = auto()


class AttributeRequiredLevel(BaseEnum):
    """
    Enum for Required Level of Attribute.
    """

    NONE = auto()
    APPLICATION_REQUIRED = auto()
    RECOMMENDED = auto()
