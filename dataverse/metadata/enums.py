"""
A collection of Dataverse Enum metadata classes.
"""
from enum import Enum


class BaseEnum(Enum):
    """
    Base Enum class as callable. Used for standard Enums that return only a string.
    Note that the API payload requires the TitleCased Enum names and not the
    integer counterparts listed in Microsofts resource pages.
    """

    def _get_value(self) -> str | dict[str, str]:
        return self.value


class StringFormat(BaseEnum):
    """
    Enum for String Format options.
    """

    EMAIL = {"value": "Email"}
    TEXT = {"value": "Text"}
    TEXT_AREA = {"value": "TextArea"}
    URL = {"value": "Url"}
    TICKER_SYMBOL = {"value": "TickerSymbol"}
    PHONETIC_GUIDE = {"value": "PhoneticGuide"}
    VERSION_NUMBER = {"value": "VersionNumber"}
    # PHONE = {"value":"Phone"} Internal use only.
    JSON = {"value": "Json"}
    RICH_TEXT = {"value": "RichText"}


class AttributeType(BaseEnum):
    STRING = "String"
    LOOKUP = "Lookup"


class AttributeTypeName(BaseEnum):
    STRING_TYPE = {"value": "StringType"}
    LOOKUP = {"value": "Lookup"}


class AssociatedMenuBehavior(BaseEnum):
    """
    Enum for Associated Menu Behavior for Relationships.
    """

    USE_COLLECTION_NAME = "UseCollectionName"
    USE_LABEL = "UseLabel"
    DO_NOT_DISPLAY = "DoNotDisplay"


class AssociatedMenuGroup(BaseEnum):
    """
    Enum for Associated Menu Group for Relationships.
    """

    DETAILS = "Details"
    SALES = "Sales"
    SERVICE = "Service"
    MARKETING = "Marketing"


class CascadeType(BaseEnum):
    """
    Enum for Cascade Types.
    """

    NO_CASCADE = "NoCascade"
    CASCADE = "Cascade"
    ACTIVE = "Active"
    USER_OWNED = "UserOwned"
    REMOVE_LINK = "RemoveLink"
    RESTRICT = "Restrict"


class OwnershipType(BaseEnum):
    """
    Enum for Ownership Types for Entities.
    """

    NONE = "None"
    USER_OWNED = "UserOwned"
    ORGANIZATION_OWNED = "OrganizationOwned"


class AttributeRequiredLevel(BaseEnum):
    """
    Enum for Required Level of Attribute.
    """

    NONE = "None"
    APPLICATION_REQUIRED = "ApplicationRequired"
    RECOMMENDED = "Recommended"
