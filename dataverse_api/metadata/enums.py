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
    BIG_INT = "BigInt"
    BOOLEAN = "Boolean"
    DATE_TIME = "DateTime"
    DECIMAL = "Decimal"
    INTEGER = "Integer"
    LOOKUP = "Lookup"
    MEMO = "Memo"
    STRING = "String"


class AttributeTypeName(BaseEnum):
    BIG_INT = {"value": "BigIntType"}
    BOOLEAN = {"value": "BooleanType"}
    DATE_TIME = {"value": "DateTimeType"}
    DECIMAL = {"value": "DecimalType"}
    INTEGER = {"value": "IntegerType"}
    LOOKUP = {"value": "LookupType"}
    MEMO = {"Value": "MemoType"}
    STRING_TYPE = {"value": "StringType"}


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


class DateTimeFormat(BaseEnum):
    """
    Enum for DateTime Formats.
    """

    DATE_ONLY = "DateOnly"
    DATE_AND_TIME = "DateAndTime"


class IntegerFormat(BaseEnum):
    """
    Enum for Integer Formats.
    """

    NONE = "None"
    DURATION = "Duration"
    TIME_ZONE = "TimeZone"
    LANGUAGE = "Language"
    LOCALE = "Locale"


class MemoFormat(BaseEnum):
    """
    Enum for Memo Formats.
    """

    TEXT_AREA = "TextArea"
    RICH_TEXT = "RichText"


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
