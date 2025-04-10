"""
A collection of Dataverse Complex Property metadata classes.
"""

from collections.abc import Sequence
from typing import Any, overload

from pydantic import Field

from dataverse_api.metadata.base import BASE_TYPE, MetadataBase
from dataverse_api.metadata.enums import (
    AssociatedMenuBehavior,
    AssociatedMenuGroup,
    AttributeRequiredLevel,
    CascadeType,
)


class RequiredLevel(MetadataBase):
    """
    Complex Property describing the Required Level of an Attribute.

    Parameters
    ----------
    value : AttributeRequiredLevel
        Enum designating the actual required level value for the Attribute.
    can_be_changed: bool
        Whether the setting can be changed.
    """

    value: AttributeRequiredLevel = AttributeRequiredLevel.NONE
    can_be_changed: bool = True

    def model_post_init(self, _: Any) -> None:
        self.managed_property_logical_name: str = "canmodifyrequirementlevelsettings"


def required_level_default() -> RequiredLevel:
    return RequiredLevel()


class LocalizedLabel(MetadataBase):
    """
    Complex property describing a Localized Label in Dataverse.
    Localized labels are associated with a Microsoft Locale ID (LCID)

    Parameters
    ----------
    label : str
        The text to be shown.
    language_code : int
        The associated language code for the specific localized label.
    """

    label: str
    language_code: int = 1033
    is_managed: bool = False

    def model_post_init(self, _: Any) -> None:
        self.odata_type: str = BASE_TYPE + "LocalizedLabel"


class Label(MetadataBase):
    """
    Complex property describing a Label in Dataverse.

    Parameters
    ----------
    localized_labels : list of `LocalizedLabels`
        The list of localized labels defined for the `Label`. Each
        `LocalizedLabel` defines the label for an associated language code.
    """

    localized_labels: Sequence[LocalizedLabel] = Field(default_factory=list)

    def model_post_init(self, _: Any) -> None:
        self.odata_type: str = BASE_TYPE + "Label"


@overload
def create_label() -> Label: ...


@overload
def create_label(*, label: str) -> Label: ...


@overload
def create_label(*, label: str, language_code: int) -> Label: ...


@overload
def create_label(*, labels: Sequence[tuple[str, int]]) -> Label: ...


def create_label(
    *,
    label: str | None = None,
    language_code: int | None = None,
    labels: Sequence[tuple[str, int]] | None = None,
) -> Label:
    """
    Creates a new `Label` instance.

    Parameters
    ----------
    label : string or tuple of string and integer
        For creating one `LocalizedLabel` within the `Label`.
    code : int
        The language code for a given `label`.
    labels: tuple of string and integer
        For specifying multiple `LocalizedLabel`s within the `Label`.
        Each tuple represents one label/language code pair.

    Returns
    -------
    `Label`
        A metadata object according to Dataverse specification.

    Raises
    ------
    ValueError: If input types are incorrect.
    AssertionError: If tuples are incorrectly typed.

    Notes
    -----
    The language codes recognized by the Dataverse organization can be retrieved
    using the `DataverseClient.get_language_codes` method. Language codes specified
    outside of the values returned by this method will be silently ignored by the API.
    """

    if language_code is None:
        language_code = 1033

    if label:
        localized_labels = [LocalizedLabel(label=label, language_code=language_code)]
    elif labels:
        localized_labels = [LocalizedLabel(label=label[0], language_code=label[1]) for label in labels]
    else:
        localized_labels = [LocalizedLabel(label="Label")]
    return Label(localized_labels=localized_labels)


class AssociatedMenuConfiguration(MetadataBase):
    """
    Complex Property for Associated Menu Config.

    Parameters
    ----------
    behavior : AssociatedMenuBehavior
        Describes the behavior of the associated menu for a one-to-many relationship.
    group: AssociatedMenuGroup
        Describes the group in which to display the associated menu for an entity relationship.
    order : int
        The order for the associated menu. Value must be higher than 10 000.
    label : Label
        The label for the associated menu.
    """

    behavior: AssociatedMenuBehavior = AssociatedMenuBehavior.USE_COLLECTION_NAME
    group: AssociatedMenuGroup = AssociatedMenuGroup.DETAILS
    order: int = 10000
    label: Label = Field(default_factory=lambda: create_label(label=""))


class CascadeConfiguration(MetadataBase):
    """
    Complex Property for Cascade Configuration.
    """

    assign: CascadeType = Field(default=CascadeType.CASCADE)
    delete: CascadeType = Field(default=CascadeType.CASCADE)
    merge: CascadeType = Field(default=CascadeType.CASCADE)
    reparent: CascadeType = Field(default=CascadeType.CASCADE)
    share: CascadeType = Field(default=CascadeType.CASCADE)
    unshare: CascadeType = Field(default=CascadeType.CASCADE)
