"""
Some helper definitions for key actions in Dataverse.
These are not defined in Dataverse but are used to abstract some use-cases
for this framework.
"""
from dataclasses import dataclass, field


@dataclass(slots=True)
class Publisher:
    """
    A class to define a new Dataverse Publisher.

    Parameters
    ----------
    name : str
        The display name of the Publisher.
    unique_name : str
        The unique name of the Publisher.
    description : str
    prefix: str
        The prefix of Entities and Attributes associated with the Publisher.
    option_prefix: int
        The prefix of options for Choices associated with the Publisher.
    """

    name: str
    unique_name: str
    description: str
    prefix: str
    option_prefix: int

    def __call__(self) -> dict[str, int | str]:
        return {
            "friendlyname": self.name,
            "uniquename": self.unique_name,
            "description": self.description,
            "customizationprefix": self.prefix,
            "customizationoptionvalueprefix": self.option_prefix,
        }


@dataclass(slots=True)
class Solution:
    """
    A class to define a new Dataverse Solution.

    Parameters
    ----------
    name : str
        The display name of the Solution.
    unique_name : str
        The unique name of the Solution.
    description : str
        A description of the Solution.
    publisher_guid : str
        The Dataverse GUID of the associated Publisher.
    """

    name: str
    unique_name: str
    description: str
    publisher_guid: str
    version: str = field(init=False, default="1.0.0.0")

    def __call__(self) -> dict[str, int | str]:
        return {
            "friendlyname": self.name,
            "uniquename": self.unique_name,
            "description": self.description,
            "version": self.version,
            "publisher@odata.bind": f"publishers({self.publisher_guid})",
        }
