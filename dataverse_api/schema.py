from dataclasses import dataclass


@dataclass
class DataverseRelationships:
    """
    For describing relationships for Entity.
    """

    single_valued: list[str]
    collection_valued: list[str]
