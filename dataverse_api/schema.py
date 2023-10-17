"""
Contains the DataverseSchema class, used for retrieving and
processing the Entity schema from Dataverse.

Author: Marcus Risanger
"""

import logging

from dataverse_api._api import DataverseAPI
from dataverse_api.dataclasses import (
    DataverseAuth,
    DataverseBatchCommand,
    DataverseEntityAttribute,
    DataverseEntityData,
    DataverseEntitySchema,
    DataverseRawSchema,
    DataverseRelationships,
)
from dataverse_api.utils import (
    assign_expected_type,
    extract_batch_response_data,
    get_val,
)

log = logging.getLogger("dataverse-api")


class DataverseSchema(DataverseAPI):
    """
    This class is for retrieving necessary information to validate
    data payloads to Dataverse. The processed schema can be used to
    check column names, key column combinations in payload, adherence
    to min/max numerical and datetime values, max text lengths, image
    dimensions and size, Picklist column choices, and related Entities.
    """

    def __init__(
        self,
        auth: DataverseAuth,
        logical_name: str,
        validate: bool = False,
    ):
        super().__init__(auth=auth)
        self.validate = validate
        self.logical_name = logical_name
        self.schema = DataverseEntitySchema()
        self.raw_schema: DataverseRawSchema = self._get_entity_metadata()

    def fetch(self) -> DataverseEntitySchema:
        """
        Returns a readily processed Entity schema.
        """
        self._parse_all_metadata()
        return self.schema

    def _get_entity_metadata(self) -> DataverseRawSchema:
        """
        Required for initialization using logical name of Dataverse Entity.
        Fetches entity metadata and column/altkey metadata if validation is True.

        Note: The order of DataverseBatchCommands is important due to unpacking
        into the `DataverseRawSchema` object on return.
        """
        url = f"EntityDefinitions(LogicalName='{self.logical_name}')"
        organization_columns = [
            "languagecode",
            "blockedattachments",
        ]
        org_cols = ",".join(organization_columns)

        data = [
            DataverseBatchCommand(url),
            DataverseBatchCommand(f"organizations?$select={org_cols}"),
        ]
        if self.validate:
            data.extend(
                [
                    DataverseBatchCommand(url + "/Attributes"),
                    DataverseBatchCommand(url + "/Keys"),
                    DataverseBatchCommand(url + "/OneToManyRelationships"),
                    DataverseBatchCommand(url + "/ManyToOneRelationships"),
                ]
            )
        response = self._batch_operation(data)
        response_data = extract_batch_response_data(response)
        return DataverseRawSchema(*response_data)

    def _parse_all_metadata(self) -> None:
        """
        Parses the initial raw entity data into the appropriate schema.
        """
        log.debug("Parsing EntitySet metadata.")
        self._parse_meta_entity()
        self._parse_organization_info()
        if self.validate:
            log.debug("Parsing Validation metadata.")
            self._parse_meta_columns()
            self._parse_meta_keys()
            self._parse_relationship_metadata()
            self._parse_picklist_metadata()
        log.debug("Metadata parsing complete.")

    def _parse_meta_entity(self):
        """
        Parses the raw schema for EntitySet to assign the correct
        entity name and primary key data to schema.
        """
        entity_data = DataverseEntityData(
            name=self.raw_schema.entity_data["EntitySetName"],
            primary_attr=self.raw_schema.entity_data["PrimaryIdAttribute"],
            primary_img=self.raw_schema.entity_data["PrimaryImageAttribute"],
        )
        self.schema.entity = entity_data
        log.info(f"EntitySetName: {self.schema.entity.name}")

    def _parse_organization_info(self):
        """
        Parses the raw schema for Organizations table to assign the
        correct language code to the schema.
        """
        code: str = self.raw_schema.organization_data["value"][0]["languagecode"]
        self.schema.entity.language_code = code

        exts: str = self.raw_schema.organization_data["value"][0]["blockedattachments"]
        self.schema.entity.illegal_extensions = exts.split(";")

    def _parse_meta_columns(self):
        """
        Parses the raw schema for Attributes to assign the correct
        metadata per attribute into the schema.
        """
        attribute_schema = dict()
        cols: list[dict] = self.raw_schema.attribute_data["value"]
        for col in cols:
            logical_name = col["LogicalName"]
            valid_attr = col["IsValidODataAttribute"]
            valid_create = col["IsValidForCreate"]
            valid_update = col["IsValidForUpdate"]
            attr_type = col["AttributeTypeName"]["Value"]

            if (
                not valid_attr
                or (not valid_create and not valid_update)
                and attr_type != "FileType"
            ):
                continue

            attribute_schema[logical_name] = DataverseEntityAttribute(
                schema_name=col["SchemaName"],
                can_create=valid_create,
                can_update=valid_update,
                attr_type=attr_type,
                data_type=assign_expected_type(attr_type),
                max_height=col.get("MaxHeight"),
                max_length=col.get("MaxLength"),
                max_size_kb=col.get("MaxSizeInKB"),
                max_width=col.get("MaxWidth"),
                max_value=get_val(col, "Max"),
                min_value=get_val(col, "Min"),
            )

        self.schema.attributes = attribute_schema

    def _parse_meta_keys(self) -> None:
        """
        Parses the raw schema for Keys to assign the correct
        sets of key column combinations to schema.
        """
        keys: list[set] = list()
        altkeys: list[dict] = self.raw_schema.altkey_data["value"]
        for key in altkeys:
            keys.append(set(key["KeyAttributes"]))  # KeyAttributes is a list
        self.schema.altkeys = keys

    def _get_picklist_metadata(self) -> None:
        """
        Looks for Picklist columns in the schema and fetches the
        related picklist choice information. Stores choice information
        directly in the existing column schema.

        TODO: Add stuff like STATECODE and multi-column picklists
        """
        meta_types = {
            "PicklistType": "PicklistAttributeMetadata",
            "MultiSelectPicklistType": "MultiSelectPicklistAttributeMetadata",
            "StateType": "StateAttributeMetadata",
            "StatusType": "StatusAttributeMetadata",
        }

        picklist_attrs: list[tuple[str, str]] = list()
        for attr_name, attr in self.schema.attributes.items():
            if attr.attr_type in meta_types:
                picklist_attrs.append((attr_name, attr.attr_type))

        if len(picklist_attrs) == 0:
            return

        # Preparing batch command to retrieve all picklists simultaneously
        batch: list[DataverseBatchCommand] = []
        for attr_name, attr_type in picklist_attrs:
            meta_url = (
                f"EntityDefinitions(LogicalName='{self.logical_name}')"
                + f"/Attributes(LogicalName='{attr_name}')"
                + f"/Microsoft.Dynamics.CRM.{meta_types[attr_type]}?$select=LogicalName"
                + "&$expand=OptionSet($select=Options),GlobalOptionSet($select=Options)"
            )
            batch.append(DataverseBatchCommand(meta_url))

        metadata = self._batch_operation(batch)

        self.raw_schema.choice_data = extract_batch_response_data(metadata)

    def _parse_picklist_metadata(self) -> None:
        """
        Parses the valid labels and related values for all Picklist columns
        for the given Entity.
        """
        if self.raw_schema.choice_data is None:
            self._get_picklist_metadata()

        for col in self.raw_schema.choice_data:
            choices = dict()
            for option in col["OptionSet"]["Options"]:
                for label in option["Label"]["LocalizedLabels"]:
                    if label["LanguageCode"] == self.schema.entity.language_code:
                        choices[label["Label"]] = option["Value"]
            self.schema.attributes[col["LogicalName"]].choices = choices

    def _parse_relationship_metadata(self) -> None:
        """
        Stores a list of target Entity names in schema valid for mention in
        $expand clauses for Dataverse querying.

        The metadata returned by the OneToMany and ManyToOne API endpoints
        are the same, but the "direction" of the relationship determines
        which attribute is the valid one in a query expand clause.

        Collection-valued attributes point to the many-side of a relationship
        these are available for *deep inserts*, creating rows in the parent and
        child tables simultaneously.

        Single-valued attributes point to the one-side of a relationship
        these are available for *binding* (associating) rows in current
        entity against a specific parent entity record.

        Both single-valued and collection-valued attributes can be used
        in expand-clauses in a query.
        """

        collection_valued = []
        attr = "ReferencedEntityNavigationPropertyName"
        for rel in self.raw_schema.one_many_data["value"]:
            if rel["ReferencingEntity"] == "asyncoperation":
                # Expanding relationships to this Entity returns
                # a 500 response from the server, don't know why
                continue
            collection_valued.append(rel[attr])

        single_valued = []
        attr = "ReferencingEntityNavigationPropertyName"
        for rel in self.raw_schema.many_one_data["value"]:
            single_valued.append(rel[attr])

        self.schema.relationships = DataverseRelationships(
            single_valued=single_valued, collection_valued=collection_valued
        )
