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
    DataverseColumn,
    DataverseEntitySchema,
    DataverseRawSchema,
)
from dataverse_api.utils import (
    assign_expected_type,
    extract_batch_response_data,
    get_val,
)


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
        data = [DataverseBatchCommand(url, "GET")]
        if self.validate:
            data.extend(
                [
                    DataverseBatchCommand(url + "/Attributes"),
                    DataverseBatchCommand(url + "/Keys"),
                    DataverseBatchCommand("organizations?$select=languagecode"),
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
        logging.info("Parsing EntitySet metadata.")
        self._parse_meta_entity()
        if self.validate:
            logging.info("Parsing Validation metadata.")
            self._parse_language_code()
            self._parse_meta_columns()
            self._parse_meta_keys()
            self._parse_relationship_metadata()
            self._parse_picklist_metadata()

    def _parse_language_code(self):
        """
        Parses the raw schema for Organizations table to assign the
        correct language code to the schema.
        """
        code = self.raw_schema.language_data["value"][0]["languagecode"]
        self.schema.language_code = code
        logging.info(f"Language code: {self.schema.language_code}")

    def _parse_meta_entity(self):
        """
        Parses the raw schema for EntitySet to assign the correct
        entity name and primary key data to schema.
        """
        self.schema.name = self.raw_schema.entity_data["EntitySetName"]
        self.schema.key = self.raw_schema.entity_data["PrimaryIdAttribute"]
        logging.info(f"EntitySetName: {self.schema.name}")
        logging.info(f"Primary Key: {self.schema.key}")

    def _parse_meta_columns(self):
        """
        Parses the raw schema for Attributes to assign the correct
        metadata per attribute into the schema.
        """
        schema_columns = dict()
        cols: list[dict] = self.raw_schema.column_data["value"]
        for col in cols:
            valid_attr = col["IsValidODataAttribute"]
            valid_create = col["IsValidForCreate"]
            valid_update = col["IsValidForUpdate"]

            if not valid_attr or (not valid_create and not valid_update):
                continue

            schema_columns[col["LogicalName"]] = DataverseColumn(
                schema_name=col["SchemaName"],
                can_create=valid_create,
                can_update=valid_update,
                attr_type=col["AttributeType"],
                data_type=assign_expected_type(col["AttributeTypeName"]["Value"]),
                max_height=col.get("MaxHeight"),
                max_length=col.get("MaxLength"),
                max_size=col.get("MaxSizeInKB"),
                max_width=col.get("MaxWidth"),
                max_value=get_val(col, "Max"),
                min_value=get_val(col, "Min"),
            )
        self.schema.columns = schema_columns

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
        """
        picklist_cols = [
            col_name
            for col_name, col in self.schema.columns.items()
            if col.attr_type == "Picklist"
        ]

        if len(picklist_cols) == 0:
            return

        # Preparing batch command to retrieve all picklists simultaneously
        batch: list[DataverseBatchCommand] = []
        for col in picklist_cols:
            meta_url = (
                f"EntityDefinitions(LogicalName='{self.logical_name}')"
                + f"/Attributes(LogicalName='{col}')"
                + "/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$select=LogicalName"
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
                    if label["LanguageCode"] == self.schema.language_code:
                        choices[label["Label"]] = option["Value"]
            self.schema.columns[col["LogicalName"]].choices = choices

    def _parse_relationship_metadata(self) -> None:
        """
        Stores a list of target Entity names in schema valid for mention in
        $expand clauses for Dataverse querying.

        The metadata returned by the OneToMany and ManyToOne API endpoints
        are the same, but the "direction" of the relationship determines
        which attribute is the valid one in a query expand clause.
        """
        valid_entities = []
        attr = "ReferencedEntityNavigationPropertyName"
        for rel in self.raw_schema.one_many_data["value"]:
            if rel["ReferencingEntity"] == "asyncoperation":
                # Expanding relationships to this Entity returns
                # a 500 response from the server, don't know why
                continue
            valid_entities.append(rel[attr])

        attr = "ReferencingEntityNavigationPropertyName"
        for rel in self.raw_schema.many_one_data["value"]:
            valid_entities.append(rel[attr])

        self.schema.relationships = valid_entities
