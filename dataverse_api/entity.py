import logging
from collections.abc import Collection, Iterable, Mapping, MutableMapping, Sequence
from copy import copy
from typing import Any, Literal, overload

import requests
from narwhals.dependencies import is_into_dataframe
from narwhals.typing import IntoFrameT

from dataverse_api._api import Dataverse
from dataverse_api.errors import DataverseError, DataverseModeError
from dataverse_api.metadata.base import BASE_TYPE, MetadataDumper
from dataverse_api.metadata.complex_properties import Label
from dataverse_api.metadata.entity import get_altkey_metadata
from dataverse_api.schema import DataverseRelationships
from dataverse_api.utils.batching import (
    APICommand,
    RequestMethod,
    chunk_data,
    transform_to_batch_for_create,
    transform_to_batch_for_delete,
    transform_to_batch_for_upsert,
    transform_upsert_data,
)
from dataverse_api.utils.data import (
    convert_dataframe_to_dict,
    extract_collection_valued_relationships,
    extract_single_valued_relationships,
)


class DataverseEntity(Dataverse):
    def __init__(
        self,
        session: requests.Session,
        environment_url: str,
        logical_name: str,
    ):
        super().__init__(session=session, environment_url=environment_url)

        self.__logical_name = logical_name
        self.__supports_create_multiple = False
        self.__supports_update_multiple = False

        # Populate entity properties
        self.update_schema("all")

    @property
    def logical_name(self) -> str:
        return self.__logical_name

    @property
    def entity_set_name(self) -> str:
        return self.__entity_set_name

    @property
    def primary_id_attr(self) -> str:
        return self.__primary_id_attr

    @property
    def primary_img_attr(self) -> str | None:
        return self.__primary_img_attr

    @property
    def alternate_keys(self) -> dict[str, list[str]]:
        return self.__alternate_keys

    @property
    def supports_create_multiple(self) -> bool:
        return self.__supports_create_multiple

    @property
    def supports_update_multiple(self) -> bool:
        return self.__supports_update_multiple

    @property
    def relationships(self) -> DataverseRelationships:
        return self.__relationships

    def __get_entity_set_properties(self) -> None:
        """
        Fetch key attributes of the Entity.

          - `EntitySetName`, used as the API endpoint
          - `PrimaryIdAttribute`, the primary ID column
          - `PrimaryImageAttribute`, the primary image column (if any)

        Returns
        -------
        EntityData
            A dataclass with the three relevant attributes.
        """
        columns = ["EntitySetName", "PrimaryIdAttribute", "PrimaryImageAttribute"]
        logging.debug("Retrieving EntityDefinitions for %s", self.logical_name)
        resp = self._api_call(
            method=RequestMethod.GET,
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')",
            params={"$select": ",".join(columns)},
        ).json()

        self.__entity_set_name = resp["EntitySetName"]
        self.__primary_id_attr = resp["PrimaryIdAttribute"]
        self.__primary_img_attr = resp.get("PrimaryImageAttribute")

    def __get_entity_alternate_keys(self) -> None:
        """
        Fetch the alternate keys (if any) for the Entity.
        """
        columns = ["SchemaName", "KeyAttributes"]
        logging.debug("Retrieving alternate keys for %s", self.logical_name)
        resp = self._api_call(
            method=RequestMethod.GET,
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')/Keys",
            params={"$select": ",".join(columns)},
        ).json()["value"]

        self.__alternate_keys = {r["SchemaName"]: r["KeyAttributes"] for r in resp}

    def __get_entity_sdk_messages(self) -> None:
        """
        Fetch sdk messages to determine whether Entity supports certain actions.
        """
        create, update = "CreateMultiple", "UpdateMultiple"
        actions = [create, update]
        col = "primaryobjecttypecode"
        msg_col = "sdkmessageid/name"

        params: dict[str, str] = dict()
        params["$select"] = "sdkmessagefilterid"
        params["$expand"] = "sdkmessageid($select=name)"
        params["$filter"] = (
            f"""({" or ".join(f"{msg_col} eq '{x}'" for x in actions)}) and {col} eq '{self.logical_name}'"""
        )

        logging.debug("Retrieving SDK messages for %s", self.logical_name)
        resp = self._api_call(
            method=RequestMethod.GET,
            url="sdkmessagefilters",
            params=params,
        ).json()["value"]
        returned_actions = {row["sdkmessageid"]["name"] for row in resp}

        if create in returned_actions:
            self.__supports_create_multiple = True
        if update in returned_actions:
            self.__supports_update_multiple = True

    def __get_entity_relationships(self) -> None:
        """
        Fetch the relationships for the Entity.

        Collection-valued: 1:N relationships where this Entity is on the one-side.
        Single-valued: N:1 relationships where this Entity is on the many-side.
        """
        one_to_many = self._api_call(
            method=RequestMethod.GET,
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')/OneToManyRelationships",
        ).json()["value"]
        many_to_one = self._api_call(
            method=RequestMethod.GET,
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')/ManyToOneRelationships",
        ).json()["value"]

        collection_valued = extract_collection_valued_relationships(
            data=one_to_many, entity_logical_name=self.logical_name
        )
        single_valued = extract_single_valued_relationships(data=many_to_one)

        self.__relationships = DataverseRelationships(single_valued=single_valued, collection_valued=collection_valued)

    def update_schema(
        self, arg: Literal["all", "altkeys", "properties", "messages", "relationships"] | None = None
    ) -> None:
        """
        Update schema.
        """
        if arg == "altkeys":
            self.__get_entity_alternate_keys()
            return

        if arg == "properties":
            self.__get_entity_set_properties()
            return

        if arg == "messages":
            self.__get_entity_sdk_messages()
            return

        if arg == "relationships":
            self.__get_entity_relationships()
            return

        if arg == "all":
            self.__get_entity_alternate_keys()
            self.__get_entity_sdk_messages()
            self.__get_entity_set_properties()
            self.__get_entity_relationships()

    @overload
    def read(
        self,
        *,
        select: Collection[str] | None = None,
        top: int | None = None,
        filter: str | None = None,
        page_size: int | None = None,
        expand: str | None = None,
        order_by: str | None = None,
        return_formatted_values: bool = False,
        return_responses: Literal[False] = False,
    ) -> list[dict[str, Any]]: ...

    @overload
    def read(
        self,
        *,
        select: Collection[str] | None = None,
        top: int | None = None,
        filter: str | None = None,
        page_size: int | None = None,
        expand: str | None = None,
        order_by: str | None = None,
        return_formatted_values: bool = False,
        return_responses: Literal[True],
    ) -> list[requests.Response]: ...

    def read(
        self,
        *,
        select: Collection[str] | None = None,
        top: int | None = None,
        filter: str | None = None,
        page_size: int | None = None,
        expand: str | None = None,
        order_by: str | None = None,
        return_formatted_values: bool = False,
        return_responses: bool = False,
    ) -> list[dict[str, Any]] | list[requests.Response]:
        """
        Read data from Entity.

        Parameters
        ----------
        select : Collection[str]
            Columns to return in the query.
        top : int
            Optional limit on returned records.
        filter : str
            A fully qualified filtering string.
        expand : str
            A fully qualified expand string.
        orderby : str
            A fully qualified order_by string.
        apply : str
            A fully qualified string describing aggregation and grouping of returned records.
        page_size : int
            Limits the total number of records retrieved per API call.
        return_formatted_values : bool
            Return formatted values (e.g. for lookup, choice columns) in response.
        return_responses : bool
            Returns complete responses instead of data records.

        Returns
        -------
        list[dict[str, Any]]
            The extended "value" element for all response-JSONs from server.
        """

        additional_headers: dict[str, str] = dict()
        formatted_arg = "odata.include-annotations=OData.Community.Display.V1.FormattedValue"
        page_size_arg = f"odata.maxpagesize={page_size}"

        if page_size is not None and return_formatted_values:
            additional_headers["Prefer"] = f"{formatted_arg},{page_size_arg}"
        elif page_size is not None:
            additional_headers["Prefer"] = page_size_arg
        elif return_formatted_values:
            additional_headers["Prefer"] = formatted_arg

        params: dict[str, Any] = dict()
        if select:
            params["$select"] = ",".join(select)
        if filter:
            params["$filter"] = filter
        if top:
            params["$top"] = top
        if order_by:
            params["$orderby"] = order_by
        if expand:
            params["$expand"] = expand

        output: list[requests.Response] = list()
        url = self.entity_set_name

        # Looping through pages
        logging.debug("Fetching data for read operation on %s.", self.logical_name)
        response = self._api_call(
            method=RequestMethod.GET,
            url=url,
            headers=additional_headers,
            params=params,
        )
        output.append(response)
        while response.json().get("@odata.nextLink"):
            response = self._api_call(method=RequestMethod.GET, url=response.json()["@odata.nextLink"])
            output.append(response)

        if return_responses:
            logging.debug("Fetched all data for read operation, %d responses.", len(output))
            return output
        else:
            data_output: list[dict[str, Any]] = []
            for resp in output:
                data_output.extend(resp.json()["value"])
            logging.debug("Fetched all data for read operation, %d elements.", len(data_output))
            return data_output

    @overload
    def create(
        self,
        data: Sequence[MutableMapping[str, Any]] | IntoFrameT,
        *,
        mode: Literal["individual", "multiple"] = "individual",
        detect_duplicates: bool = False,
        return_created: bool = False,
        threading: bool = False,
    ) -> list[requests.Response]: ...

    @overload
    def create(
        self,
        data: Sequence[MutableMapping[str, Any]] | IntoFrameT,
        *,
        mode: Literal["batch"],
        detect_duplicates: bool = False,
        return_created: bool = False,
        batch_size: int | None = None,
        threading: bool = False,
    ) -> list[requests.Response]: ...

    def create(
        self,
        data: Sequence[MutableMapping[str, Any]] | IntoFrameT,
        *,
        mode: Literal["individual", "multiple", "batch"] = "individual",
        detect_duplicates: bool = False,
        return_created: bool = False,
        batch_size: int | None = None,
        threading: bool = False,
    ) -> list[requests.Response]:
        """
        Create rows in Dataverse Entity. Failures will occur if trying to insert
        a record where alternate key already exists, partial success is possible.

        data : Sequence[MutableMapping[str, Any] | IntoFrameT
            The data to create in Dataverse, JSON serializable.
        mode : Literal["individual", "multiple", "batch"]
            Whether to create rows using individual requests, `CreateMultiple` web API action
            or as batch requests using the `$batch` endpoint.
        detect_duplicates : bool
            Whether Dataverse will run duplicate detection rules upon insertion.
        return_created : bool
            Whether the returned list of Responses will contain information on
            created rows.
        batch_size : int
            Optional override if batch mode is specified, useful for tuning workload
            per batch if 429s occur.
        """
        if is_into_dataframe(data):
            data = convert_dataframe_to_dict(data)

        assert isinstance(data, Sequence)

        headers: dict[str, str] = dict()
        if detect_duplicates:
            headers["MSCRM.SuppressDuplicateDetection"] = "false"

        if return_created:
            headers["Prefer"] = "return=representation"

        length = len(data)
        if mode == "individual":
            logging.debug("%d rows to insert using individual inserts.", length)
            return self.__create_singles(headers=headers, data=data, threading=threading)

        if mode == "multiple":
            if not self.supports_create_multiple:
                raise DataverseError(f"CreateMultiple is not supported by {self.logical_name}. Use a different mode.")
            logging.debug("%d rows to insert using CreateMultiple.", length)
            return self.__create_multiple(headers=headers, data=data, threading=threading)

        if mode == "batch":
            logging.debug(
                "%d rows to insert using batch insertion.",
                length,
            )
            return self.__create_batch(data=data, batch_size=batch_size, threading=threading)

        raise DataverseModeError(mode, "individual", "multiple", "batch")

    def __create_singles(
        self,
        data: Collection[MutableMapping[str, Any]],
        headers: Mapping[str, str],
        threading: bool,
    ) -> list[requests.Response]:
        """
        Insert rows one by one using threaded API call.
        """
        calls = [
            APICommand(
                method=RequestMethod.POST,
                url=self.entity_set_name,
                headers=headers,
                json=row,
            )
            for row in data
        ]

        if threading:
            return self._threaded_call(calls=calls)
        return self._individual_call(calls=calls)

    def __create_multiple(
        self, headers: Mapping[str, str], data: Sequence[MutableMapping[str, Any]], threading: bool
    ) -> list[requests.Response]:
        """
        Insert rows by using the `CreateMultiple` Web API Action.
        """
        # Preserving input data
        data = copy(data)

        # Adding odata type to each record
        for row in data:
            row["@odata.type"] = BASE_TYPE + self.logical_name

        # Chunking the payload to suggested size
        calls = [
            APICommand(
                method=RequestMethod.POST,
                url=f"{self.entity_set_name}/{BASE_TYPE}CreateMultiple",
                headers=headers,
                json={"Targets": rows},
            )
            for rows in chunk_data(data=data, size=500)
        ]

        if threading:
            return self._threaded_call(calls)
        return self._individual_call(calls)

    def __create_batch(
        self, data: Collection[MutableMapping[str, Any]], batch_size: int | None, threading: bool
    ) -> list[requests.Response]:
        """
        Run a batch insert operation on the given data.
        """
        batch_data = transform_to_batch_for_create(url=self.entity_set_name, data=data)
        return self._batch_api_call(batch_data, batch_size=batch_size, threading=threading)

    def __delete_singles(self, data: Iterable[str], threading: bool) -> list[requests.Response]:
        calls = [
            APICommand(
                method=RequestMethod.DELETE,
                url=f"{self.entity_set_name}({id})",
            )
            for id in data
        ]

        if threading:
            return self._threaded_call(calls=calls)
        return self._individual_call(calls=calls)

    @overload
    def delete(
        self,
        *,
        mode: Literal["individual"] = "individual",
        ids: Collection[str],
    ) -> list[requests.Response]: ...

    @overload
    def delete(
        self,
        *,
        mode: Literal["individual"] = "individual",
        filter: str,
        threading: bool = False,
    ) -> list[requests.Response]: ...

    @overload
    def delete(
        self, *, mode: Literal["batch"], filter: str, batch_size: int | None = None, threading: bool = False
    ) -> list[requests.Response]: ...

    @overload
    def delete(
        self, *, mode: Literal["batch"], ids: Collection[str], batch_size: int | None = None, threading: bool = False
    ) -> list[requests.Response]: ...

    def delete(
        self,
        *,
        mode: Literal["individual", "batch"] = "individual",
        ids: Collection[str] | None = None,
        filter: str | None = None,
        batch_size: int | None = None,
        threading: bool = False,
    ) -> list[requests.Response]:
        """
        Delete rows in Entity.

        Specify either a list of ID's for deletion or a filter
        string for determining which records to delete.

        Parameters
        ----------
        mode : Literal["individual","batch"]
            Whether to delete rows using individual requests or batch requests.
        ids : Collection[str]
            List of primary IDs to delete. Takes precedence if passed.
        filter : str
            Filter statement for targeting specific records in Entity
            for deletion. Use `filter="all"` to delete all records.
        batch_size : int
            Optional override if batch mode is specified, useful for tuning workload
            per batch if 429s occur.
        """
        if ids is None and filter is None:
            raise DataverseError("Function requires either ids to delete or a string passed as filter.")

        if filter == "all":
            filter = None

        if ids is None:
            records = self.read(select=[self.primary_id_attr], filter=filter)
            ids = {row[self.primary_id_attr] for row in records}

        length = len(ids)
        logging.info("%d rows to delete.", length)
        if mode == "individual":
            logging.debug("%d rows to delete using individual deletes.", length)
            return self.__delete_singles(data=ids, threading=threading)

        if mode == "batch":
            logging.debug("%d rows to delete using batch deletes.", length)
            batch_data = transform_to_batch_for_delete(url=self.entity_set_name, data=ids)
            return self._batch_api_call(batch_data, batch_size=batch_size or 100, timeout=120, threading=threading)

        raise DataverseModeError(mode, "individual", "batch")

    def __delete_column_singles(self, data: Iterable[str], column: str, threading: bool) -> list[requests.Response]:
        """
        Delete row column value by individual requests.
        """
        calls = [
            APICommand(
                method=RequestMethod.DELETE,
                url=f"{self.entity_set_name}({id})/{column}",
            )
            for id in data
        ]
        if threading:
            return self._threaded_call(calls=calls)
        return self._individual_call(calls=calls)

    @overload
    def delete_columns(
        self,
        columns: Collection[str],
        *,
        mode: Literal["individual", "batch"] = "individual",
        ids: Collection[str],
        threading: bool,
    ) -> list[requests.Response]: ...

    @overload
    def delete_columns(
        self,
        columns: Collection[str],
        *,
        mode: Literal["individual", "batch"] = "individual",
        filter: str,
        threading: bool,
    ) -> list[requests.Response]: ...

    def delete_columns(
        self,
        columns: Collection[str],
        *,
        ids: Collection[str] | None = None,
        filter: str | None = None,
        mode: Literal["individual", "batch"] = "individual",
        threading: bool = False,
    ) -> list[requests.Response]:
        """
        Delete values in specific column for rows in Entity.

        Specify either a list of ID's for deletion or a filter
        string for determining which records to delete.

        Parameters
        ----------
        column : Collection[str]
            The columns in Dataverse to target for deletion.
        mode : Literal["individual", "batch"]
            Whether to delete columns using individual requests or batch requests.
        ids : Collection[str]
            List of primary IDs to delete. Takes precedence if passed.
        filter : str
            Filter statement for targeting specific records in Entity for deletion.
            Use `filter="all"` to delete all records.
        """
        if ids is None and filter is None:
            raise DataverseError("Function requires either ids to delete or a string passed as filter.")

        if filter == "all":
            filter = None

        if ids is None:
            records = self.read(select=[self.primary_id_attr], filter=filter)
            ids = {row[self.primary_id_attr] for row in records}

        length = len(ids) * len(columns)  # Total number of deletion requests
        output: list[requests.Response] = []
        if mode == "individual":
            logging.debug("%d properties to delete. Using individual deletes.", length)
            for col in columns:
                output.extend(self.__delete_column_singles(data=ids, column=col, threading=threading))
            return output

        if mode == "batch":
            logging.debug("%d properties to delete. Using batch deletes.", length)
            for col in columns:
                batch_data = transform_to_batch_for_delete(url=self.entity_set_name, data=ids, column=col)
                output.extend(self._batch_api_call(batch_data, threading=threading))
            return output

        raise DataverseModeError(mode, "individual", "batch")

    def __upsert_singles(
        self, data: Collection[Mapping[str, Any]], keys: Iterable[str], is_primary_id: bool, threading: bool
    ) -> list[requests.Response]:
        """
        Upsert row by individual requests.
        """
        calls = [
            APICommand(
                method=RequestMethod.PATCH,
                url=f"{self.entity_set_name}({key})",
                json=payload,
            )
            for key, payload in transform_upsert_data(data, keys, is_primary_id)
        ]
        if threading:
            return self._threaded_call(calls=calls)
        return self._individual_call(calls=calls)

    def upsert(
        self,
        data: Collection[MutableMapping[str, Any]] | IntoFrameT,
        *,
        mode: Literal["individual", "batch"] = "individual",
        altkey_name: str | None = None,
        threading: bool = False,
    ) -> list[requests.Response]:
        """
        Upsert data into Entity.

        Parameters
        ----------
        data : Collection[MutableMapping[str, Any]] | IntoFrameT
            The data to upsert.
        mode : Literal["individual", "batch"]
            Whether to upsert data using individual requests or batch requests.
        altkey_name : str
            The alternate key to use as ID (if any).
            Will assume entity primary ID attribute if not given.
        """

        if altkey_name is not None:
            try:
                key_columns = self.alternate_keys[altkey_name]
            except KeyError:
                raise DataverseError(f"Altkey '{altkey_name}' is not valid for Entity '{self.logical_name}'.")
            is_primary_id = False
        else:
            key_columns = [self.primary_id_attr]
            is_primary_id = True

        if is_into_dataframe(data):
            data = convert_dataframe_to_dict(data)

        assert isinstance(data, Collection)

        if mode == "individual":
            logging.debug("%d rows to upsert. Using individual upserts.", len(data))
            return self.__upsert_singles(data=data, keys=key_columns, is_primary_id=is_primary_id, threading=threading)

        if mode == "batch":
            logging.debug("%d rows to upsert. Using batch upserts.", len(data))
            batch_data = transform_to_batch_for_upsert(
                url=self.entity_set_name,
                data=data,
                keys=key_columns,
                is_primary_id=is_primary_id,
            )
            return self._batch_api_call(batch_data, threading=threading)

        raise DataverseModeError(mode, "individual", "batch")

    def add_attribute(
        self,
        attribute: MetadataDumper,
        solution_name: str | None = None,
        return_representation: bool = False,
    ) -> requests.Response:
        """
        Add attribute to Entity.

        Parameters
        ----------
        attribute : MetadataDumper
            Dumpable metadata for new Attribute.
        solution_name : str
            Unique name for solution attribute is part of.
        return_representation : bool
            Whether to include the metadata representation after
            creation in the response from server.

        Returns
        -------
        requests.Response
            The response from the server.
        """

        headers: dict[str, str] = dict()

        if solution_name is not None:
            headers["MSCRM.SolutionUniqueName"] = solution_name

        if return_representation:
            headers["Prefer"] = "return=representation"

        return self._api_call(
            method=RequestMethod.POST,
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')/Attributes",
            headers=headers,
            json=attribute.dump_to_dataverse(),
        )

    @overload
    def remove_attribute(self, *, attribute_id: str) -> requests.Response: ...

    @overload
    def remove_attribute(self, *, logical_name: str) -> requests.Response: ...

    def remove_attribute(
        self,
        *,
        attribute_id: str | None = None,
        logical_name: str | None = None,
    ) -> requests.Response:
        """
        Remove Attribute from Entity.

        Parameters
        ----------
        attribute_id : str
            GUID of Attribute.
        logical_name : str
            LogicalName of Attribute.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        if attribute_id is None and logical_name is None:
            raise DataverseError("Supply either 'id' or 'logical_name' kwarg.")

        if attribute_id:
            return self._api_call(
                method=RequestMethod.DELETE,
                url=f"EntityDefinitions(LogicalName='{self.logical_name}')/Attributes({attribute_id})",
            )

        return self._api_call(
            method=RequestMethod.DELETE,
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')/Attributes(LogicalName='{logical_name}')",
        )

    def add_alternate_key(
        self,
        schema_name: str,
        display_name: str | Label,
        key_attributes: Collection[str],
        return_representation: bool = False,
    ) -> requests.Response:
        """
        Add an alternate key to Entity.

        Parameters
        ----------
        alternate_key : MetadataDumper
            Dumpable metadata for new Alternate Key.
        """
        headers: dict[str, str] = dict()
        if return_representation:
            headers["Prefer"] = "return_representation"

        key = get_altkey_metadata(
            schema_name=schema_name,
            display_name=display_name,
            key_attributes=key_attributes,
        )

        resp = self._api_call(
            method=RequestMethod.POST,
            url=f"EntityMetadata(LogicalName='{self.logical_name}')/Keys",
            headers=headers,
            json=key.dump_to_dataverse(),
        )

        self.update_schema("altkeys")

        return resp

    @overload
    def remove_alternate_key(self, *, altkey_id: str) -> requests.Response: ...

    @overload
    def remove_alternate_key(self, *, logical_name: str) -> requests.Response: ...

    def remove_alternate_key(
        self,
        *,
        altkey_id: str | None = None,
        logical_name: str | None = None,
    ) -> requests.Response:
        """
        Remove Alternate Key from Entity.

        Parameters
        ----------
        attribute_id : str
            GUID of Alternate Key.
        logical_name : str
            LogicalName of Alternate Key.

        Returns
        -------
        requests.Response
            The response from the server.
        """
        if altkey_id is None and logical_name is None:
            raise DataverseError("Supply either 'id' or 'logical_name' kwarg.")

        if altkey_id:
            resp = self._api_call(
                method=RequestMethod.DELETE,
                url=f"EntityDefinitions(LogicalName='{self.logical_name}')/Attributes({altkey_id})",
            )

        resp = self._api_call(
            method=RequestMethod.DELETE,
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')/Attributes(LogicalName='{logical_name}')",
        )

        self.update_schema("altkeys")

        return resp
