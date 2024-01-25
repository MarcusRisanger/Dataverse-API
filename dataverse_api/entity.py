import logging
from collections.abc import Collection, Iterable, Mapping, MutableMapping, Sequence
from copy import copy
from typing import Any, Literal, overload

import pandas as pd
import requests

from dataverse_api._api import Dataverse
from dataverse_api.errors import DataverseError, DataverseModeError
from dataverse_api.metadata.base import BASE_TYPE, MetadataDumper
from dataverse_api.metadata.complex_properties import Label
from dataverse_api.metadata.entity import get_altkey_metadata
from dataverse_api.utils.batching import (
    RequestMethod,
    ThreadCommand,
    chunk_data,
    transform_to_batch_for_create,
    transform_to_batch_for_delete,
    transform_to_batch_for_upsert,
    transform_upsert_data,
)
from dataverse_api.utils.data import convert_dataframe_to_dict


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
        self.update_schema()

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
        Fetch sdk messages to determine whether entity supports certain actions.
        """
        create, update = "CreateMultiple", "UpdateMultiple"
        actions = [create, update]
        col = "primaryobjecttypecode"
        msg_col = "sdkmessageid/name"

        params: dict[str, str] = dict()
        params["$select"] = "sdkmessagefilterid"
        params["$expand"] = "sdkmessageid($select=name)"
        params[
            "$filter"
        ] = f"""({' or '.join(f"{msg_col} eq '{x}'" for x in actions)}) and {col} eq '{self.logical_name}'"""

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

    def update_schema(self, arg: Literal["altkeys", "properties", "messages"] | None = None) -> None:
        """
        Update schema.
        """
        if arg == "altkeys":
            self.__get_entity_alternate_keys()
            return

        self.__get_entity_alternate_keys()
        self.__get_entity_sdk_messages()
        self.__get_entity_set_properties()

    def read(
        self,
        *,
        select: list[str] | None = None,
        filter: str | None = None,
        top: int | None = None,
        page_size: int | None = None,
        expand: str | None = None,
        order_by: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Read data from Entity.

        Optional querying keyword args:
          - select: A single column or list of columns to return from the
            current entity.
          - filter: A fully qualified filtering string.
          - expand: A fully qualified expand string.
          - orderby: A fully qualified order_by string.
          - top: Optional limit on returned records.
          - apply: A fully qualified string describing aggregation
            and grouping of returned records.
          - page_size: Limits the total number of records
            retrieved per API call.
        """

        additional_headers: dict[str, str] = dict()
        if page_size is not None:
            additional_headers["Prefer"] = f"odata.maxpagesize={page_size}"

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

        output: list[dict[str, Any]] = list()
        url = self.entity_set_name

        # Looping through pages
        logging.debug("Fetching data for read operation on %s.", self.logical_name)
        response = self._api_call(
            method=RequestMethod.GET,
            url=url,
            headers=additional_headers,
            params=params,
        ).json()
        output.extend(response["value"])
        while response.get("@odata.nextLink"):
            response = self._api_call(method=RequestMethod.GET, url=response["@odata.nextLink"]).json()
            output.extend(response["value"])

        logging.debug("Fetched all data for read operation, %d elements.", len(output))
        return output

    def create(
        self,
        data: Sequence[MutableMapping[str, Any]] | pd.DataFrame,
        *,
        mode: Literal["individual", "multiple", "batch"] = "individual",
        detect_duplicates: bool = False,
        return_created: bool = False,
    ) -> list[requests.Response]:
        """
        Create rows in Dataverse Entity. Failures will occur if trying to insert
        a record where alternate key already exists, partial success is possible.

        data : sequence of Serializable JSON dicts or `pandas.DataFrame`.
            The data to create in Dataverse.
        mode : Literal["individual", "multiple", "batch"]
            Whether to create rows using single requests, `CreateMultiple` action or as
            batch requests.
        detect_duplicates : bool
            Whether Dataverse will run duplicate detection rules upon insertion.
        return_created : bool
            Whether the returned list of Responses will contain information on
            created rows.
        """
        # TODO: Return data option?

        if isinstance(data, pd.DataFrame):
            data = convert_dataframe_to_dict(data)

        headers: dict[str, str] = dict()
        if detect_duplicates:
            headers["MSCRM.SuppressDuplicateDetection"] = "false"

        if return_created:
            headers["Prefer"] = "return=representation"

        length = len(data)
        if mode == "individual":
            logging.debug("%d rows to insert using individual inserts.", length)
            return self.__create_singles(headers=headers, data=data)

        if mode == "multiple":
            if not self.supports_create_multiple:
                raise DataverseError(f"CreateMultiple is not supported by {self.logical_name}. Use a different mode.")
            logging.debug("%d rows to insert using CreateMultiple.", length)
            return self.__create_multiple(headers=headers, data=data)

        if mode == "batch":
            logging.debug(
                "%d rows to insert using batch insertion.",
                length,
            )
            return self.__create_batch(data=data)

        raise DataverseModeError(mode, "individual", "multiple", "batch")

    def __create_singles(
        self,
        data: Sequence[MutableMapping[str, Any]],
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> list[requests.Response]:
        """
        Insert rows one by one using threaded API call.
        """
        calls = [
            ThreadCommand(
                method=RequestMethod.POST,
                url=self.entity_set_name,
                headers=headers,
                params=params,
                json=row,
            )
            for row in data
        ]

        return self._threaded_call(calls=calls)

    def __create_multiple(
        self, headers: Mapping[str, str], data: Sequence[MutableMapping[str, Any]]
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
            ThreadCommand(
                method=RequestMethod.POST,
                url=f"{self.entity_set_name}/{BASE_TYPE}CreateMultiple",
                headers=headers,
                json={"Targets": rows},
            )
            for rows in chunk_data(data=data, size=500)
        ]

        # Threading the write operation
        return self._threaded_call(calls)

    def __create_batch(self, data: Sequence[MutableMapping[str, Any]]) -> list[requests.Response]:
        """
        Run a batch insert operation on the given data.
        """
        batch_data = transform_to_batch_for_create(url=self.entity_set_name, data=data)
        return self._batch_api_call(batch_data)

    def __delete_singles(self, data: Iterable[str]) -> list[requests.Response]:
        calls = [
            ThreadCommand(
                method=RequestMethod.DELETE,
                url=f"{self.entity_set_name}({id})",
            )
            for id in data
        ]

        return self._threaded_call(calls=calls)

    @overload
    def delete(self, *, mode: Literal["individual", "batch"], ids: Collection[str]) -> list[requests.Response]:
        ...

    @overload
    def delete(self, *, mode: Literal["individual", "batch"], filter: str) -> list[requests.Response]:
        ...

    @overload
    def delete(self, *, mode: Literal["batch"], batch_size: int, filter: str) -> list[requests.Response]:
        ...

    @overload
    def delete(self, *, mode: Literal["batch"], batch_size: int, ids: Collection[str]) -> list[requests.Response]:
        ...

    def delete(
        self,
        *,
        mode: Literal["individual", "batch"] = "individual",
        ids: Collection[str] | None = None,
        filter: str | None = None,
        batch_size: int | None = None,
    ) -> list[requests.Response]:
        """
        Delete rows in Entity.

        Specify either a list of ID's for deletion or a filter
        string for determining which records to delete.

        Parameters
        ----------
        mode : Literal["individual","batch"]
            Whether to delete rows using single requests or batch requests.
        ids : list[str]
            List of primary IDs to delete. Takes precedence if passed.
        filter : str
            Filter statement for targeting specific records in Entity
            for deletion. Use `filter="all"` to delete all records.
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
            return self.__delete_singles(data=ids)

        if mode == "batch":
            logging.debug("%d rows to delete using batch deletes.", length)
            batch_data = transform_to_batch_for_delete(url=self.entity_set_name, data=ids)
            return self._batch_api_call(batch_data, batch_size=batch_size or 100, timeout=120)

        raise DataverseModeError(mode, "individual", "batch")

    def __delete_column_singles(self, data: Iterable[str], column: str) -> list[requests.Response]:
        """
        Delete row column value by individual requests.
        """
        calls = [
            ThreadCommand(
                method=RequestMethod.DELETE,
                url=f"{self.entity_set_name}({id})/{column}",
            )
            for id in data
        ]
        return self._threaded_call(calls=calls)

    @overload
    def delete_columns(
        self,
        columns: Collection[str],
        *,
        mode: Literal["individual", "batch"],
        ids: Collection[str],
    ) -> list[requests.Response]:
        ...

    @overload
    def delete_columns(
        self,
        columns: Collection[str],
        *,
        mode: Literal["individual", "batch"],
        filter: str,
    ) -> list[requests.Response]:
        ...

    def delete_columns(
        self,
        columns: Collection[str],
        *,
        mode: Literal["individual", "batch"] = "individual",
        ids: Collection[str] | None = None,
        filter: str | None = None,
    ) -> list[requests.Response]:
        """
        Delete values in specific column for rows in Entity.

        Specify either a list of ID's for deletion or a filter
        string for determining which records to delete.

        Parameters
        ----------
        column : collection of str
            The columns in Dataverse to target for deletion.
        mode : Literal["individual", "batch"]
            Whether to delete columns using single requests or batch requests.
        ids : collection of str
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
            logging.debug("%d properties to delete. Using single deletes.", length)
            for col in columns:
                output.extend(self.__delete_column_singles(data=ids, column=col))
            return output

        if mode == "batch":
            logging.debug("%d properties to delete. Using batch deletes.", length)
            for col in columns:
                batch_data = transform_to_batch_for_delete(url=self.entity_set_name, data=ids, column=col)
                output.extend(self._batch_api_call(batch_data))
            return output

        raise DataverseModeError(mode, "individual", "batch")

    def __upsert_singles(
        self,
        data: Collection[Mapping[str, Any]],
        keys: Iterable[str],
        is_primary_id: bool,
    ) -> list[requests.Response]:
        """
        Upsert row by individual requests.
        """
        calls = [
            ThreadCommand(
                method=RequestMethod.PATCH,
                url=f"{self.entity_set_name}({key})",
                json=payload,
            )
            for key, payload in transform_upsert_data(data, keys, is_primary_id)
        ]
        return self._threaded_call(calls=calls)

    def upsert(
        self,
        data: Collection[MutableMapping[str, Any]] | pd.DataFrame,
        *,
        mode: Literal["individual", "batch"] = "individual",
        altkey_name: str | None = None,
    ) -> list[requests.Response]:
        """
        Upsert data into Entity.

        Parameters
        ----------
        data : collection of mutablemappings or dataframe
            The data to upsert.
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

        if isinstance(data, pd.DataFrame):
            data = convert_dataframe_to_dict(data)

        if mode == "individual":
            logging.debug("%d rows to upsert. Using individual upserts.", len(data))
            return self.__upsert_singles(data=data, keys=key_columns, is_primary_id=is_primary_id)

        if mode == "batch":
            logging.debug("%d rows to upsert. Using batch upserts.", len(data))
            batch_data = transform_to_batch_for_upsert(
                url=self.entity_set_name,
                data=data,
                keys=key_columns,
                is_primary_id=is_primary_id,
            )
            return self._batch_api_call(batch_data)

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
    def remove_attribute(self, *, attribute_id: str) -> requests.Response:
        ...

    @overload
    def remove_attribute(self, *, logical_name: str) -> requests.Response:
        ...

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
        key_attributes: Sequence[str],
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
    def remove_alternate_key(self, *, altkey_id: str) -> requests.Response:
        ...

    @overload
    def remove_alternate_key(self, *, logical_name: str) -> requests.Response:
        ...

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
