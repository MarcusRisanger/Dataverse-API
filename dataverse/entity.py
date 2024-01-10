import logging
from collections.abc import Collection, Iterable, Mapping, MutableMapping, Sequence
from copy import copy
from typing import Any, overload

import pandas as pd
import requests

from dataverse._api import Dataverse
from dataverse.errors import DataverseError
from dataverse.metadata.base import BASE_TYPE
from dataverse.utils.batching import (
    RequestMethod,
    ThreadCommand,
    chunk_data,
    transform_to_batch_data_for_create,
    transform_to_batch_for_delete,
)
from dataverse.utils.dataframes import convert_dataframe_to_dict


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
        self.__get_entity_set_properties()
        self.__get_entity_alternate_keys()
        self.__get_entity_sdk_messages()

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
        detect_duplicates: bool = False,
    ) -> list[requests.Response]:
        """
        Create rows in Dataverse Entity. Failures will occur if trying to insert
        a record where alternate key already exists, partial success is possible.

        data : sequence of Serializable JSON dicts or `pandas.DataFrame`.
            The data to create in Dataverse.
        detect_duplicates : bool
            Whether Dataverse will run duplicate detection rules upon insertion.
        """
        # TODO: Return data option?

        if isinstance(data, pd.DataFrame):
            data = convert_dataframe_to_dict(data)

        headers: dict[str, str] = dict()
        if detect_duplicates:
            headers["MSCRM.SuppressDuplicateDetection"] = "false"

        length = len(data)
        if length < 10:
            logging.debug("%d rows to insert. Using single inserts.", length)
            resp = self.__create_singles(headers=headers, data=data)
        elif self.supports_create_multiple:
            logging.debug("%d rows to insert. Using CreateMultiple.", length)
            resp = self.__create_multiple(headers=headers, data=data)
        else:
            logging.debug(
                "%d rows to insert. CreateMultiple not supported. Inserting batch.",
                length,
            )
            resp = self.__create_batch(data=data)

        return resp

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
        batch_data = transform_to_batch_data_for_create(url=self.entity_set_name, data=data)
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
    def delete(self, *, ids: Collection[str]) -> list[requests.Response]:
        ...

    @overload
    def delete(self, *, filter: str) -> list[requests.Response]:
        ...

    def delete(self, *, ids: Collection[str] | None = None, filter: str | None = None) -> list[requests.Response]:
        """
        Delete rows in Entity.

        Specify either a

        Parameters
        ----------
        ids : list[str]
            List of primary IDs to delete. Takes precedence if passed.
        filter : str
            Filter statement for targeting specific records in Entity for deletion.
            Use `filter="all"` to delete all records.
        """
        if ids is None and filter is None:
            raise DataverseError("Function requires either ids to delete or a string passed as filter.")

        filter = None if filter == "all" else filter

        if ids is None:
            records = self.read(select=[self.primary_id_attr], filter=filter)
            ids = {row[self.primary_id_attr] for row in records}

        length = len(ids)
        if length < 10:
            logging.debug("%d rows to delete. Using single deletes.", length)
            resp = self.__delete_singles(data=ids)
        else:
            logging.debug("%d rows to delete. Using batch deletes.", length)
            batch_data = transform_to_batch_for_delete(url=self.entity_set_name, data=ids)
            resp = self._batch_api_call(batch_data)

        return resp
