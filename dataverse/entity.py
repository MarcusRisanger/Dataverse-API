import logging
from collections.abc import MutableMapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import copy
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import requests

from dataverse._api import Dataverse
from dataverse.metadata.base import BASE_TYPE
from dataverse.utils.batching import BatchMode, chunk_data, transform_to_batch_data
from dataverse.utils.dataframes import convert_dataframe_to_dict


@dataclass(slots=True)
class EntityData:
    entity_set_name: str
    primary_id_attr: str
    primary_img_attr: str | None = field(default=None)


class DataverseEntity(Dataverse):
    def __init__(self, session: requests.Session, environment_url: str, logical_name: str):
        super().__init__(session=session, environment_url=environment_url)

        self.__logical_name = logical_name

        # Populate entity properties
        entity_data = self.__get_entity_set_properties()
        self.__entity_set_name = entity_data.entity_set_name
        self.__primary_id_attr = entity_data.primary_id_attr
        self.__primary_img_attr = entity_data.primary_img_attr
        self.__alternate_keys = self.__get_entity_alternate_keys()

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

    def __get_entity_set_properties(self) -> EntityData:
        """
        To fetch the some key attributes of the Entity.

          - `EntitySetName`, used as the API endpoint
          - `PrimaryIdAttribute`, the primary ID column
          - `PrimaryImageAttribute`, the primary image column (if any)

        Returns
        -------
        EntityData
            A dataclass with the three relevant attributes.
        """
        columns = ["EntitySetName", "PrimaryIdAttribute", "PrimaryImageAttribute"]
        resp: dict[str, Any] = self._api_call(
            method="GET",
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')",
            params={"$select": ",".join(columns)},
        ).json()

        return EntityData(
            entity_set_name=resp["EntitySetName"],
            primary_id_attr=resp["PrimaryIdAttribute"],
            primary_img_attr=resp.get("PrimaryImageAttribute"),
        )

    def __get_entity_alternate_keys(self) -> dict[str, list[str]]:
        """
        To fetch the alternate keys (if any) for the Entity.

        Returns
        -------
        dict
            A dictionary with alternate key schema names and
            related key attributes per key.
        """
        columns = ["SchemaName", "KeyAttributes"]
        resp = self._api_call(
            method="GET",
            url=f"EntityDefinitions(LogicalName='{self.logical_name}')/Keys",
            params={"$select": ",".join(columns)},
        ).json()["value"]

        return {r["SchemaName"]: r["KeyAttributes"] for r in resp}

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
        Reads data from Entity.

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

        additional_headers = dict()
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
            params["$apply"] = order_by
        if expand:
            params["$expand"] = expand

        output = []
        url = self.entity_set_name

        # Looping through pages
        while True:
            response = self._api_call(
                method="GET",
                url=url,
                headers=additional_headers,
                params=params,
            ).json()
            output.extend(response["value"])
            next_link = response.get("@odata.nextLink")
            if next_link is None:
                break
            url = next_link

        return output

    def insert(self, data: Sequence[MutableMapping[str, Any]] | pd.DataFrame) -> list[requests.Response]:
        """
        Inserts data to Dataverse Entity.

        data : Serializable JSON dict or `pandas.DataFrame`.
            The data to insert to Dataverse.
        """
        if isinstance(data, pd.DataFrame):
            data = convert_dataframe_to_dict(data)

        length = len(data)
        if length < 10:
            logging.debug("%d rows to insert. Using single inserts.", length)
            resp = self.__create_singles(data=data)
        else:
            logging.debug("%d rows to insert. Using CreateMultiple.", length)
            resp = self.__create_multiple(data=copy(data))

        return resp

    def __create_singles(self, data: Sequence[MutableMapping[str, Any]]) -> list[requests.Response]:
        with ThreadPoolExecutor() as exec:
            futures = [
                exec.submit(
                    self._api_call,
                    method="POST",
                    url=self.entity_set_name,
                    json=payload,
                )
                for payload in data
            ]
            resp = [future.result() for future in as_completed(futures)]
        return resp

    def __create_multiple(self, data: Sequence[MutableMapping[str, Any]]) -> list[requests.Response]:
        # Adding odata type to each record
        for row in data:
            row["@odata.type"] = BASE_TYPE + self.logical_name

        # Chunking the payload to suggested size
        payload_chunks = [{"Targets": rows} for rows in chunk_data(data=data, size=100)]
        url = f"{self.entity_set_name}/{BASE_TYPE}CreateMultiple"

        # Threading the write operation
        with ThreadPoolExecutor() as exec:
            futures = [
                exec.submit(
                    self._api_call,
                    method="POST",
                    url=url,
                    json=payload,
                )
                for payload in payload_chunks
            ]
            resp = [future.result() for future in as_completed(futures)]

        return resp

    def __insert_batch(self, data: Sequence[MutableMapping[str, Any]]) -> list[requests.Response]:
        batch_data = transform_to_batch_data(url=self.entity_set_name, data=data, mode=BatchMode.POST)

        return self._batch_api_call(batch_data)
