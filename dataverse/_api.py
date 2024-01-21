import logging
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urljoin
from uuid import uuid4

import requests

from dataverse.utils.batching import BatchCommand, RequestMethod, ThreadCommand, chunk_data


class Dataverse:
    """
    The main entrypoint for communicating with a given Dataverse Environment.

    Parameters
    ----------
    session: requests.Session
        The authenticated session used to communicate with the Web API.
    environment_url : str
        The environment URL that is used as a base for all API calls.
    """

    def __init__(self, session: requests.Session, environment_url: str):
        self._session = session
        self._environment_url = environment_url
        self._endpoint = urljoin(environment_url, "api/data/v9.2/")

    def _api_call(
        self,
        method: RequestMethod,
        url: str,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        data: str | None = None,
        json: Mapping[str, Any] | None = None,
    ) -> requests.Response:
        """
        Send API call to Dataverse. Fails silently, emits warnings
        if responses are not in 200-range.

        Parameters
        ----------
        method : str
            Request method.
        url : str
            URL added to endpoint.
        headers : dict
            Optional request headers. Will replace defaults.
        data : dict
            String payload.
        json : str
            Serializable JSON payload.

        Returns
        -------
        requests.Response
            Response from API call.
        """
        request_url = urljoin(self._endpoint, url)

        default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "OData-Version": "4.0",
            "OData-MaxVersion": "4.0",
        }

        if headers is not None:
            for k, v in headers.items():
                default_headers[k] = v

        resp = self._session.request(
            method=method.name,
            url=request_url,
            headers=default_headers,
            params=params,
            data=data,
            json=json,
            timeout=120,
        )
        if not (200 <= resp.status_code <= 299):
            logging.error(
                "Request failed for %s to %s.",
                method.name,
                request_url,
            )
        return resp

    def _batch_api_call(
        self,
        batch_commands: Sequence[BatchCommand],
        id_generator: Callable[[], str] | None = None,
    ) -> list[requests.Response]:
        if id_generator is None:
            id_generator = lambda: str(uuid4())  # noqa: E731

        batches: list[ThreadCommand] = list()
        for batch in chunk_data(batch_commands, 500):
            # Generate a unique ID for the batch
            id = f"batch_{id_generator()}"

            # Preparing batch data
            batch_data = [comm.encode(id, self._endpoint) for comm in batch]
            batch_data.append(f"\n--{id}--\n\n")

            payload = "\n".join(batch_data)
            headers = {"Content-Type": f'multipart/mixed; boundary="{id}"', "If-None-Match": "null"}

            batches.append(ThreadCommand(method=RequestMethod.POST, url="$batch", headers=headers, data=payload))

        return self._threaded_call(batches)

    def _threaded_call(self, calls: Sequence[ThreadCommand]) -> list[requests.Response]:
        """
        Performs a threaded API call using `concurrent.futures.ThreadPoolExecutor`
        """
        with ThreadPoolExecutor() as exec:
            futures = [
                exec.submit(
                    self._api_call,
                    method=call.method,
                    url=call.url,
                    headers=call.headers,
                    params=call.params,
                    data=call.data,
                    json=call.json,
                )
                for call in calls
            ]
            resp = [future.result() for future in as_completed(futures)]

        return resp
