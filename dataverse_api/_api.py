import logging
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urljoin
from uuid import uuid4

import requests

from dataverse_api.errors import DataverseAPIError
from dataverse_api.utils.batching import APICommand, BatchCommand, RequestMethod, chunk_data
from dataverse_api.utils.data import serialize_json


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
        timeout: int | None = None,
    ) -> requests.Response:
        """
        Send API call to Dataverse.

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
        timeout : int
            The timeout limit in seconds per call.

        Returns
        -------
        requests.Response
            Response from API call.

        Raises
        ------
        requests.HTTPError
            For failing requests.
        """
        request_url = urljoin(self._endpoint, url)

        default_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "OData-Version": "4.0",
            "OData-MaxVersion": "4.0",
        }

        if headers:
            for k, v in headers.items():
                default_headers[k] = v

        if timeout is None:
            timeout = 120

        if json is not None and data is None:
            data = serialize_json(json)

        resp = self._session.request(
            method=method,
            url=request_url,
            headers=default_headers,
            params=params,
            data=data,
            timeout=timeout,
        )

        try:
            resp.raise_for_status()
        except requests.HTTPError:
            error_msg = resp.json()["error"]["message"].splitlines()[0]
            raise DataverseAPIError(message=f"{method} request failed: {error_msg}", response=resp) from None

        return resp

    def _batch_api_call(
        self,
        batch_commands: Sequence[BatchCommand],
        batch_size: int,
        id_generator: Callable[[], str] | None = None,
        timeout: int | None = None,
        threading: bool = False,
    ) -> list[requests.Response]:
        """
        Performs a batch requests.

        Parameters
        ----------
        batch_commands : Sequence[BatchCommand]
            The request descriptions for each batch command to submit.
        batch_size : int
            Batch size for tuning sizes.
        id_generator : Callable[[], str]
            Optional callable for generating unique batch IDs.
        timeout : int | None
            Optional timeout override.

        Returns
        -------
        list[requests.Responses]
            The responses per request.
        """

        if id_generator is None:
            id_generator = lambda: str(uuid4())  # noqa: E731

        calls: list[APICommand] = list()
        for batch in chunk_data(data=batch_commands, size=batch_size):
            # Generate a unique ID for the batch
            id = f"batch_{id_generator()}"

            # Preparing batch data
            batch_data = [comm.encode(batch_id=id, api_url=self._endpoint) for comm in batch]
            batch_data.append(f"\n--{id}--\n\n")

            payload = "\n".join(batch_data)
            headers = {"Content-Type": f'multipart/mixed; boundary="{id}"', "If-None-Match": "null"}

            calls.append(APICommand(method=RequestMethod.POST, url="$batch", headers=headers, data=payload))

        if threading:
            return self._threaded_call(calls=calls, timeout=timeout)
        else:
            return self._individual_call(calls=calls, timeout=timeout)

    def _individual_call(self, calls: Sequence[APICommand], timeout: int | None = None) -> list[requests.Response]:
        """
        Performs a sequential API calls.

        Parameters
        ----------
        calls : Sequence[APICommand]
            The descriptions of each request to submit.
        timeout : int | None
            Optional timeout override.

        Returns
        -------
        list[requests.Responses]
            The responses per request.
        """

        out = []
        for call in calls:
            try:
                out.append(self._api_call(**call.__dict__, timeout=timeout))
            except DataverseAPIError as e:
                logging.error(f"API request error: {e.args[0]}")
                out.append(e.response)
        return out

    def _threaded_call(self, calls: Sequence[APICommand], timeout: int | None = None) -> list[requests.Response]:
        """
        Performs a threaded API call using `concurrent.futures.ThreadPoolExecutor`

        Parameters
        ----------
        calls : Sequence[APICommand]
            The descriptions of each request to submit.
        timeout : int | None
            Optional timeout override.

        Returns
        -------
        list[requests.Responses]
            The responses per request.
        """
        with ThreadPoolExecutor() as exec:
            futures = [
                exec.submit(
                    self._api_call,
                    **call.__dict__,
                    timeout=timeout,
                )
                for call in calls
            ]

            # Need something like this for handling
            # exceptions during threaded calls
            resp: list[requests.Response] = []
            for future in as_completed(futures):
                try:
                    resp.append(future.result())
                except DataverseAPIError as e:
                    logging.error(f"API request error: {e.args[0]}")
                    resp.append(e.response)

        return resp
