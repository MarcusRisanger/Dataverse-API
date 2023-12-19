from typing import Any, Callable, Sequence
from urllib.parse import urljoin
from uuid import uuid4

import requests

from dataverse.errors import DataverseError
from dataverse.utils.batching import BatchCommand, chunk_data


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
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        data: str | None = None,
        json: dict[str, Any] | None = None,
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

        Returns
        -------
        requests.Response
            Response from API call.

        Raises
        ------
        requests.exceptions.HTTPError
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

        try:
            resp = self._session.request(
                method=method,
                url=request_url,
                headers=default_headers,
                params=params,
                data=data,
                json=json,
                timeout=120,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            message = ""
            if e.response:
                try:
                    message = e.response.json()["error"]["message"]
                except Exception:
                    pass
            messages: list[str] = [f"Error with GET request: {e.args[0]}"]
            if message:
                messages.append(f"Response: {message}")
            raise DataverseError(
                message="\n".join(messages),
                response=e.response,
                # details=e.response.args if e.response.args else None,
            ) from e

        return resp

    def _batch_api_call(
        self,
        batch_commands: Sequence[BatchCommand],
        id_generator: Callable[[], str] = lambda: str(uuid4),
    ) -> list[requests.Response]:
        responses: list[requests.Response] = []
        for batch in chunk_data(batch_commands):
            # Generate a unique ID for the batch
            id = f"batch_{id_generator()}"

            # Preparing batch data
            batch_data = [comm.encode(id, self._endpoint) for comm in batch]
            batch_data.append(f"\n--{id}--\n\n")

            data = "\n".join(batch_data)
            headers = {"Content-Type": f'multipart/mixed; boundary="{id}"', "If-None-Match": "null"}

            rsp = self._api_call(method="post", url="$batch", headers=headers, data=data)
            responses.append(rsp)
        return responses
