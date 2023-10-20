"""
Contains the DataverseAPI class, where the standard requests against
the Dataverse Web API are stored. Meant to be inherited by other
classes requiring calls against the API.

Author: Marcus Risanger
"""

import logging
from collections.abc import Callable
from typing import Any, Optional, Union
from urllib.parse import urljoin

import requests

from dataverse_api.dataclasses import DataverseAuth, DataverseBatchCommand
from dataverse_api.errors import DataverseError
from dataverse_api.utils import (
    batch_command,
    batch_id_generator,
    chunk_data,
    expand_headers,
)

log = logging.getLogger("dataverse-api")


class DataverseAPI:
    """
    Base class used to interact with the Web API.

    Args:
      - app_id: Azure App Registration ID
      - client_secret: Secret for App registration
      - authority_url: Authority URL for App registration
      - dynamics_url: Base environment url
      - scopes: The App registration scope names
    """

    def __init__(
        self,
        auth: DataverseAuth,
    ):
        self.api_url = urljoin(auth.resource, "/api/data/v9.2/")
        self._auth = auth.auth
        self._default_headers = {
            "Accept": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Content-Type": "application/json",
        }

    def _get(
        self,
        url: Optional[str] = None,
        additional_headers: Optional[dict] = None,
        **kwargs,
    ) -> requests.Response:
        """
        GET is used to retrieve data from Dataverse.

        Optional args:
          - url: Appended to API endpoint
          - additional_headers: Headers to overwrite default headers

        Kwargs:
          - data: Request payload (str, bytes etc.)
          - json: Request JSON serializable payload
          - params: Relevant request parameters
          - url_override: The complete request URL, overriding the url keyword.
        """
        url = kwargs.get("url_override") or urljoin(self.api_url, url)
        if url is None:
            raise DataverseError("Needs either url or url_override argument as kwarg.")
        headers = expand_headers(self._default_headers, additional_headers)

        try:
            response = requests.get(
                url=url,
                auth=self._auth,
                headers=headers,
                data=kwargs.get("data"),
                json=kwargs.get("json"),
                params=kwargs.get("params"),
            )
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            raise DataverseError(
                (
                    f"Error with GET request: {e.args[0]}"
                    + f" // Response body: {e.response.text}"
                ),
                response=e.response,
            )

    def _post(
        self,
        url: str,
        additional_headers: Optional[dict] = None,
        data: Optional[str] = None,
        json: Optional[dict] = None,
    ) -> requests.Response:
        """
        POST is used to write new data or send a batch request to Dataverse.

        Args:
          - url: Appended to API endpoint
          - additional_headers: Headers to overwrite default headers
          - data: Request payload (str, bytes etc.)
          - json: Request JSON serializable payload
        """
        headers = expand_headers(self._default_headers, additional_headers)
        url = urljoin(self.api_url, url)

        try:
            response = requests.post(
                url=url,
                auth=self._auth,
                headers=headers,
                data=data,
                json=json,
            )
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            raise DataverseError(
                (
                    f"Error with POST request: {e.args[0]}"
                    + f" // Response body: {e.response.text}"
                ),
                response=e.response,
            )

    def _put(
        self,
        url: str,
        additional_headers: Optional[dict] = None,
        data: Optional[str] = None,
        json: Optional[dict] = None,
    ) -> requests.Response:
        """
        PUT is used to update a single column value for a single record.

        Args:
          - url: Appended to API endpoint
          - additional_headers: Headers to overwrite default headers
          - data: Request payload (str, bytes etc.)
          - json: Request JSON serializable payload
        """
        headers = expand_headers(self._default_headers, additional_headers)
        url = urljoin(self.api_url, url)
        try:
            response = requests.put(
                url=url,
                auth=self._auth,
                headers=headers,
                data=data,
                json=json,
            )
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            raise DataverseError(
                (
                    f"Error with POST request: {e.args[0]}"
                    + f" // Response body: {e.response.text}"
                ),
                response=e.response,
            )

    def _patch(
        self,
        url: str,
        data: Optional[Union[str, bytes]] = None,
        json: Optional[dict[str, Any]] = None,
        additional_headers: Optional[dict] = None,
    ) -> requests.Response:
        """
        PATCH is used to update several values for a single record.

        Args:
          - url: Postfix of API endpoint to isolate unique record
          - additional_headers: If it is required to overwrite default
            or add new header elements
          - data: JSON serializable dictionary containing data payload.
        """
        headers = expand_headers(self._default_headers, additional_headers)
        url = urljoin(self.api_url, url)

        try:
            response = requests.patch(
                url=url,
                auth=self._auth,
                headers=headers,
                data=data,
                json=json,
            )
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            raise DataverseError(
                (
                    f"Error with PATCH request: {e.args[0]}"
                    + f" // Response body: {e.response.text}"
                ),
                response=e.response,
            )

    def _delete(
        self, url: str, additional_headers: Optional[dict] = None
    ) -> requests.Response:
        """
        DELETE is used to either purge whole records or a specific
        column value for a particular record.

        Args:
          - url: Postfix of API endpoint to isolate unique record
            or record + column
          - additional_headers: If it is required to overwrite default
            or add new header elements
        """
        headers = expand_headers(self._default_headers, additional_headers)
        url = urljoin(self.api_url, url)

        try:
            response = requests.delete(
                url=url,
                auth=self._auth,
                headers=headers,
            )
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            raise DataverseError(
                (
                    f"Error with DELETE request: {e.args[0]}"
                    + f" // Response body: {e.response.text}"
                ),
                response=e.response,
            )

    def _batch_operation(
        self,
        data: list[DataverseBatchCommand],
        batch_id_generator: Callable[..., str] = batch_id_generator,
    ) -> requests.Response:
        """
        Generalized function to run batch commands against Dataverse.

        Data containing either a list of DataverseBatchCommands containing
        the relevant data for submission, where each dict or table row
        contains necessary information for one single batch command.

        DataverseBatchCommands have the following attributes:
          - uri: The postfix after API endpoint to form the full command URI.
          - mode: The mode used by the singular batch command.
          - data: The data to be transmitted related to the the command.

        Args:
          - data: A list of `DataverseBatchCommand` to be executed
          - batch_id_generator: An optional function call for overriding
            default unique batch ID generation.
        """

        for chunk in chunk_data(data, size=1000):
            batch_id = f"batch_{batch_id_generator()}"

            # Preparing batch data
            batch_data = ""
            for row in chunk:
                batch_data += batch_command(
                    batch_id=batch_id, api_url=self.api_url, row=row
                )

            # Note: Trailing space in final line is crucial
            # Request fails to meet specification without it
            batch_data += f"\n\n--{batch_id}--\n "

            # Preparing batch-specific headers
            additional_headers = {
                "Content-Type": f'multipart/mixed; boundary="{batch_id}"',
                "If-None-Match": "null",
            }

            log.debug(
                f"Sending batch ID {batch_id} containing {len(chunk)} "
                + "commands for processing in Dataverse."
            )

            response = self._post(
                url="$batch", additional_headers=additional_headers, data=batch_data
            )
            log.debug(f"Successfully completed {len(chunk)} batch command chunk.")

        log.debug("Successfully completed all batch commands.")
        return response
