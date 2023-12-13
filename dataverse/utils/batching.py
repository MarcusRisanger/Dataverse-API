import json
from dataclasses import dataclass, field
from textwrap import dedent
from typing import Any, Generator, Sequence, TypeVar
from urllib.parse import urljoin

from dataverse.utils.text import encode_altkeys

T = TypeVar("T")


@dataclass(slots=True)
class BatchCommand:
    """
    For encapsulating a singular Dataverse batch command.

    Parameters
    ----------
    url : st
        The url that will be appended to the endpoint url.
    mode : str
        The request mode for the batch command.
    data : dict
        JSON serializable payload
    single_col : bool
        Whether the batch command targets a single column,
        such as for instance in a PUT or DELETE. If this is
        set to True, a data

    """

    url: str
    mode: str = field(default="GET")
    data: dict[str, Any] | None = field(default=None)
    single_col: bool = field(default=False)
    content_type: str = field(init=False, default="Content-Type: application/json")

    def __post_init__(self) -> None:
        if self.single_col and self.mode != "GET":
            assert self.data
            assert len(self.data) == 1
            col, value = list(self.data.items())[0]
            self.url += f"/{col}"
            self.data = {"value": value}

        if self.mode == "POST":
            self.content_type += "; type=entry"

        self.url = encode_altkeys(self.url)

    def encode(self, batch_id: str, api_url: str) -> str:
        """
        Encodes the batch command into a string.

        Parameters
        ----------
        batch_id : str
            A generated batch ID.
        api_url : str
            The base API endpoint.

        Returns
        -------
        str
            The batch command encoded as a string.
        """

        url = urljoin(api_url, self.url)

        row_command = f"""\
        --{batch_id}
        Content-Type: application/http
        Content-Transfer-Encoding: binary

        {self.mode} {url} HTTP/1.1
        {self.content_type}

        {json.dumps(self.data)}
        """
        return dedent(row_command)


def chunk_data(data: Sequence[T], size: int = 500) -> Generator[Sequence[T], None, None]:
    """
    Simple function to chunk a list into a maximum number of
    elements per chunk.

    Parameters
    ----------
    data : list of `DataverseBatchCommand`
        List containing all commands to be chunked.
    size: int, optional
        Chunking size.

    Yields
    ------
    list of `DataverseBatchCommand`
    """
    for i in range(0, len(data), size):
        yield data[i : i + size]  # noqa E203
