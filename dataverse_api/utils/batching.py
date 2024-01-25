from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum, auto
from textwrap import dedent
from typing import Any, Collection, Generator, Mapping, MutableMapping, Sequence, TypeVar
from urllib.parse import urljoin

from dataverse_api.utils.data import serialize_json
from dataverse_api.utils.text import encode_altkeys

T = TypeVar("T")


class RequestMethod(Enum):
    GET = auto()
    POST = auto()
    PATCH = auto()
    PUT = auto()
    DELETE = auto()


@dataclass(slots=True)
class ThreadCommand:
    """
    For encapsulating a single request for Threaded execution.

    Parameters
    ----------
    url : str
    method : str
    """

    url: str
    method: RequestMethod
    headers: Mapping[str, str] | None = None
    params: Mapping[str, str] | None = None
    data: str | None = None
    json: MutableMapping[str, Any] | None = None


@dataclass(slots=True)
class BatchCommand:
    """
    For encapsulating a singular Dataverse batch command.

    Parameters
    ----------
    url : str
        The url that will be appended to the endpoint url.
    method : RequestMethod
        The request method for the batch command.
    headers : dict
        Any additional headers to pass for the specific batch command.
    data : dict
        Optional JSON serializable payload depending on request method.
    """

    url: str
    method: RequestMethod
    headers: Mapping[str, str] | None = field(default=None)
    data: Mapping[str, Any] | None = field(default=None)
    extra_header: str = field(init=False, default="")
    single_col: bool = field(init=False, default=False)
    content_type: str = field(init=False, default="Content-Type: application/json")

    def __post_init__(self) -> None:
        if self.method == RequestMethod.PUT:
            self.single_col = True
            assert self.data is not None
            assert len(self.data) == 1
            col, value = list(self.data.items())[0]
            self.url += f"/{col}"
            self.data = {"value": value}

        if self.method == RequestMethod.POST:
            self.content_type += "; type=entry"

        if self.headers:
            print("Extra!")
            self.extra_header = "\n".join([f"{k}: {v}" for k, v in self.headers.items()])

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

        {self.method.name} {url} HTTP/1.1
        {self.content_type}
        {self.extra_header}\n
        {serialize_json(self.data)}
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


def transform_to_batch_for_create(
    url: str,
    data: Collection[Mapping[str, Any]],
) -> list[BatchCommand]:
    """Transform data payload to creation batch data."""
    return [BatchCommand(url, method=RequestMethod.POST, data=row) for row in data]


def transform_to_batch_for_delete(url: str, data: Iterable[str], column: str | None = None) -> list[BatchCommand]:
    """
    Transform data payload to deletion batch data.

    Parameters
    ----------
    url : str
        The EntitySetName of targeted Dataverse Entity.
    data : iterable of str
        Primary IDs for deletion.
    column : str
        Optional column to target for deletion.

    Returns
    -------
    list of BatchDataCommand
        Payload for passing to the API batch call endpoint.
    """
    column = "" if column is None else f"/{column}"
    return [BatchCommand(url=f"{url}({id}){column}", method=RequestMethod.DELETE) for id in data]


UpsertDataType = tuple[str, dict[str, Any]]


def transform_upsert_data(
    data: Collection[Mapping[str, Any]],
    keys: Iterable[str],
    is_primary_id: bool,
) -> Generator[UpsertDataType, None, None]:
    """
    Transform upsert data to keys and payload.

    Parameters
    ----------
    data : Collection[Mapping[str, Any]]
        The data to upsert to Dataverse.
    keys : Iterable[str]
        The keys to extract from the data to form the target row identifier.
    is_primary_id : bool
        Whether the given key (singular) is the primary ID attribute for the Entity.

    Yields
    -------
    tuple : str, dict
        [0] : The target row identifier
        [1] : The data payload
    """
    for row in data:
        if is_primary_id:
            # No repr on string
            row_key = [f"{row[part]}" for part in keys]
        else:
            # Repr on string
            row_key = [f"{part}={row[part].__repr__()}" for part in keys]
        row_data = {k: v for k, v in row.items() if k not in keys}

        yield ",".join(row_key), row_data


def transform_to_batch_for_upsert(
    url: str,
    data: Collection[MutableMapping[str, Any]],
    keys: Iterable[str],
    is_primary_id: bool = False,
) -> list[BatchCommand]:
    """
    Transform data payload to upsert batch data.

    Parameters
    ----------
    url : str
        The Entity endpoint for upsertion.
    data : collection of data dictionaries
        The data to be upserted into Dataverse.
    keys : iterable of str
        The keys used to identify unique rows in the dataset.
    is_id : bool
        Whether the supplied singular key is the Entity primary ID attribute.
    """
    commands = []

    for keys, payload in transform_upsert_data(data, keys, is_primary_id):
        commands.append(
            BatchCommand(
                url=f"{url}({keys})",
                method=RequestMethod.PATCH,
                data=payload,
            )
        )

    return commands
