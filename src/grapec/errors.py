"""Error types shared by all transports."""

from __future__ import annotations

import enum
from collections.abc import Iterable, Iterator, Mapping
from typing import Any


class Status(enum.IntEnum):
    """Call status codes, aligned with the gRPC status code numbers."""

    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16


# what the ``metadata`` call option accepts, repeat a key with pairs
MetadataLike = Mapping[str, str | bytes] | Iterable[tuple[str, str | bytes]]


class Headers(Mapping[str, "str | bytes"]):
    """Metadata as it went over the wire, a key may carry several values.

    ``headers[key]`` is the first value, ``get_all(key)`` every value in the
    order received. Binary values (``-bin`` keys) are ``bytes``. Compares
    equal to a plain ``dict`` when no key repeats.
    """

    __slots__ = ("_values",)

    def __init__(self, items: "Mapping[str, str | bytes] | Iterable[tuple[str, str | bytes]] | None" = None) -> None:
        self._values: dict[str, list[str | bytes]] = {}
        for key, value in metadata_pairs(items):
            self._values.setdefault(key, []).append(value)

    def __getitem__(self, key: str) -> str | bytes:
        return self._values[key][0]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def get_all(self, key: str) -> list[str | bytes]:
        """Every value of ``key`` in order, an empty list when it is absent."""
        return list(self._values.get(key, ()))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Headers):
            return self._values == other._values
        if isinstance(other, Mapping):
            return self._values == {key: [value] for key, value in other.items()}
        return NotImplemented

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:
        return f"Headers({list(metadata_pairs(self))!r})"


def metadata_pairs(
    items: "Mapping[str, str | bytes] | Iterable[tuple[str, str | bytes]] | None",
) -> Iterator[tuple[str, str | bytes]]:
    """``(key, value)`` pairs of outgoing or incoming metadata, repeated keys included."""
    if items is None:
        return
    if isinstance(items, Headers):
        for key, values in items._values.items():
            for value in values:
                yield key, value
    elif isinstance(items, Mapping):
        yield from items.items()
    else:
        for key, value in items:
            yield key, value


class GrapecError(Exception):
    """Base class for call failures.

    Mistakes in the calling program (wrong types, bad schemas, unsupported
    options) raise ``TypeError`` or ``ValueError`` instead.
    """


class RpcError(GrapecError):
    """The remote side answered the call with a non OK status.

    ``headers`` and ``trailers`` hold the response metadata the server sent
    along with the status, binary values (``-bin`` keys) are ``bytes``.
    """

    def __init__(
        self,
        code: Status,
        message: str = "",
        details: bytes = b"",
        *,
        headers: Mapping[str, Any] | Iterable[tuple[str, Any]] | None = None,
        trailers: Mapping[str, Any] | Iterable[tuple[str, Any]] | None = None,
    ) -> None:
        super().__init__(f"{code.name}: {message}" if message else code.name)
        self.code = code
        self.message = message
        self.details = details
        self.headers = Headers(headers)
        self.trailers = Headers(trailers)



class TransportError(GrapecError):
    """The call failed below the application level, the connection is unusable."""
