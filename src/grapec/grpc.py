"""gRPC over HTTP/2, built on the sans-IO ``h2`` library.

:class:`GrpcProtocol` holds all protocol knowledge (HTTP/2 state, gRPC
framing, headers, compression) and never touches a socket. The sync and
async connections in ``_sync.py`` and ``_async.py`` only move bytes between
it and the network.
"""

from __future__ import annotations

import base64
import binascii
import gzip
import struct as _struct
import zlib
from typing import Any
from urllib.parse import unquote

import h2.config
import h2.connection
import h2.errors
import h2.events
import h2.exceptions
import h2.settings

from .errors import Headers, MetadataLike, RpcError, Status, TransportError, metadata_pairs

USER_AGENT = "grapec"

_ENCODERS = {
    "gzip": gzip.compress,
    "deflate": zlib.compress,
}
# zlib wbits per grpc-encoding, gzip framing and zlib framing
_DECODERS = {
    "gzip": 31,
    "deflate": 15,
}
ACCEPT_ENCODING = "identity,gzip,deflate"

# HTTP/2 receive window per stream and per connection, the 64 KiB protocol
# default would stall a large response on every round trip
_WINDOW_SIZE = 4 * 1024 * 1024

# errors from the IO layer or h2 that mean the connection is unusable
IO_ERRORS = (OSError, h2.exceptions.H2Error, EOFError)

_FRAME_HEADER = 9
_CLOSED = h2.connection.ConnectionState.CLOSED
# transport level trailers that are reported through RpcError / the payload
_STATUS_KEYS = frozenset({"grpc-status", "grpc-message", "grpc-status-details-bin"})


def authority(host: str, port: int) -> str:
    """``host:port`` for the ``:authority`` header, IPv6 literals get brackets."""
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


class GrpcProtocol:
    """Sans-IO gRPC client state for one HTTP/2 connection, one call at a time."""

    def __init__(self, authority: str, *, tls: bool, max_receive_size: int | None = None) -> None:
        self._authority = authority
        self._scheme = "https" if tls else "http"
        self._max_receive_size = max_receive_size
        self._h2 = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=True, header_encoding="utf-8")
        )
        self._h2.local_settings = h2.settings.Settings(
            client=True, initial_values={h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: _WINDOW_SIZE}
        )
        self._h2.initiate_connection()
        self._h2.increment_flow_control_window(_WINDOW_SIZE - self._h2.inbound_flow_control_window)
        self.dead = False
        self._call: _Call | None = None
        self._inbuf = bytearray()

    @property
    def healthy(self) -> bool:
        if self.dead:
            return False
        return self._h2.state_machine.state is not _CLOSED

    @property
    def busy(self) -> bool:
        """A call was started and neither finished, cancelled nor aborted."""
        return self._call is not None

    # -- driving the connection ----------------------------------------------

    def start(
        self,
        path: str,
        payload: bytes,
        *,
        timeout: float | None,
        metadata: MetadataLike | None,
        compression: str | None,
    ) -> None:
        """Queue one unary call. Follow with ``data_to_send`` / ``feed`` until ``done``."""
        if self._call is not None:
            raise TransportError("connection is busy")
        if compression is not None and compression != "identity":
            try:
                payload = _ENCODERS[compression](payload)
            except KeyError:
                raise ValueError(f"unsupported compression {compression!r}") from None
            flag = 1
        else:
            compression = None
            flag = 0
        headers = self._request_headers(path, timeout, metadata, compression)
        stream_id = self._h2.get_next_available_stream_id()
        self._h2.send_headers(stream_id, headers)
        self._call = _Call(stream_id, _struct.pack(">BI", flag, len(payload)) + payload)

    def data_to_send(self) -> bytes:
        """Bytes to write to the network, pushes as much request body as flow control allows."""
        call = self._call
        if call is not None and call.pending and not call.ended and not self.dead:
            while call.pending:
                window = min(self._h2.local_flow_control_window(call.stream_id), self._h2.max_outbound_frame_size)
                if window <= 0:
                    break
                chunk, call.pending = call.pending[:window], call.pending[window:]
                self._h2.send_data(call.stream_id, bytes(chunk), end_stream=not call.pending)
        return self._h2.data_to_send()

    def feed(self, data: bytes) -> None:
        """Process bytes read from the network.

        Frames are handed to h2 one at a time so that events of a frame that
        completes the current call survive an error raised by a later frame
        in the same read (h2 rejects anything after GOAWAY).
        """
        if not data:
            self.dead = True
            raise TransportError("connection closed by peer")
        self._inbuf += data
        while len(self._inbuf) >= _FRAME_HEADER and not self.dead:
            length = int.from_bytes(self._inbuf[:3], "big") + _FRAME_HEADER
            if len(self._inbuf) < length:
                break
            frame = bytes(self._inbuf[:length])
            del self._inbuf[:length]
            self._feed_frame(frame)

    def feed_idle(self, data: bytes) -> None:
        """``feed`` for bytes that arrived while no call was active, never raises."""
        try:
            self.feed(data)
        except TransportError:
            pass

    def _feed_frame(self, frame: bytes) -> None:
        call = self._call
        try:
            events = self._h2.receive_data(frame)
        except h2.exceptions.H2Error as exc:
            was_terminated = self.dead or self._h2.state_machine.state is _CLOSED
            self.dead = True
            if was_terminated:
                self._terminated(call, exc)
                return
            raise
        for event in events:
            if isinstance(event, h2.events.ConnectionTerminated):
                self.dead = True
                self._terminated(call, None, event.error_code)
                return
            if call is None or getattr(event, "stream_id", None) != call.stream_id:
                continue
            if isinstance(event, h2.events.DataReceived):
                self._h2.acknowledge_received_data(event.flow_controlled_length, call.stream_id)
                call.data += event.data
                self._check_size(call)
            elif isinstance(event, h2.events.ResponseReceived):
                call.headers = event.headers
            elif isinstance(event, h2.events.TrailersReceived):
                call.trailers = event.headers
            elif isinstance(event, h2.events.StreamEnded):
                call.ended = True
            elif isinstance(event, h2.events.StreamReset):
                if call.ended:
                    continue  # RST_STREAM(NO_ERROR) after END_STREAM is allowed
                self.dead = True
                raise TransportError(f"stream reset by peer (error {event.error_code})")

    def _check_size(self, call: _Call) -> None:
        """Give up on a response message larger than ``max_receive_size`` as soon as its length prefix arrives."""
        limit = self._max_receive_size
        if limit is None or len(call.data) < 5:
            return
        # a body running past its own length prefix is caught by _unframe,
        # but it must not grow without bound meanwhile
        length = max(int.from_bytes(call.data[1:5], "big"), len(call.data) - 5)
        if length > limit:
            # the RST_STREAM stays queued in h2 for the next data_to_send
            self._call = None
            try:
                self._h2.reset_stream(call.stream_id, h2.errors.ErrorCodes.CANCEL)
            except h2.exceptions.StreamClosedError:
                pass  # the stream already ended, nothing to reset
            raise _too_large(length, limit)

    def _terminated(self, call: _Call | None, cause: BaseException | None, error_code: Any = None) -> None:
        """The peer closed the connection. Only an unfinished call is an error."""
        if call is not None and call.ended:
            return
        suffix = f" (error {error_code})" if error_code is not None else ""
        raise TransportError(f"connection terminated by peer{suffix}") from cause

    @property
    def done(self) -> bool:
        return self._call is not None and self._call.ended

    def result(self) -> tuple[bytes, Headers, Headers]:
        """Response message, headers and trailers of the finished call.

        Raises ``RpcError`` for a non OK status.
        """
        call = self._call
        assert call is not None and call.ended
        self._call = None
        return call.finish(self._max_receive_size)

    def cancel(self) -> bytes:
        """Give up on the current call, returns the RST_STREAM bytes to flush.

        The connection stays usable. Returns ``b""`` and marks the connection
        dead if the stream cannot be reset.
        """
        call = self._call
        self._call = None
        if call is None:
            return b""
        try:
            if not call.ended:
                self._h2.reset_stream(call.stream_id, h2.errors.ErrorCodes.CANCEL)
            return self._h2.data_to_send()
        except Exception:
            self.dead = True
            return b""

    def abort(self) -> None:
        """Forget the current call and mark the connection unusable."""
        self._call = None
        self.dead = True

    def close(self) -> bytes:
        self.dead = True
        try:
            self._h2.close_connection()
            return self._h2.data_to_send()
        except Exception:
            return b""

    # -- headers ---------------------------------------------------------------

    def _request_headers(
        self, path: str, timeout: float | None, metadata: MetadataLike | None, compression: str | None
    ) -> list[tuple[str, str]]:
        headers = [
            (":method", "POST"),
            (":scheme", self._scheme),
            (":path", path),
            (":authority", self._authority),
            ("te", "trailers"),
            ("content-type", "application/grpc"),
            ("user-agent", USER_AGENT),
            ("grpc-accept-encoding", ACCEPT_ENCODING),
        ]
        if compression is not None:
            headers.append(("grpc-encoding", compression))
        if timeout is not None:
            headers.append(("grpc-timeout", _format_timeout(timeout)))
        for key, value in metadata_pairs(metadata):
            key = key.lower()
            if key.startswith(":") or key.startswith("grpc-"):
                raise ValueError(f"metadata key {key!r} is reserved")
            if isinstance(value, bytes):
                if not key.endswith("-bin"):
                    raise ValueError(f"binary metadata key {key!r} must end with -bin")
                headers.append((key, base64.b64encode(value).decode("ascii")))
            else:
                headers.append((key, value))
        return headers


class _Call:
    def __init__(self, stream_id: int, body: bytes) -> None:
        self.stream_id = stream_id
        self.pending = memoryview(body)
        self.headers: list[tuple[str, str]] = []
        self.trailers: list[tuple[str, str]] = []
        self.data = bytearray()
        self.ended = False

    def finish(self, max_receive_size: int | None) -> tuple[bytes, Headers, Headers]:
        raw_headers = dict(self.headers)
        raw_trailers = dict(self.trailers)
        http_status = raw_headers.get(":status")
        if not self.trailers and "grpc-status" in raw_headers:
            # trailers-only response, gRPC reports its metadata as trailers
            headers = Headers()
            trailers = _metadata(self.headers)
        else:
            headers = _metadata(self.headers)
            trailers = _metadata(self.trailers)
        if http_status != "200":
            raise RpcError(
                _status_from_http(http_status),
                f"unexpected HTTP status {http_status}",
                headers=headers,
                trailers=trailers,
            )

        # trailers-only responses carry grpc-status in the headers
        meta = raw_trailers or raw_headers
        raw_status = meta.get("grpc-status")
        if raw_status is None:
            raise TransportError("response is missing grpc-status")
        code = _status_code(raw_status)
        message = unquote(meta.get("grpc-message", ""))
        details = _b64decode(meta.get("grpc-status-details-bin", ""))
        if code is not Status.OK:
            raise RpcError(code, message, details, headers=headers, trailers=trailers)

        payload = _unframe(bytes(self.data), raw_headers.get("grpc-encoding", "identity"), max_receive_size)
        return payload, headers, trailers


def _b64decode(text: str) -> bytes:
    if not text:
        return b""
    try:
        return base64.b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError):
        return b""


def _metadata(raw: list[tuple[str, str]]) -> Headers:
    """User visible metadata: no pseudo headers, no status trailers, ``-bin`` decoded.

    Repeated keys keep every value. A proxy may fold repeated binary values
    into one comma separated header, those are split again.
    """
    pairs: list[tuple[str, str | bytes]] = []
    for key, value in raw:
        if key.startswith(":") or key in _STATUS_KEYS:
            continue
        if key.endswith("-bin"):
            pairs.extend((key, _b64decode(part.strip())) for part in value.split(","))
        else:
            pairs.append((key, value))
    return Headers(pairs)


def _too_large(size: int, limit: int) -> RpcError:
    return RpcError(Status.RESOURCE_EXHAUSTED, f"received message of {size} bytes exceeds max_receive_size ({limit})")


def _unframe(data: bytes, encoding: str, limit: int | None) -> bytes:
    if len(data) < 5:
        raise TransportError("response body is truncated")
    compressed, length = _struct.unpack(">BI", data[:5])
    if len(data) != 5 + length:
        raise TransportError("response does not contain exactly one message")
    message = data[5:]
    if not compressed:
        return message
    wbits = _DECODERS.get(encoding)
    if wbits is None:
        raise TransportError(f"unsupported response compression {encoding!r}")
    # bounded, a tiny compressed message must not expand past the limit
    decoder = zlib.decompressobj(wbits)
    try:
        out = decoder.decompress(message, limit + 1 if limit is not None else 0)
    except zlib.error as exc:
        raise TransportError(f"cannot decompress response: {exc}") from exc
    if limit is not None and len(out) > limit:
        raise RpcError(Status.RESOURCE_EXHAUSTED, f"decompressed message exceeds max_receive_size ({limit})")
    if not decoder.eof:
        raise TransportError("cannot decompress response: compressed data is truncated")
    return out


def _status_code(raw: str) -> Status:
    try:
        return Status(int(raw))
    except ValueError:
        return Status.UNKNOWN


def _status_from_http(http_status: str | None) -> Status:
    mapping = {
        "400": Status.INTERNAL,
        "401": Status.UNAUTHENTICATED,
        "403": Status.PERMISSION_DENIED,
        "404": Status.UNIMPLEMENTED,
        "429": Status.UNAVAILABLE,
        "502": Status.UNAVAILABLE,
        "503": Status.UNAVAILABLE,
        "504": Status.UNAVAILABLE,
    }
    return mapping.get(http_status or "", Status.UNKNOWN)


def _format_timeout(seconds: float) -> str:
    # grpc-timeout carries at most 8 digits, pick the finest unit that fits
    for unit, factor in (("n", 1e9), ("u", 1e6), ("m", 1e3), ("S", 1), ("M", 1 / 60), ("H", 1 / 3600)):
        value = int(seconds * factor)
        if value < 100_000_000:
            return f"{max(value, 1)}{unit}"
    return "99999999H"


def tls_context() -> Any:
    import ssl

    ctx = ssl.create_default_context()
    ctx.set_alpn_protocols(["h2"])
    return ctx
