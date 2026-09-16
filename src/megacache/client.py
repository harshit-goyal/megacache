"""Small RESP2 client used by the native MegaCache CLI."""

import socket
from typing import Any, BinaryIO, Optional, Sequence, Union


class MegaCacheClientError(Exception):
    """Base error for native client failures."""


class MegaCacheCommandError(MegaCacheClientError):
    """Error returned by the MegaCache server."""


class MegaCacheProtocolError(MegaCacheClientError):
    """Malformed or unexpected RESP response."""


CommandPart = Union[str, bytes, int]


class MegaCacheClient:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        password: Optional[str] = None,
        timeout: float = 5,
    ) -> None:
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout
        self._socket: Optional[socket.socket] = None
        self._stream: Optional[BinaryIO] = None

    def __enter__(self) -> "MegaCacheClient":
        self.connect()
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def connect(self) -> None:
        if self._socket is not None:
            return
        try:
            self._socket = socket.create_connection(
                (self.host, self.port), timeout=self.timeout
            )
            self._stream = self._socket.makefile("rwb")
            if self.password is not None:
                self.command("AUTH", self.password)
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self._stream is not None:
            self._stream.close()
        if self._socket is not None:
            self._socket.close()
        self._stream = None
        self._socket = None

    def command(self, *parts: CommandPart) -> Any:
        if not parts:
            raise ValueError("a command is required")
        if self._stream is None:
            self.connect()
        assert self._stream is not None
        encoded = [self._encode_part(part) for part in parts]
        payload = b"*" + str(len(encoded)).encode("ascii") + b"\r\n"
        payload += b"".join(
            b"$"
            + str(len(part)).encode("ascii")
            + b"\r\n"
            + part
            + b"\r\n"
            for part in encoded
        )
        try:
            self._stream.write(payload)
            self._stream.flush()
            return self._read_response()
        except (OSError, EOFError) as exc:
            raise MegaCacheClientError(
                "connection to {}:{} failed: {}".format(
                    self.host, self.port, exc
                )
            ) from exc

    def _read_response(self) -> Any:
        assert self._stream is not None
        marker = self._stream.read(1)
        if not marker:
            raise EOFError("server closed the connection")
        line = self._readline()
        if marker == b"+":
            return line.decode("utf-8")
        if marker == b"-":
            raise MegaCacheCommandError(line.decode("utf-8", "replace"))
        if marker == b":":
            try:
                return int(line)
            except ValueError as exc:
                raise MegaCacheProtocolError("invalid integer response") from exc
        if marker == b"$":
            return self._read_bulk(line)
        if marker == b"*":
            try:
                count = int(line)
            except ValueError as exc:
                raise MegaCacheProtocolError("invalid array response") from exc
            if count == -1:
                return None
            if count < 0:
                raise MegaCacheProtocolError("invalid array length")
            return [self._read_response() for _ in range(count)]
        raise MegaCacheProtocolError(
            "unknown RESP marker {!r}".format(marker)
        )

    def _read_bulk(self, length_line: bytes) -> Optional[bytes]:
        assert self._stream is not None
        try:
            length = int(length_line)
        except ValueError as exc:
            raise MegaCacheProtocolError("invalid bulk string length") from exc
        if length == -1:
            return None
        if length < 0:
            raise MegaCacheProtocolError("invalid bulk string length")
        value = self._stream.read(length)
        if len(value) != length or self._stream.read(2) != b"\r\n":
            raise MegaCacheProtocolError("incomplete bulk string response")
        return value

    def _readline(self) -> bytes:
        assert self._stream is not None
        line = self._stream.readline(65_537)
        if len(line) > 65_536 or not line.endswith(b"\r\n"):
            raise MegaCacheProtocolError("unterminated or oversized response")
        return line[:-2]

    @staticmethod
    def _encode_part(value: CommandPart) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, int):
            return str(value).encode("ascii")
        if isinstance(value, str):
            return value.encode("utf-8")
        raise TypeError("command parts must be strings, bytes, or integers")

