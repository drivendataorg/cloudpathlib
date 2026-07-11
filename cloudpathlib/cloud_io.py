"""Buffered cloud I/O without a local cache."""

from __future__ import annotations

import io
from abc import abstractmethod
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional, Type, Union

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer as _ReadableBuffer
    from _typeshed import WriteableBuffer as _WriteableBuffer
else:
    _ReadableBuffer = Union[bytes, bytearray, memoryview]
    _WriteableBuffer = Union[bytearray, memoryview]

from .client import Client
from .cloudpath import CloudPath


def _validate_file_mode(mode: str) -> None:
    """Validate an ``open`` mode using the same grammar as the stdlib."""
    if not isinstance(mode, str):
        raise TypeError(f"mode must be a string, not {type(mode).__name__}")
    if not mode or any(character not in "rwaxbt+" for character in mode):
        raise ValueError(f"invalid mode: {mode!r}")
    if sum(mode.count(character) for character in "rwax") != 1:
        raise ValueError("must have exactly one of create/read/write/append mode")
    if mode.count("+") > 1 or mode.count("b") > 1 or mode.count("t") > 1:
        raise ValueError(f"invalid mode: {mode!r}")
    if "b" in mode and "t" in mode:
        raise ValueError("can't have text and binary mode at once")


class _CloudStorageRaw(io.RawIOBase):
    """Raw adapter backed by client streaming hooks."""

    def __init__(
        self,
        client: Client,
        cloud_path: CloudPath,
        mode: str = "rb",
    ) -> None:
        super().__init__()
        self._client = client
        self._cloud_path = cloud_path
        self._mode = mode
        self._pos = 0
        self._size: Optional[int] = None
        self._closed = False
        self._upload_error: Optional[BaseException] = None

    def readable(self) -> bool:
        """Return whether object was opened for reading."""
        return "r" in self._mode or "+" in self._mode

    def writable(self) -> bool:
        """Return whether object was opened for writing."""
        return "w" in self._mode or "a" in self._mode or "+" in self._mode or "x" in self._mode

    def seekable(self) -> bool:
        """Return whether object supports random access.

        Streaming writes are sequential-only; seeking is only valid for readable streams.
        """
        return self.readable()

    def readinto(self, b: _WriteableBuffer, /) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        view = memoryview(b).cast("B")
        if len(view) == 0:
            return 0

        start = self._pos
        end = start + len(view) - 1

        if self._size is None:
            try:
                self._size = self._get_size()
            except Exception:
                pass

        if self._size is not None and end >= self._size:
            end = self._size - 1
            if start >= self._size:
                return 0

        try:
            data = self._range_get(start, end)
        except Exception as e:
            if self._is_eof_error(e):
                return 0
            raise

        n = len(data)
        if n == 0:
            return 0

        n = min(n, len(view))
        view[:n] = data[:n]

        self._pos += n
        return n

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """
        Change stream position.

        Args:
            offset: Offset in bytes
            whence: Position to seek from (SEEK_SET, SEEK_CUR, SEEK_END)

        Returns:
            New absolute position
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            if self._size is None:
                self._size = self._get_size()
            if self._size is None:
                raise OSError("Unable to determine file size for SEEK_END")
            new_pos = self._size + offset
        else:
            raise ValueError(
                f"invalid whence ({whence}, should be {io.SEEK_SET}, "
                f"{io.SEEK_CUR}, or {io.SEEK_END})"
            )

        if new_pos < 0:
            raise ValueError("negative seek position")

        self._pos = new_pos
        return self._pos

    def tell(self) -> int:
        """Return current stream position."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        return self._pos

    def write(self, b: _ReadableBuffer, /) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        if self._upload_error is not None:
            raise self._upload_error

        data = bytes(b)
        try:
            self._upload_chunk(data)
        except BaseException as error:
            self._upload_error = error
            raise
        return len(data)

    def close(self) -> None:
        """Close the file."""
        if self._closed:
            return

        self._closed = True

        try:
            if self.writable() and self._upload_error is not None:
                try:
                    self._abort_upload()
                except Exception:
                    pass
                finally:
                    raise self._upload_error
            if self.writable():
                try:
                    self._finalize_upload()
                except BaseException:
                    try:
                        self._abort_upload()
                    except Exception:
                        pass
                    raise
        finally:
            super().close()

    def _abort_upload(self) -> None:
        """Best-effort cleanup after a write or finalization failure."""
        pass

    @abstractmethod
    def _upload_chunk(self, data: bytes) -> None:
        pass

    @abstractmethod
    def _finalize_upload(self) -> None:
        pass

    def _range_get(self, start: int, end: int) -> bytes:
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        return self._client._get_content_length(self._cloud_path)

    def _is_eof_error(self, error: Exception) -> bool:
        """
        Check if an error indicates EOF/out of range.

        Override in subclasses for provider-specific error handling.
        """
        return False


class _CloudMultipartStorageRaw(_CloudStorageRaw):
    """Shared buffered multipart upload lifecycle."""

    _INITIAL_PART_SIZE: int
    _MAX_PART_SIZE: int
    _MAX_PARTS: int
    _PARTS_PER_SIZE_TIER: int
    _PROVIDER_NAME: str

    def __init__(self, client: Client, cloud_path: CloudPath, mode: str = "rb") -> None:
        super().__init__(client, cloud_path, mode)
        self._upload_id: Optional[str] = None
        self._parts: list[dict[str, Any]] = []
        self._part_number = 1
        self._write_buffer = bytearray()

    @classmethod
    def _part_size_for_number(cls, part_number: int) -> int:
        tier = (part_number - 1) // cls._PARTS_PER_SIZE_TIER
        return min(cls._INITIAL_PART_SIZE * (2**tier), cls._MAX_PART_SIZE)

    def _target_part_size(self) -> int:
        return self._part_size_for_number(self._part_number)

    def _check_part_limit(self) -> None:
        if self._part_number > self._MAX_PARTS:
            raise OSError(
                f"{self._PROVIDER_NAME} upload exceeded the {self._MAX_PARTS:,}-part limit"
            )

    def _upload_buffered_part(self, size: int) -> None:
        self._check_part_limit()
        if self._upload_id is None:
            self._upload_id = self._client._initiate_multipart_upload(self._cloud_path)
        data = bytes(self._write_buffer[:size])
        part = self._client._upload_part(
            self._cloud_path, self._upload_id, self._part_number, data
        )
        del self._write_buffer[:size]
        self._parts.append(part)
        self._part_number += 1

    def _upload_chunk(self, data: bytes) -> None:
        if not data:
            return
        self._write_buffer.extend(data)
        target_size = self._target_part_size()
        while len(self._write_buffer) >= target_size:
            self._upload_buffered_part(target_size)
            target_size = self._target_part_size()

    def _finalize_upload(self) -> None:
        if self._write_buffer:
            self._upload_buffered_part(len(self._write_buffer))
        if self._upload_id is None:
            self._client._put_empty_object(self._cloud_path)
            return
        self._client._complete_multipart_upload(self._cloud_path, self._upload_id, self._parts)
        self._reset_upload()

    def _abort_upload(self) -> None:
        try:
            if self._upload_id is not None:
                self._client._abort_multipart_upload(self._cloud_path, self._upload_id)
        finally:
            self._reset_upload()

    def _reset_upload(self) -> None:
        self._upload_id = None
        self._parts.clear()
        self._part_number = 1
        self._write_buffer.clear()


class CloudBufferedIO(io.BufferedIOBase):
    """Buffered binary I/O backed by a cloud client."""

    def __init__(
        self,
        raw_io_class: Type[_CloudStorageRaw],
        client: Client,
        cloud_path: CloudPath,
        mode: str = "rb",
        buffer_size: int = 64 * 1024,
    ) -> None:
        _validate_file_mode(mode)
        if "b" not in mode:
            raise ValueError("CloudBufferedIO requires binary mode (must include 'b')")
        if "a" in mode or "+" in mode:
            raise io.UnsupportedOperation(
                "append and update modes require the local-cache implementation"
            )

        raw = raw_io_class(client, cloud_path, mode)

        if "r" in mode:
            self._buffer: Union[io.BufferedReader, io.BufferedWriter]
            self._buffer = io.BufferedReader(raw, buffer_size=buffer_size)  # type: ignore[arg-type,assignment]
        else:
            self._buffer = io.BufferedWriter(raw, buffer_size=buffer_size)  # type: ignore[arg-type,assignment]

        self._cloud_path = cloud_path
        self._mode = mode
        self._buffer_size_val = buffer_size

    @property
    def name(self) -> str:
        """File name (the cloud URL)."""
        return str(self._cloud_path)

    @property
    def mode(self) -> str:
        """File mode."""
        return self._mode

    @property
    def _buffer_size(self) -> int:
        """Buffer size for compatibility with tests."""
        return self._buffer_size_val

    def read(self, size: Optional[int] = -1, /) -> bytes:
        return self._buffer.read(size)

    def read1(self, size: int = -1, /) -> bytes:
        return self._buffer.read1(size)  # type: ignore[attr-defined]

    def readinto(self, b: _WriteableBuffer, /) -> int:
        return self._buffer.readinto(b)

    def readinto1(self, b: _WriteableBuffer, /) -> int:
        return self._buffer.readinto1(b)  # type: ignore[attr-defined]

    def write(self, b: _ReadableBuffer, /) -> int:
        return self._buffer.write(b)

    def seek(self, offset: int, whence: int = io.SEEK_SET, /) -> int:
        return self._buffer.seek(offset, whence)

    def tell(self) -> int:
        return self._buffer.tell()

    def flush(self) -> None:
        self._buffer.flush()

    def close(self) -> None:
        if hasattr(self, "_buffer") and not self._buffer.closed:
            self._buffer.close()

    def readable(self) -> bool:
        return self._buffer.readable()

    def writable(self) -> bool:
        return self._buffer.writable()

    def seekable(self) -> bool:
        return self._buffer.seekable()

    @property
    def closed(self) -> bool:
        return self._buffer.closed

    def __enter__(self) -> CloudBufferedIO:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
        /,
    ) -> None:
        self.close()


class CloudTextIO(io.TextIOWrapper):
    """Text I/O backed by a cloud client."""

    def __init__(
        self,
        raw_io_class: Type[_CloudStorageRaw],
        client: Client,
        cloud_path: CloudPath,
        mode: str = "rt",
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        buffer_size: int = 64 * 1024,
        line_buffering: bool = False,
    ) -> None:
        _validate_file_mode(mode)
        if "b" in mode:
            raise ValueError("CloudTextIO requires text mode (no 'b' in mode)")
        if "a" in mode or "+" in mode:
            raise io.UnsupportedOperation(
                "append and update modes require the local-cache implementation"
            )

        if "t" not in mode and "r" in mode:
            binary_mode = mode.replace("r", "rb", 1)
        elif "t" not in mode and "w" in mode:
            binary_mode = mode.replace("w", "wb", 1)
        elif "t" not in mode and "a" in mode:
            binary_mode = mode.replace("a", "ab", 1)
        elif "t" not in mode and "x" in mode:
            binary_mode = mode.replace("x", "xb", 1)
        else:
            binary_mode = mode.replace("t", "b")

        buffered = CloudBufferedIO(
            raw_io_class, client, cloud_path, mode=binary_mode, buffer_size=buffer_size
        )

        super().__init__(
            buffered,
            encoding=encoding,
            errors=errors,
            newline=newline,
            line_buffering=line_buffering,
        )

        self._cloud_path = cloud_path
        self._mode = mode

    @property
    def name(self) -> str:
        """File name (the cloud URL)."""
        return str(self._cloud_path)

    @property
    def mode(self) -> str:
        """File mode."""
        return self._mode
