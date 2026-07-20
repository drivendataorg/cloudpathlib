"""Buffered cloud I/O without a local cache."""

from __future__ import annotations

import io
from abc import abstractmethod
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from types import TracebackType
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type, Union

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer as _ReadableBuffer
    from _typeshed import WriteableBuffer as _WriteableBuffer
else:
    _ReadableBuffer = Union[bytes, bytearray, memoryview]
    _WriteableBuffer = Union[bytearray, memoryview]

from .client import Client
from .cloudpath import CloudPath

# Bytes fetched/buffered per request for buffered streaming I/O. Sized to match the
# multi-MiB block sizes used by comparable tools (fsspec/s3fs/gcsfs) so per-request
# latency does not dominate sequential throughput; reads never fetch past EOF, so
# small objects only pay for their actual size.
DEFAULT_BUFFER_SIZE = 5 * 1024 * 1024


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
        self._size_fetch_failed = False
        self._closed = False
        self._upload_error: Optional[BaseException] = None
        # Optional conflict check run just before a write is finalized (set by CloudPath.open)
        self._pre_finalize: Optional[Callable[[], None]] = None
        # concurrent requests for this stream (read prefetch / background part uploads)
        self._max_concurrency = max(1, int(getattr(client, "streaming_max_concurrency", 1)))
        self._executor: Optional[ThreadPoolExecutor] = None
        self._prefetch: Dict[int, Future] = {}

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

        size = self._known_size()
        if size is not None and end >= size:
            end = size - 1
            if start >= size:
                return 0

        try:
            data = self._fetch_range(start, end)
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

    def readall(self) -> bytes:
        """Read from the current position to EOF in a single ranged request when possible."""
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if not self.readable():
            raise io.UnsupportedOperation("not readable")

        self._discard_prefetch()
        size = self._known_size()
        if size is None:
            # Size unknown: fall back to the default chunked read loop.
            return super().readall()
        if self._pos >= size:
            return b""

        data = self._range_get(self._pos, size - 1)
        self._pos += len(data)
        return data

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
        if not self.seekable():
            raise io.UnsupportedOperation("seek")

        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            size = self._known_size()
            if size is None:
                raise OSError("Unable to determine file size for SEEK_END")
            new_pos = size + offset
        else:
            raise ValueError(
                f"invalid whence ({whence}, should be {io.SEEK_SET}, "
                f"{io.SEEK_CUR}, or {io.SEEK_END})"
            )

        if new_pos < 0:
            raise ValueError("negative seek position")

        if new_pos != self._pos:
            self._discard_prefetch()
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
        self._pos += len(data)
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
                    if self._pre_finalize is not None:
                        self._pre_finalize()
                    self._finalize_upload()
                except BaseException:
                    try:
                        self._abort_upload()
                    except Exception:
                        pass
                    raise
        finally:
            self._shutdown_executor()
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

    def _ensure_executor(self) -> Optional[ThreadPoolExecutor]:
        """Thread pool for this stream's background requests; None when sequential."""
        if self._max_concurrency <= 1:
            return None
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_concurrency, thread_name_prefix="cloudpathlib-stream"
            )
        return self._executor

    def _shutdown_executor(self) -> None:
        self._discard_prefetch()
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None

    def _discard_prefetch(self) -> None:
        for future in self._prefetch.values():
            future.cancel()
        self._prefetch.clear()

    def _fetch_range(self, start: int, end: int) -> bytes:
        """Fetch [start, end], serving from and topping up background prefetch when enabled."""
        executor = self._ensure_executor()
        if executor is None:
            return self._range_get(start, end)

        chunk_len = end - start + 1
        future = self._prefetch.pop(start, None)
        if future is not None:
            # a short prefetched chunk is a legal short read for RawIOBase consumers
            data = future.result()
            self._schedule_prefetch(start + max(len(data), 1), chunk_len)
            return data

        # position changed or first read: pending prefetches no longer line up
        self._discard_prefetch()
        data = self._range_get(start, end)
        self._schedule_prefetch(end + 1, chunk_len)
        return data

    def _schedule_prefetch(self, next_start: int, chunk_len: int) -> None:
        """Queue reads ahead of the current position, up to the concurrency window."""
        size = self._known_size()
        executor = self._ensure_executor()
        if size is None or chunk_len <= 0 or executor is None:
            return
        start = next_start
        while len(self._prefetch) < self._max_concurrency and start < size:
            if start not in self._prefetch:
                self._prefetch[start] = executor.submit(
                    self._range_get, start, min(start + chunk_len, size) - 1
                )
            start += chunk_len

    def _range_get(self, start: int, end: int) -> bytes:
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        return self._client._get_content_length(self._cloud_path)

    def _known_size(self) -> Optional[int]:
        """Fetch and memoize the object size, attempting the lookup at most once."""
        if self._size is None and not self._size_fetch_failed:
            try:
                self._size = self._get_size()
            except Exception:
                self._size_fetch_failed = True
        return self._size

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
        self._parts: Dict[int, dict[str, Any]] = {}
        self._part_futures: Dict[int, Future] = {}
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
        del self._write_buffer[:size]
        part_number = self._part_number
        self._part_number += 1

        executor = self._ensure_executor()
        if executor is None:
            self._parts[part_number] = self._client._upload_part(
                self._cloud_path, self._upload_id, part_number, data
            )
            return

        # bound in-flight parts (and their buffered bytes) to the concurrency window
        self._harvest_part_futures(block=len(self._part_futures) >= self._max_concurrency)
        self._part_futures[part_number] = executor.submit(
            self._client._upload_part, self._cloud_path, self._upload_id, part_number, data
        )

    def _harvest_part_futures(self, block: bool = False, drain: bool = False) -> None:
        """Collect finished background part uploads, re-raising the first failure."""
        if not self._part_futures:
            return
        if drain:
            wait(list(self._part_futures.values()))
        elif block:
            wait(list(self._part_futures.values()), return_when=FIRST_COMPLETED)

        error: Optional[BaseException] = None
        for part_number in [n for n, f in self._part_futures.items() if f.done()]:
            future = self._part_futures.pop(part_number)
            try:
                self._parts[part_number] = future.result()
            except BaseException as e:
                if error is None:
                    error = e
        if error is not None:
            raise error

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
        self._harvest_part_futures(drain=True)
        if self._upload_id is None:
            self._client._put_empty_object(self._cloud_path)
            return
        ordered_parts = [self._parts[number] for number in sorted(self._parts)]
        self._client._complete_multipart_upload(self._cloud_path, self._upload_id, ordered_parts)
        self._reset_upload()

    def _abort_upload(self) -> None:
        try:
            for future in self._part_futures.values():
                future.cancel()
            wait(list(self._part_futures.values()))
            if self._upload_id is not None:
                self._client._abort_multipart_upload(self._cloud_path, self._upload_id)
        finally:
            self._reset_upload()

    def _reset_upload(self) -> None:
        self._upload_id = None
        self._parts.clear()
        self._part_futures.clear()
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
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        pre_finalize: Optional[Callable[[], None]] = None,
    ) -> None:
        _validate_file_mode(mode)
        if "b" not in mode:
            raise ValueError("CloudBufferedIO requires binary mode (must include 'b')")
        if "a" in mode or "+" in mode:
            raise io.UnsupportedOperation(
                "append and update modes require the local-cache implementation"
            )

        raw = raw_io_class(client, cloud_path, mode)
        if pre_finalize is not None:
            raw._pre_finalize = pre_finalize

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
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        line_buffering: bool = False,
        pre_finalize: Optional[Callable[[], None]] = None,
    ) -> None:
        _validate_file_mode(mode)
        if "b" in mode:
            raise ValueError("CloudTextIO requires text mode (no 'b' in mode)")
        if "a" in mode or "+" in mode:
            raise io.UnsupportedOperation(
                "append and update modes require the local-cache implementation"
            )

        # only r/w/x can reach here: 'b' was rejected above and 'a'/'+' raised earlier
        if "t" not in mode and "r" in mode:
            binary_mode = mode.replace("r", "rb", 1)
        elif "t" not in mode and "w" in mode:
            binary_mode = mode.replace("w", "wb", 1)
        elif "t" not in mode and "x" in mode:
            binary_mode = mode.replace("x", "xb", 1)
        else:
            binary_mode = mode.replace("t", "b")

        buffered = CloudBufferedIO(
            raw_io_class,
            client,
            cloud_path,
            mode=binary_mode,
            buffer_size=buffer_size,
            pre_finalize=pre_finalize,
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
