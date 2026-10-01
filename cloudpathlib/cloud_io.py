"""Buffered cloud I/O without a local cache."""

from __future__ import annotations

import io
from abc import abstractmethod
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import IO, TYPE_CHECKING, Any, Callable, Dict, Optional, Type, Union
import warnings

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer as _ReadableBuffer
    from _typeshed import WriteableBuffer as _WriteableBuffer
else:
    _ReadableBuffer = Union[bytes, bytearray, memoryview]
    _WriteableBuffer = Union[bytearray, memoryview]

if TYPE_CHECKING:
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
        pre_finalize: Optional[Callable[[], None]] = None,
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
        self._pre_finalize = pre_finalize
        # concurrent requests for this stream (read prefetch / background part uploads)
        self._max_concurrency = max(1, int(getattr(client, "streaming_max_concurrency", 1)))
        self._executor: Optional[ThreadPoolExecutor] = None
        self._prefetch: Dict[int, Future] = {}

    @property
    def name(self) -> str:
        """The cloud URL; surfaced as `.name` by the buffered and text wrappers too."""
        return str(self._cloud_path)

    @property
    def mode(self) -> str:
        return self._mode

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
    """Buffered multipart upload lifecycle shared by S3, Azure, and GCS.

    Part sizes start at the client's minimum part size and double every
    `_PARTS_PER_SIZE_TIER` parts (capped at the maximum part size) so that very large
    streams stay within the provider's part-count limit.
    """

    _PARTS_PER_SIZE_TIER = 1_000

    def __init__(
        self,
        client: Client,
        cloud_path: CloudPath,
        mode: str = "rb",
        pre_finalize: Optional[Callable[[], None]] = None,
    ) -> None:
        super().__init__(client, cloud_path, mode, pre_finalize)
        self._min_part_size = client._multipart_min_part_size
        self._max_part_size = client._multipart_max_part_size
        self._max_parts = client._multipart_max_parts
        self._upload_id: Optional[str] = None
        self._parts: Dict[int, dict[str, Any]] = {}
        self._part_futures: Dict[int, Future] = {}
        self._part_number = 1
        self._write_buffer = bytearray()

    def _part_size_for_number(self, part_number: int) -> int:
        tier = (part_number - 1) // self._PARTS_PER_SIZE_TIER
        return min(self._min_part_size * (2**tier), self._max_part_size)

    def _target_part_size(self) -> int:
        return self._part_size_for_number(self._part_number)

    def _check_part_limit(self) -> None:
        if self._part_number > self._max_parts:
            raise OSError(
                f"{type(self._client).__name__} multipart upload exceeded the "
                f"{self._max_parts:,}-part limit"
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


def open_stream(
    raw_io_class: Type[_CloudStorageRaw],
    client: Client,
    cloud_path: CloudPath,
    mode: str,
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    pre_finalize: Optional[Callable[[], None]] = None,
) -> IO[Any]:
    """Build a file object over a provider raw stream the way the builtin `open` does.

    Returns the raw stream for `buffering=0`, an `io.BufferedReader`/`io.BufferedWriter`
    for binary modes, and an `io.TextIOWrapper` for text modes. `buffering` has the same
    meaning as for `open`: `-1` for the default buffer size, `1` for line buffering in
    text mode, otherwise the buffer size in bytes (which is also the size of each ranged
    request for reads).
    """
    _validate_file_mode(mode)
    binary = "b" in mode
    if "a" in mode or "+" in mode:
        raise io.UnsupportedOperation(
            "append and update modes require the local-cache implementation"
        )
    if binary and buffering == 1:
        warnings.warn(
            "line buffering (buffering=1) isn't supported in binary mode, "
            "the default buffer size will be used",
            RuntimeWarning,
            stacklevel=2,
        )
        buffering = -1
    if not binary and buffering == 0:
        raise ValueError("can't have unbuffered text I/O")

    raw_mode = mode.replace("t", "") if binary else mode.replace("t", "") + "b"
    raw = raw_io_class(client, cloud_path, raw_mode, pre_finalize)
    if buffering == 0:
        return raw  # type: ignore[return-value]

    line_buffering = buffering == 1
    buffer_size = DEFAULT_BUFFER_SIZE if buffering < 2 else buffering
    buffered: Union[io.BufferedReader, io.BufferedWriter]
    if "r" in mode:
        buffered = io.BufferedReader(raw, buffer_size)
    else:
        buffered = io.BufferedWriter(raw, buffer_size)
    if binary:
        return buffered  # type: ignore[return-value]

    text = io.TextIOWrapper(
        buffered,
        encoding=encoding,
        errors=errors,
        newline=newline,
        line_buffering=line_buffering,
    )
    # TextIOWrapper pulls from the buffer in 8 KiB `read1` calls, and BufferedReader
    # passes a `read1` straight through to the raw stream when its buffer is empty; without
    # this, every text read would become an 8 KiB ranged request regardless of `buffering`.
    text._CHUNK_SIZE = buffer_size  # type: ignore[attr-defined]
    text.mode = mode  # type: ignore[misc]  # the builtin open sets this too
    return text
