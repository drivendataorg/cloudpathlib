"""
Cloud storage streaming I/O implementations.

Provides BufferedIOBase and TextIOBase compliant file-like objects for cloud storage
that support efficient streaming with range requests and multipart uploads, without
requiring full local caching.
"""

import io
from abc import abstractmethod
from typing import Optional, Any, Type, Union, Dict


# ============================================================================
# Base Raw I/O Adapter (internal)
# ============================================================================


class _CloudStorageRaw(io.RawIOBase):
    """
    Internal raw I/O adapter for cloud storage objects.

    Implements efficient range-based reads using cloud provider APIs.
    Not exposed to users - internal implementation detail.
    """

    def __init__(
        self,
        client: Any,
        cloud_path: Any,
        mode: str = "rb",
    ):
        """
        Initialize raw cloud storage adapter.

        Args:
            client: Cloud provider client (S3Client, AzureBlobClient, etc.)
            cloud_path: CloudPath instance
            mode: File mode (currently only read modes supported in base)
        """
        super().__init__()
        self._client = client
        self._cloud_path = cloud_path
        self._mode = mode
        self._pos = 0
        self._size: Optional[int] = None
        self._closed = False

    def readable(self) -> bool:
        """Return whether object was opened for reading."""
        return "r" in self._mode or "+" in self._mode

    def writable(self) -> bool:
        """Return whether object was opened for writing."""
        return "w" in self._mode or "a" in self._mode or "+" in self._mode or "x" in self._mode

    def seekable(self) -> bool:
        """Return whether object supports random access."""
        return True

    def readinto(self, b: bytearray) -> int:  # type: ignore[override]
        """
        Read bytes into a pre-allocated buffer.

        Args:
            b: Buffer to read data into

        Returns:
            Number of bytes read (0 at EOF)
        """
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        if len(b) == 0:
            return 0

        # Calculate range to fetch
        start = self._pos
        end = start + len(b) - 1

        # Clamp end to file size if known (prevents 416 errors)
        if self._size is None:
            try:
                self._size = self._get_size()
            except Exception:
                # If we can't get size, try the request anyway
                pass

        if self._size is not None and end >= self._size:
            # Clamp to last valid byte
            end = self._size - 1
            if start >= self._size:
                # Already at EOF
                return 0

        # Fetch data from cloud storage
        try:
            data = self._range_get(start, end)
        except Exception as e:
            # If we get an error reading beyond EOF, treat as EOF
            if self._is_eof_error(e):
                return 0
            raise

        # Copy data into buffer
        n = len(data)
        if n == 0:
            return 0

        # Ensure we don't write more than the buffer can hold
        n = min(n, len(b))

        # Use the most compatible approach: byte-by-byte copy
        # This works with all buffer types (bytearray, memoryview, etc.)
        try:
            # Try the fast path first (works for most cases)
            b[:n] = data[:n]
        except (ValueError, TypeError, IndexError):
            # Fall back to byte-by-byte copy for complex memoryview structures
            for i in range(n):
                try:
                    b[i] = data[i]
                except IndexError:
                    # If we hit an index error, stop here
                    n = i
                    break

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

    def write(self, b: bytes) -> int:  # type: ignore[override]
        """
        Write bytes to the stream.

        This method is required by RawIOBase for writable streams.
        The actual implementation is delegated to subclasses via _upload_chunk.

        Args:
            b: Bytes to write

        Returns:
            Number of bytes written
        """
        if not self.writable():
            raise io.UnsupportedOperation("not writable")

        # Delegate to subclass implementation
        # Note: Don't check _closed here because BufferedWriter may call write() during close/flush
        self._upload_chunk(bytes(b), None)
        return len(b)

    def close(self) -> None:
        """Close the file."""
        if self._closed:
            return

        # Mark as closed FIRST to prevent recursive calls
        self._closed = True

        # Finalize any pending writes
        if self.writable():
            try:
                self._finalize_upload(None)
            except Exception:
                # If finalization fails, already marked as closed
                pass

        # Call parent close() to properly set the closed state
        # This is required for the `closed` property to return True
        super().close()

    @abstractmethod
    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]]) -> None:
        """
        Upload a chunk of data.

        Args:
            data: Bytes to upload
            upload_state: Upload state dictionary (for multipart uploads)
        """
        pass

    @abstractmethod
    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]]) -> None:
        """
        Finalize the upload process.

        Args:
            upload_state: Upload state dictionary (for multipart uploads)
        """
        pass

    # Abstract methods to be implemented by subclasses

    @abstractmethod
    def _range_get(self, start: int, end: int) -> bytes:
        """
        Fetch a byte range from cloud storage.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (inclusive)

        Returns:
            Bytes in the requested range
        """
        pass

    @abstractmethod
    def _get_size(self) -> int:
        """
        Get the total size of the cloud object.

        Returns:
            Size in bytes
        """
        pass

    def _is_eof_error(self, error: Exception) -> bool:
        """
        Check if an error indicates EOF/out of range.

        Override in subclasses for provider-specific error handling.
        """
        return False


# ============================================================================
# Public Buffered Binary I/O
# ============================================================================


class CloudBufferedIO(io.BufferedIOBase):
    """
    Buffered binary file-like object for cloud storage.

    Wraps a raw cloud storage adapter with Python's standard buffered I/O classes
    (BufferedReader, BufferedWriter, or BufferedRandom) based on the mode.

    Example:
        >>> from cloudpathlib import S3Client
        >>> client = S3Client()
        >>> with CloudBufferedIO(client, "s3://bucket/file.bin", mode="rb") as f:
        ...     data = f.read(1024)
    """

    def __init__(
        self,
        raw_io_class: Type[_CloudStorageRaw],
        client: Any,
        cloud_path: Any,
        mode: str = "rb",
        buffer_size: int = 64 * 1024,
    ):
        """
        Initialize cloud buffered I/O.

        Args:
            raw_io_class: The raw I/O class to use for this provider
            client: Cloud provider client instance
            cloud_path: CloudPath instance
            mode: File mode ('rb', 'wb', 'ab', 'r+b', 'w+b', 'a+b', 'xb')
            buffer_size: Size of read/write buffer in bytes (default 64 KiB)
        """
        if "b" not in mode:
            raise ValueError("CloudBufferedIO requires binary mode (must include 'b')")

        # Create raw adapter using provided class
        raw = raw_io_class(client, cloud_path, mode)

        # Choose appropriate buffered class based on mode
        if "+" in mode:
            # Read and write (e.g., 'r+b', 'w+b')
            self._buffer: Union[io.BufferedReader, io.BufferedWriter, io.BufferedRandom] = io.BufferedRandom(raw, buffer_size=buffer_size)  # type: ignore[arg-type]
        elif "r" in mode:
            # Read only (e.g., 'rb')
            self._buffer = io.BufferedReader(raw, buffer_size=buffer_size)  # type: ignore[arg-type,assignment]
        else:
            # Write only (e.g., 'wb', 'ab', 'xb')
            self._buffer = io.BufferedWriter(raw, buffer_size=buffer_size)  # type: ignore[arg-type,assignment]

        # Store additional attributes
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

    # Delegate all I/O methods to the internal buffer
    def read(self, size: Optional[int] = -1) -> bytes:  # type: ignore[override]
        return self._buffer.read(size)

    def read1(self, size: int = -1) -> bytes:
        return self._buffer.read1(size)  # type: ignore[attr-defined]

    def readinto(self, b):
        return self._buffer.readinto(b)

    def readinto1(self, b):
        return self._buffer.readinto1(b)  # type: ignore[attr-defined]

    def write(self, b):
        return self._buffer.write(b)

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._buffer.seek(offset, whence)

    def tell(self) -> int:
        return self._buffer.tell()

    def flush(self):
        return self._buffer.flush()

    def close(self):
        if hasattr(self, "_buffer") and not self._buffer.closed:
            # Flush the buffer first (which will call write() on the raw stream)
            try:
                self._buffer.flush()
            except Exception:
                pass
            # Then close the raw stream (which will finalize uploads)
            if hasattr(self._buffer, "raw") and not self._buffer.raw.closed:
                self._buffer.raw.close()
            # Mark buffer as closed by setting the internal state
            # We can't call self._buffer.close() because it would try to flush again
            # and call raw.close() again, causing issues
            try:
                # Try to access the internal _closed attribute
                if hasattr(self._buffer, "_closed"):
                    object.__setattr__(self._buffer, "_closed", True)
            except Exception:
                pass

    def readable(self) -> bool:
        return self._buffer.readable()

    def writable(self) -> bool:
        return self._buffer.writable()

    def seekable(self) -> bool:
        return self._buffer.seekable()

    @property
    def closed(self) -> bool:
        return self._buffer.closed

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ============================================================================
# Public Text I/O
# ============================================================================


class CloudTextIO(io.TextIOWrapper):
    """
    Text file-like object for cloud storage.

    Implements TextIOBase for seamless integration with standard Python I/O
    and third-party libraries. Handles encoding/decoding and newline translation.

    Example:
        >>> from cloudpathlib import S3Client
        >>> client = S3Client()
        >>> with CloudTextIO(client, "s3://bucket/file.txt", mode="rt") as f:
        ...     text = f.read()
    """

    def __init__(
        self,
        raw_io_class: Type[_CloudStorageRaw],
        client: Any,
        cloud_path: Any,
        mode: str = "rt",
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        newline: Optional[str] = None,
        buffer_size: int = 64 * 1024,
    ):
        """
        Initialize cloud text I/O.

        Args:
            raw_io_class: The raw I/O class to use for this provider
            client: Cloud provider client instance
            cloud_path: CloudPath instance
            mode: File mode ('rt', 'wt', 'at', 'r+t', 'w+t', 'a+t', 'xt', or same without 't')
            encoding: Text encoding (default: utf-8)
            errors: Error handling strategy (default: strict)
            newline: Newline handling (None, '', '\\n', '\\r', '\\r\\n')
            buffer_size: Size of buffer in bytes
        """
        if "b" in mode:
            raise ValueError("CloudTextIO requires text mode (no 'b' in mode)")

        # Ensure mode has 't' or is text mode
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

        # Create underlying buffered I/O
        buffered = CloudBufferedIO(
            raw_io_class, client, cloud_path, mode=binary_mode, buffer_size=buffer_size
        )

        # Initialize TextIOWrapper with the buffered stream
        super().__init__(
            buffered,
            encoding=encoding or "utf-8",
            errors=errors,
            newline=newline,
        )

        # Store additional attributes
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
