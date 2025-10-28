"""
HTTP-specific streaming I/O implementations.

Provides streaming I/O for HTTP/HTTPS using range requests and chunked uploads.
"""

from typing import Optional, Dict, Any

from ..cloud_io import _CloudStorageRaw
from ..cloudpath import register_raw_io_class
from ..enums import FileCacheMode


@register_raw_io_class("http")
@register_raw_io_class("https")
class _HttpStorageRaw(_CloudStorageRaw):
    """
    HTTP-specific raw I/O adapter.

    Implements efficient range-based reads and chunked uploads for HTTP/HTTPS.
    Note: Write operations require the server to support PUT requests.
    """

    def __init__(self, client, cloud_path, mode: str = "rb"):
        """
        Initialize HTTP raw adapter.

        Args:
            client: HttpClient or HttpsClient instance
            cloud_path: HttpPath or HttpsPath instance
            mode: File mode
        """
        super().__init__(client, cloud_path, mode)

        # For chunked uploads
        self._upload_buffer: list = []

    def _range_get(self, start: int, end: int) -> bytes:
        """
        Fetch a byte range from HTTP.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (inclusive)

        Returns:
            Bytes in the requested range
        """
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        """
        Get the total size of the HTTP resource.

        Returns:
            Size in bytes
        """
        return self._client._get_content_length(self._cloud_path)

    def _is_eof_error(self, error: Exception) -> bool:
        """Check if error indicates EOF/out of range."""
        error_str = str(error).lower()
        return (
            "416" in error_str
            or "requested range not satisfiable" in error_str
            or "invalid range" in error_str
        )

    # ---- Write support (chunked upload) ----

    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]] = None) -> None:
        """
        Buffer a chunk of data for HTTP upload.

        Args:
            data: Bytes to upload
            upload_state: Upload state dictionary (unused)
        """
        if not data:
            return

        # Buffer the data for later upload
        self._upload_buffer.append(data)

    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]] = None) -> None:
        """
        Upload buffered data to HTTP using PUT with chunked transfer encoding.

        Args:
            upload_state: Upload state dictionary (unused)
        """
        if not self._upload_buffer:
            # No data uploaded - create empty file
            # Temporarily disable streaming mode to avoid recursion
            original_mode = self._client.file_cache_mode
            try:
                self._client.file_cache_mode = FileCacheMode.tmp_dir
                self._cloud_path.write_bytes(b"")
            finally:
                self._client.file_cache_mode = original_mode
            return

        # Concatenate all buffered chunks
        complete_data = b"".join(self._upload_buffer)

        # Use the client's PUT method to upload
        self._client._put_data(self._cloud_path, complete_data)

        # Clear the buffer
        self._upload_buffer.clear()

    def close(self) -> None:
        """Close and clean up."""
        # Note: The base class close() will call _finalize_upload which handles completion
        super().close()
