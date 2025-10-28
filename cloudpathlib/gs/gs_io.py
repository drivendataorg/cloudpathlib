"""
Google Cloud Storage-specific streaming I/O implementations.

Provides efficient streaming I/O for GCS using range requests and resumable uploads.
"""

from typing import Optional, Dict, Any

from ..cloud_io import _CloudStorageRaw
from ..cloudpath import register_raw_io_class
from ..enums import FileCacheMode


@register_raw_io_class("gs")
class _GSStorageRaw(_CloudStorageRaw):
    """
    GCS-specific raw I/O adapter.

    Implements efficient range-based reads and resumable uploads for GCS.
    """

    def __init__(self, client, cloud_path, mode: str = "rb"):
        """
        Initialize GCS raw adapter.

        Args:
            client: GSClient instance
            cloud_path: GSPath instance
            mode: File mode
        """
        super().__init__(client, cloud_path, mode)

        # For uploads (GCS buffers parts in the client)
        self._upload_id: str = ""
        self._parts: list = []

    def _range_get(self, start: int, end: int) -> bytes:
        """
        Fetch a byte range from GCS.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (inclusive)

        Returns:
            Bytes in the requested range
        """
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        """
        Get the total size of the GCS object.

        Returns:
            Size in bytes
        """
        return self._client._get_content_length(self._cloud_path)

    def _is_eof_error(self, error: Exception) -> bool:
        """Check if error indicates EOF/out of range."""
        error_str = str(error)
        if "416" in error_str or "Requested Range Not Satisfiable" in error_str:
            return True
        if hasattr(error, "code") and error.code == 416:
            return True
        return False

    # ---- Write support (resumable upload) ----

    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]] = None) -> None:
        """
        Buffer data for GCS upload.

        GCS buffers parts in the client and uploads all at once when finalized.

        Args:
            data: Bytes to upload
            upload_state: Upload state dictionary
        """
        if not data:
            return

        # Initialize upload if needed (GCS doesn't need explicit init)
        if not self._upload_id:
            self._upload_id = self._client._initiate_multipart_upload(self._cloud_path)

        # Upload part using client method (which buffers internally for GCS)
        part_number = len(self._parts) + 1
        part_info = self._client._upload_part(self._cloud_path, self._upload_id, part_number, data)
        self._parts.append(part_info)

    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]] = None) -> None:
        """
        Upload buffered data to GCS.

        Args:
            upload_state: Upload state dictionary
        """
        if not self._parts:
            # No data uploaded - create empty file
            # Temporarily disable streaming mode to avoid recursion
            original_mode = self._client.file_cache_mode
            try:
                self._client.file_cache_mode = FileCacheMode.tmp_dir
                self._cloud_path.write_bytes(b"")
            finally:
                self._client.file_cache_mode = original_mode
            return

        try:
            # Complete upload using client method (which uploads all buffered parts)
            self._client._complete_multipart_upload(self._cloud_path, self._upload_id, self._parts)
        finally:
            self._upload_id = ""
            self._parts = []

    def close(self) -> None:
        """Close and clean up."""
        # Note: The base class close() will call _finalize_upload which handles completion
        super().close()
