"""
Azure Blob Storage-specific streaming I/O implementations.

Provides efficient streaming I/O for Azure using range requests and block uploads.
"""

from typing import Optional, Dict, Any

from ..cloud_io import _CloudStorageRaw
from ..cloudpath import register_raw_io_class
from ..enums import FileCacheMode


@register_raw_io_class("azure")
class _AzureBlobStorageRaw(_CloudStorageRaw):
    """
    Azure Blob Storage-specific raw I/O adapter.

    Implements efficient range-based reads and block blob uploads for Azure.
    """

    def __init__(self, client, cloud_path, mode: str = "rb"):
        """
        Initialize Azure raw adapter.

        Args:
            client: AzureBlobClient instance
            cloud_path: AzureBlobPath instance
            mode: File mode
        """
        super().__init__(client, cloud_path, mode)

        # For block blob uploads
        self._upload_id: str = ""  # Azure doesn't use upload IDs
        self._parts: list = []

    def _range_get(self, start: int, end: int) -> bytes:
        """
        Fetch a byte range from Azure Blob Storage.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (inclusive)

        Returns:
            Bytes in the requested range
        """
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        """
        Get the total size of the Azure blob.

        Returns:
            Size in bytes
        """
        return self._client._get_content_length(self._cloud_path)

    def _is_eof_error(self, error: Exception) -> bool:
        """Check if error indicates EOF/out of range."""
        error_str = str(error)
        if "InvalidRange" in error_str or "out of range" in error_str.lower():
            return True
        if hasattr(error, "error_code") and error.error_code == "InvalidRange":
            return True
        return False

    # ---- Write support (block blob upload) ----

    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]] = None) -> None:
        """
        Upload a chunk of data as a block.

        Args:
            data: Bytes to upload
            upload_state: Upload state dictionary
        """
        if not data:
            return

        # Initialize upload if needed (Azure doesn't need explicit init)
        if not self._upload_id:
            self._upload_id = self._client._initiate_multipart_upload(self._cloud_path)

        # Upload block using client method (part_number is 1-indexed)
        part_number = len(self._parts) + 1
        part_info = self._client._upload_part(self._cloud_path, self._upload_id, part_number, data)
        self._parts.append(part_info)

    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]] = None) -> None:
        """
        Finalize Azure block blob upload.

        Args:
            upload_state: Upload state dictionary
        """
        if not self._parts:
            # No blocks uploaded - create empty file
            # Temporarily disable streaming mode to avoid recursion
            original_mode = self._client.file_cache_mode
            try:
                self._client.file_cache_mode = FileCacheMode.tmp_dir
                self._cloud_path.write_bytes(b"")
            finally:
                self._client.file_cache_mode = original_mode
            return

        try:
            # Complete upload using client method
            self._client._complete_multipart_upload(self._cloud_path, self._upload_id, self._parts)
        except Exception as e:
            # Note: Azure will auto-expire uncommitted blocks after 7 days
            raise e
        finally:
            self._upload_id = ""
            self._parts = []

    def close(self) -> None:
        """Close and clean up."""
        # Note: The base class close() will call _finalize_upload which handles completion
        # Azure auto-expires uncommitted blocks after 7 days, so no need for explicit cleanup
        super().close()
