"""
S3-specific streaming I/O implementations.

Provides efficient streaming I/O for S3 using range requests and multipart uploads.
"""

from typing import Optional, Dict, Any

from ..cloud_io import _CloudStorageRaw
from ..cloudpath import register_raw_io_class
from ..enums import FileCacheMode


@register_raw_io_class("s3")
class _S3StorageRaw(_CloudStorageRaw):
    """
    S3-specific raw I/O adapter.

    Implements efficient range-based reads and multipart uploads for S3.
    """

    def __init__(self, client, cloud_path, mode: str = "rb"):
        """
        Initialize S3 raw adapter.

        Args:
            client: S3Client instance
            cloud_path: S3Path instance
            mode: File mode
        """
        super().__init__(client, cloud_path, mode)

        # For multipart uploads
        self._upload_id: Optional[str] = None
        self._parts: list = []
        self._part_number: int = 1

    def _range_get(self, start: int, end: int) -> bytes:
        """
        Fetch a byte range from S3.

        Args:
            start: Start byte position (inclusive)
            end: End byte position (inclusive)

        Returns:
            Bytes in the requested range
        """
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        """
        Get the total size of the S3 object.

        Returns:
            Size in bytes
        """
        return self._client._get_content_length(self._cloud_path)

    def _is_eof_error(self, error: Exception) -> bool:
        """Check if error indicates EOF/out of range."""
        error_str = str(error)
        return (
            "InvalidRange" in error_str
            or "InvalidObjectState" in error_str
            or hasattr(error, "__class__")
            and "InvalidRange" in error.__class__.__name__
        )

    # ---- Write support (multipart upload) ----

    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]] = None) -> None:
        """
        Upload a chunk of data using multipart upload.

        Args:
            data: Bytes to upload
            upload_state: Upload state dictionary
        """
        if not data:
            return

        # Initialize multipart upload if needed
        if self._upload_id is None:
            self._upload_id = self._client._initiate_multipart_upload(self._cloud_path)

        # Upload part using client method
        part_info = self._client._upload_part(
            self._cloud_path, self._upload_id, self._part_number, data
        )
        self._parts.append(part_info)
        self._part_number += 1

    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]] = None) -> None:
        """
        Finalize S3 multipart upload.

        Args:
            upload_state: Upload state dictionary
        """
        if self._upload_id is None:
            # No upload was started - create empty file
            # Temporarily disable streaming mode to avoid recursion
            original_mode = self._client.file_cache_mode
            try:
                self._client.file_cache_mode = FileCacheMode.tmp_dir
                self._cloud_path.write_bytes(b"")
            finally:
                self._client.file_cache_mode = original_mode
            return

        try:
            # Complete multipart upload using client method
            self._client._complete_multipart_upload(self._cloud_path, self._upload_id, self._parts)
        except Exception as e:
            # Abort upload on failure
            try:
                self._client._abort_multipart_upload(self._cloud_path, self._upload_id)
            except Exception:
                pass  # Best effort abort
            raise e
        finally:
            self._upload_id = None
            self._parts = []
            self._part_number = 1

    def close(self) -> None:
        """Close and clean up."""
        # Note: We can't check self._closed here because the base close() will set it
        # The base class close() will call _finalize_upload which clears _upload_id
        # So we don't need to do anything special here - just delegate to base
        super().close()
