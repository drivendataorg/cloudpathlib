"""
Azure Blob Storage-specific streaming I/O implementations.

Provides efficient streaming I/O for Azure using range requests and block uploads.
"""

from typing import Optional, Dict, Any

from ..cloud_io import _CloudStorageRaw
from ..cloudpath import register_raw_io_class


@register_raw_io_class("azure")
class _AzureBlobStorageRaw(_CloudStorageRaw):
    """
    Azure Blob Storage-specific raw I/O adapter.

    Implements efficient range-based reads and block blob uploads for Azure.
    Each block is staged independently (true streaming) and committed on finalize.
    """

    def __init__(self, client, cloud_path, mode: str = "rb"):
        super().__init__(client, cloud_path, mode)

        # Block blob upload state
        self._upload_id: str = ""  # Azure doesn't use upload IDs
        self._parts: list = []

    def _range_get(self, start: int, end: int) -> bytes:
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        return self._client._get_content_length(self._cloud_path)

    def _is_eof_error(self, error: Exception) -> bool:
        error_str = str(error)
        if "InvalidRange" in error_str or "out of range" in error_str.lower():
            return True
        if hasattr(error, "error_code") and error.error_code == "InvalidRange":
            return True
        return False

    # ---- Write support (Azure block blob upload) ----

    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]] = None) -> None:
        if not data:
            return

        if not self._upload_id:
            self._upload_id = self._client._initiate_multipart_upload(self._cloud_path)

        part_number = len(self._parts) + 1
        part_info = self._client._upload_part(self._cloud_path, self._upload_id, part_number, data)
        self._parts.append(part_info)

    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]] = None) -> None:
        if not self._parts:
            # No blocks staged — create an empty blob directly
            self._client._put_empty_object(self._cloud_path)
            return

        try:
            self._client._complete_multipart_upload(self._cloud_path, self._upload_id, self._parts)
        finally:
            self._upload_id = ""
            self._parts = []

    def close(self) -> None:
        super().close()
