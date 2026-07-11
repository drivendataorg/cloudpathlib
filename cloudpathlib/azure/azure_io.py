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
    Writes are accumulated and staged in larger blocks to stay under Azure's
    50,000 committed-block limit per blob.
    """

    # Target block size before staging (Azure allows up to 50,000 blocks per blob)
    _BLOCK_SIZE = 4 * 1024 * 1024
    _MAX_BLOCK_SIZE = 4_000 * 1024 * 1024
    _MAX_BLOCKS = 50_000
    _BLOCKS_PER_SIZE_TIER = 1_000

    def __init__(self, client, cloud_path, mode: str = "rb"):
        super().__init__(client, cloud_path, mode)

        # Block blob upload state
        self._upload_id: str = ""  # Azure doesn't use upload IDs
        self._parts: list = []
        self._part_number: int = 1
        self._write_buffer: bytearray = bytearray()

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

    def _target_block_size(self) -> int:
        tier = (self._part_number - 1) // self._BLOCKS_PER_SIZE_TIER
        return min(self._BLOCK_SIZE * (2**tier), self._MAX_BLOCK_SIZE)

    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]] = None) -> None:
        if not data:
            return

        self._write_buffer.extend(data)

        target_block_size = self._target_block_size()
        while len(self._write_buffer) >= target_block_size:
            if self._part_number > self._MAX_BLOCKS:
                raise OSError("Azure block upload exceeded the 50,000-block limit")
            chunk = bytes(self._write_buffer[:target_block_size])
            if not self._upload_id:
                self._upload_id = self._client._initiate_multipart_upload(self._cloud_path)
            part_info = self._client._upload_part(
                self._cloud_path, self._upload_id, self._part_number, chunk
            )
            del self._write_buffer[:target_block_size]
            self._parts.append(part_info)
            self._part_number += 1
            target_block_size = self._target_block_size()

    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]] = None) -> None:
        if self._write_buffer:
            if self._part_number > self._MAX_BLOCKS:
                raise OSError("Azure block upload exceeded the 50,000-block limit")
            if not self._upload_id:
                self._upload_id = self._client._initiate_multipart_upload(self._cloud_path)
            part_info = self._client._upload_part(
                self._cloud_path,
                self._upload_id,
                self._part_number,
                bytes(self._write_buffer),
            )
            self._parts.append(part_info)
            self._write_buffer.clear()

        if not self._parts:
            # No blocks staged — create an empty blob directly
            self._client._put_empty_object(self._cloud_path)
            return

        try:
            self._client._complete_multipart_upload(self._cloud_path, self._upload_id, self._parts)
        finally:
            self._upload_id = ""
            self._parts = []
            self._part_number = 1

    def _abort_upload(self) -> None:
        try:
            self._client._abort_multipart_upload(self._cloud_path, self._upload_id)
        finally:
            self._upload_id = ""
            self._parts = []
            self._part_number = 1
            self._write_buffer.clear()

    def close(self) -> None:
        super().close()
