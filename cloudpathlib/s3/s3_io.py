"""
S3-specific streaming I/O implementations.

Provides efficient streaming I/O for S3 using range requests and multipart uploads.
"""

from typing import Optional, Dict, Any

from ..cloud_io import _CloudStorageRaw
from ..cloudpath import register_raw_io_class


@register_raw_io_class("s3")
class _S3StorageRaw(_CloudStorageRaw):
    """
    S3-specific raw I/O adapter.

    Implements efficient range-based reads and multipart uploads for S3.
    S3 requires non-final parts to be at least 5 MiB; this class accumulates
    chunks in _write_buffer and only uploads a part once _MIN_PART_SIZE is reached.
    """

    # S3 minimum part size for non-final parts: 5 MiB
    _MIN_PART_SIZE = 5 * 1024 * 1024
    _MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
    _MAX_PARTS = 10_000
    _PARTS_PER_SIZE_TIER = 1_000

    def __init__(self, client, cloud_path, mode: str = "rb"):
        super().__init__(client, cloud_path, mode)

        # Multipart upload state
        self._upload_id: Optional[str] = None
        self._parts: list = []
        self._part_number: int = 1
        # Accumulation buffer — we only flush a part when >= _MIN_PART_SIZE bytes
        self._write_buffer: bytearray = bytearray()

    def _range_get(self, start: int, end: int) -> bytes:
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        return self._client._get_content_length(self._cloud_path)

    def _is_eof_error(self, error: Exception) -> bool:
        error_str = str(error)
        return (
            "InvalidRange" in error_str
            or hasattr(error, "__class__")
            and "InvalidRange" in error.__class__.__name__
        )

    def _target_part_size(self) -> int:
        tier = (self._part_number - 1) // self._PARTS_PER_SIZE_TIER
        return min(self._MIN_PART_SIZE * (2**tier), self._MAX_PART_SIZE)

    # ---- Write support (multipart upload with 5 MiB minimum part size) ----

    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]] = None) -> None:
        if not data:
            return

        self._write_buffer.extend(data)

        # Upload full-sized parts whenever we have enough buffered data
        target_part_size = self._target_part_size()
        while len(self._write_buffer) >= target_part_size:
            if self._part_number > self._MAX_PARTS:
                raise OSError("S3 multipart upload exceeded the 10,000-part limit")
            chunk = bytes(self._write_buffer[:target_part_size])
            if self._upload_id is None:
                self._upload_id = self._client._initiate_multipart_upload(self._cloud_path)
            part_info = self._client._upload_part(
                self._cloud_path, self._upload_id, self._part_number, chunk
            )
            del self._write_buffer[:target_part_size]
            self._parts.append(part_info)
            self._part_number += 1
            target_part_size = self._target_part_size()

    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]] = None) -> None:
        # Flush remaining buffer as the final part (exempt from 5 MiB floor)
        if self._write_buffer:
            if self._part_number > self._MAX_PARTS:
                raise OSError("S3 multipart upload exceeded the 10,000-part limit")
            if self._upload_id is None:
                self._upload_id = self._client._initiate_multipart_upload(self._cloud_path)
            part_info = self._client._upload_part(
                self._cloud_path,
                self._upload_id,
                self._part_number,
                bytes(self._write_buffer),
            )
            self._parts.append(part_info)
            self._part_number += 1
            self._write_buffer = bytearray()

        if self._upload_id is None:
            # No data written — create an empty object directly
            self._client._put_empty_object(self._cloud_path)
            return

        self._client._complete_multipart_upload(self._cloud_path, self._upload_id, self._parts)
        self._upload_id = None
        self._parts = []
        self._part_number = 1
        self._write_buffer.clear()

    def _abort_upload(self) -> None:
        try:
            if self._upload_id is not None:
                self._client._abort_multipart_upload(self._cloud_path, self._upload_id)
        finally:
            self._upload_id = None
            self._parts = []
            self._part_number = 1
            self._write_buffer.clear()

    def close(self) -> None:
        super().close()
