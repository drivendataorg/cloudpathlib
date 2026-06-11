"""
Google Cloud Storage-specific streaming I/O implementations.

Provides efficient streaming I/O for GCS using range requests and resumable uploads.
Upload state is held on the raw adapter instance (not the shared client) so concurrent
writers to the same client cannot collide.
"""

from typing import Any, Dict, Optional

from ..cloud_io import _CloudStorageRaw
from ..cloudpath import register_raw_io_class


@register_raw_io_class("gs")
class _GSStorageRaw(_CloudStorageRaw):
    """
    GCS-specific raw I/O adapter.

    Implements efficient range-based reads and resumable uploads for GCS.
    Write state (_writer) lives on this adapter instance, not on the client,
    so concurrent writes to different paths on the same client are safe.
    """

    def __init__(self, client, cloud_path, mode: str = "rb"):
        super().__init__(client, cloud_path, mode)
        # Open write stream (returned by client._open_write_stream); None until first write
        self._writer: Optional[Any] = None

    def _range_get(self, start: int, end: int) -> bytes:
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        return self._client._get_content_length(self._cloud_path)

    def _is_eof_error(self, error: Exception) -> bool:
        error_str = str(error)
        if "416" in error_str or "Requested Range Not Satisfiable" in error_str:
            return True
        if hasattr(error, "code") and error.code == 416:
            return True
        return False

    # ---- Write support (resumable upload via client._open_write_stream) ----

    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]] = None) -> None:
        if not data:
            return
        if self._writer is None:
            self._writer = self._client._open_write_stream(self._cloud_path)
        self._writer.write(data)

    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]] = None) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        else:
            # No data was written — create an empty object
            self._client._put_empty_object(self._cloud_path)

    def close(self) -> None:
        super().close()
