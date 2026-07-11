"""
HTTP-specific streaming I/O implementations.

Provides streaming I/O for HTTP/HTTPS using range requests and single-PUT uploads.
"""

import tempfile
from typing import Optional, Dict, Any

from ..cloud_io import _CloudStorageRaw
from ..cloudpath import register_raw_io_class


@register_raw_io_class("http")
@register_raw_io_class("https")
class _HttpStorageRaw(_CloudStorageRaw):
    """
    HTTP-specific raw I/O adapter.

    Implements efficient range-based reads and single-PUT uploads for HTTP/HTTPS.
    Write operations require the server to support PUT requests.
    Data is buffered in _upload_buffer on the adapter instance (not on the client)
    and flushed as a single PUT on close.
    """

    def __init__(self, client, cloud_path, mode: str = "rb"):
        super().__init__(client, cloud_path, mode)
        self._upload_buffer: Any = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024)

    def _range_get(self, start: int, end: int) -> bytes:
        return self._client._range_download(self._cloud_path, start, end)

    def _get_size(self) -> int:
        return self._client._get_content_length(self._cloud_path)

    def _is_eof_error(self, error: Exception) -> bool:
        error_str = str(error).lower()
        return (
            "416" in error_str
            or "requested range not satisfiable" in error_str
            or "invalid range" in error_str
        )

    # ---- Write support (single PUT on finalize) ----

    def _upload_chunk(self, data: bytes, upload_state: Optional[Dict[str, Any]] = None) -> None:
        if not data:
            return
        self._upload_buffer.write(data)

    def _finalize_upload(self, upload_state: Optional[Dict[str, Any]] = None) -> None:
        self._upload_buffer.seek(0, 2)
        content_length = self._upload_buffer.tell()
        self._upload_buffer.seek(0)
        try:
            self._client._put_data(self._cloud_path, self._upload_buffer, content_length)
        finally:
            self._upload_buffer.close()

    def _abort_upload(self) -> None:
        self._upload_buffer.close()

    def close(self) -> None:
        super().close()
