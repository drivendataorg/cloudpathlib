"""Google Cloud Storage streaming I/O."""

from __future__ import annotations

from typing import Optional

from ..client import Client, _CloudWriteStream
from ..cloud_io import _CloudStorageRaw
from ..cloudpath import CloudPath, register_raw_io_class


@register_raw_io_class("gs")
class _GSStorageRaw(_CloudStorageRaw):
    """GCS range reads and resumable writes."""

    def __init__(self, client: Client, cloud_path: CloudPath, mode: str = "rb") -> None:
        super().__init__(client, cloud_path, mode)
        self._writer: Optional[_CloudWriteStream] = None

    def _upload_chunk(self, data: bytes) -> None:
        if not data:
            return
        if self._writer is None:
            self._writer = self._client._open_write_stream(self._cloud_path)
        self._client._write_stream(self._writer, data)

    def _finalize_upload(self) -> None:
        if self._writer is None:
            self._client._put_empty_object(self._cloud_path)
            return
        self._client._close_write_stream(self._writer)
        self._writer = None

    def _abort_upload(self) -> None:
        if self._writer is not None:
            try:
                self._client._abort_write_stream(self._writer)
            finally:
                self._writer = None
