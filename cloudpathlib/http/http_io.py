"""HTTP streaming I/O."""

from __future__ import annotations

import tempfile
from typing import Protocol, cast

from ..client import Client
from ..cloud_io import _CloudStorageRaw
from ..cloudpath import CloudPath, register_raw_io_class


class _HttpStreamingClient(Protocol):
    def _put_data(
        self,
        cloud_path: CloudPath,
        data: tempfile.SpooledTemporaryFile[bytes],
        content_length: int,
    ) -> None: ...


@register_raw_io_class("http")
@register_raw_io_class("https")
class _HttpStorageRaw(_CloudStorageRaw):
    """HTTP range reads and single-request writes."""

    def __init__(self, client: Client, cloud_path: CloudPath, mode: str = "rb") -> None:
        super().__init__(client, cloud_path, mode)
        self._upload_buffer = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024)

    def _upload_chunk(self, data: bytes) -> None:
        if data:
            self._upload_buffer.write(data)

    def _finalize_upload(self) -> None:
        self._upload_buffer.seek(0, 2)
        content_length = self._upload_buffer.tell()
        self._upload_buffer.seek(0)
        try:
            client = cast(_HttpStreamingClient, self._client)
            client._put_data(self._cloud_path, self._upload_buffer, content_length)
        finally:
            self._upload_buffer.close()

    def _abort_upload(self) -> None:
        self._upload_buffer.close()
