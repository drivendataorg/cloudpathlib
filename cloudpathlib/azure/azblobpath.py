import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

try:
    from azure.core.exceptions import ResourceNotFoundError
except ImportError:
    pass

from ..cloudpath import CloudPath, NoStatError, register_path_class


if TYPE_CHECKING:
    from .azblobclient import AzureBlobClient


@register_path_class("azure")
class AzureBlobPath(CloudPath):
    cloud_prefix: str = "az://"
    client: "AzureBlobClient"

    @property
    def drive(self) -> str:
        return self.container

    def is_dir(self) -> bool:
        return self.client._is_file_or_dir(self) == "dir"

    def is_file(self) -> bool:
        return self.client._is_file_or_dir(self) == "file"

    def mkdir(self, parents=False, exist_ok=False):
        # not possible to make empty directory on blob storage
        pass

    def touch(self):
        if self.exists():
            self.client._move_file(self, self)
        else:
            tf = TemporaryDirectory()
            p = Path(tf.name) / "empty"
            p.touch()

            self.client._upload_file(p, self)

            tf.cleanup()

    def stat(self):
        try:
            meta = self.client._get_metadata(self)
        except ResourceNotFoundError:
            raise NoStatError(
                f"No stats available for {self}; it may be a directory or not exist."
            )

        return os.stat_result(
            (
                None,  # mode
                None,  # ino
                self.cloud_prefix,  # dev,
                None,  # nlink,
                None,  # uid,
                None,  # gid,
                meta.get("size", 0),  # size,
                None,  # atime,
                meta.get("last_modified", 0).timestamp(),  # mtime,
                None,  # ctime,
            )
        )

    @property
    def container(self) -> str:
        return self._no_prefix.split("/", 1)[0]

    @property
    def blob(self) -> str:
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.client._get_metadata(self).get("etag", None)

    @property
    def md5(self) -> str:
        return self.client._get_metadata(self).get("content_settings", {}).get("content_md5", None)
