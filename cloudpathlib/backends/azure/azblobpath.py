import os
from pathlib import Path
from tempfile import TemporaryDirectory

from ...cloudpath import CloudPath, register_path_class


@register_path_class("azure")
class AzureBlobPath(CloudPath):
    cloud_prefix: str = "az://"

    @property
    def drive(self) -> str:
        return self.container

    def is_dir(self) -> bool:
        return self.backend._is_file_or_dir(self) == "dir"

    def is_file(self) -> bool:
        return self.backend._is_file_or_dir(self) == "file"

    def mkdir(self, parents=False, exist_ok=False):
        # not possible to make empty directory on blob storage
        pass

    def touch(self):
        if self.exists():
            self.backend.move_file(self, self)
        else:
            tf = TemporaryDirectory()
            p = Path(tf.name) / "empty"
            p.touch()

            self.backend._upload_file(p, self)

            tf.cleanup()

    def stat(self):
        meta = self.backend._get_metadata(self)

        print(meta)

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
        return self.backend._get_metadata(self).get("etag", None)

    @property
    def md5(self) -> str:
        return (
            self.backend._get_metadata(self).get("content_settings", {}).get("content_md5", None)
        )
