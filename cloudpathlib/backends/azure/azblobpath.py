import os
from pathlib import Path
from tempfile import TemporaryDirectory

from ...cloudpath import CloudPath
import cloudpathlib


class AzureBlobPath(CloudPath):
    cloud_prefix = "az://"

    def __init__(self, *args, **kwargs):
        self.backend_class = cloudpathlib.AzureBlobBackend
        super().__init__(*args, **kwargs)

    @property
    def drive(self):
        return self.container

    def exists(self):
        return self.backend.exists(self)

    def is_dir(self):
        return self.backend.is_file_or_dir(self) == "dir"

    def is_file(self):
        return self.backend.is_file_or_dir(self) == "file"

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

            self.backend.upload_file(p, self)

            tf.cleanup()

    def stat(self):
        meta = self.backend.get_metadata(self)

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
    def container(self):
        return self._no_prefix.split("/", 1)[0]

    @property
    def blob(self):
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.backend.get_metadata(self).get("etag", None)

    @property
    def md5(self):
        return self.backend.get_metadata(self).get("content_settings", {}).get("content_md5", None)
