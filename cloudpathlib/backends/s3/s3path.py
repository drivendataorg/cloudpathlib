import os
from pathlib import Path
from tempfile import TemporaryDirectory

from ...cloudpath import CloudPath
from .s3backend import S3Backend


class S3Path(CloudPath):
    cloud_prefix = "s3://"
    backend_class = S3Backend

    @property
    def drive(self):
        return self.bucket

    def is_dir(self):
        return self.backend.is_file_or_dir(self) == "dir"

    def is_file(self):
        return self.backend.is_file_or_dir(self) == "file"

    def mkdir(self, parents=False, exist_ok=False):
        # not possible to make empty directory on s3
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
    def bucket(self):
        return self._no_prefix.split("/", 1)[0]

    @property
    def key(self):
        key = self._no_prefix_no_drive

        # key should never have starting slash for
        # use with boto, etc.
        if key.startswith("/"):
            key = key[1:]

        return key

    @property
    def etag(self):
        return self.backend.get_metadata(self).get("etag")
