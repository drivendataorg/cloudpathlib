from pathlib import PurePosixPath
from typing import Tuple, Union, Optional

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

from ..cloudpath import CloudPath, NoStatError, register_path_class


if TYPE_CHECKING:
    from .httpclient import HttpClient


@register_path_class("http")
class HttpPath(CloudPath):
    cloud_prefix = "http://"
    client: "HttpClient"

    def __init__(
        self,
        cloud_path: Union[str, "HttpPath"],
        client: Optional["HttpClient"] = None,
    ) -> None:
        super().__init__(cloud_path, client)

        self._path = (
            PurePosixPath(self._url.path)
            if self._url.path.startswith("/")
            else PurePosixPath(f"/{self._url.path}")
        )

    @property
    def drive(self) -> str:
        # For HTTP paths, no drive; use .anchor for scheme + netloc
        return self._url.netloc

    @property
    def anchor(self) -> str:
        return f"{self._url.scheme}://{self._url.netloc}/"

    @property
    def _no_prefix_no_drive(self) -> str:
        # netloc appears in anchor and drive for httppath; so don't double count
        return self._str[len(self.anchor) - 1 :]

    def is_dir(self) -> bool:
        if not self.exists():
            return False

        # HTTP doesn't really have directories, but some servers might list files if treated as such
        # Here we'll assume paths without are dirs
        return self._path.suffix == ""

    def is_file(self) -> bool:
        if not self.exists():
            return False

        # HTTP doesn't have a direct file check, but we assume if it has a suffix, it's a file
        return self._path.suffix != ""

    def mkdir(self, parents: bool = False, exist_ok: bool = False) -> None:
        pass  # no-op for HTTP Paths

    def touch(self, exist_ok: bool = True) -> None:
        if self.exists():
            if not exist_ok:
                raise FileExistsError(f"File already exists: {self}")

            raise NotImplementedError(
                "Touch not implemented for existing HTTP files since we can't update the modified time."
            )
        else:
            empty_file = Path(TemporaryDirectory().name) / "empty_file.txt"
            empty_file.parent.mkdir(parents=True, exist_ok=True)
            empty_file.write_text("")
            self.client._upload_file(empty_file, self)

    def stat(self, follow_symlinks: bool = True) -> os.stat_result:
        try:
            meta = self.client._get_metadata(self)
        except:  # noqa E722
            raise NoStatError(f"Could not get metadata for {self}")

        return os.stat_result(
            (  # type: ignore
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

    def as_url(self, presign: bool = False, expire_seconds: int = 60 * 60) -> str:
        if presign:
            raise NotImplementedError("Presigning not supported for HTTP paths")

        return (
            self._url.geturl()
        )  # recreate from what was initialized so we have the same query params, etc.

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def parents(self) -> Tuple["HttpPath", ...]:
        return super().parents + (self._new_cloudpath(""),)

    def get(self, **kwargs):
        return self.client.request(self, "GET", **kwargs)

    def put(self, **kwargs):
        return self.client.request(self, "PUT", **kwargs)

    def post(self, **kwargs):
        return self.client.request(self, "POST", **kwargs)

    def delete(self, **kwargs):
        return self.client.request(self, "DELETE", **kwargs)

    def head(self, **kwargs):
        return self.client.request(self, "HEAD", **kwargs)
