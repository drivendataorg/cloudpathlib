import os
from typing import Iterable, Optional, Union

from ..client import Client
from .fsspecpath import fsspec_implementation, FsspecPath

from fsspec.spec import AbstractFileSystem
from s3fs import S3FileSystem


class FsspecClient(Client):
    _cloud_meta = fsspec_implementation

    def __init__(
        self,
        filesystem: Optional[AbstractFileSystem] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
    ):
        self.filesystem = S3FileSystem()
        super().__init__(local_cache_dir=local_cache_dir)

    def _download_file(
        self, cloud_path: FsspecPath, local_path: Union[str, os.PathLike]
    ) -> Union[str, os.PathLike]:
        self.filesystem.get_file(rpath=cloud_path._no_prefix, lpath=local_path)

    def _exists(self, cloud_path: FsspecPath) -> bool:
        return self.filesystem.exists(cloud_path._no_prefix)

    def _is_dir(self, cloud_path: FsspecPath) -> bool:
        return self.filesystem.isdir(cloud_path._no_prefix)

    def _is_file(self, cloud_path: FsspecPath) -> bool:
        return self.filesystem.isfile(cloud_path._no_prefix)

    def _list_dir(self, cloud_path: FsspecPath, recursive=False) -> Iterable[FsspecPath]:
        if recursive:
            return (
                self.CloudPath(cloud_path.cloud_prefix + obj)
                for obj in self.filesystem.glob(f"{cloud_path._no_prefix}/**/*", detail=False)
            )
        return (
            self.CloudPath(cloud_path.cloud_prefix + obj)
            for obj in self.filesystem.ls(cloud_path._no_prefix, detail=False)
        )

    def _move_file(self, src: FsspecPath, dst: FsspecPath) -> FsspecPath:
        self.filesystem.mv(src, dst, recursive=False)
        return dst

    def _remove(self, cloud_path: FsspecPath) -> None:
        self.filesystem.rm(cloud_path._no_prefix)

    def _stat(self, cloud_path: FsspecPath) -> os.stat_result:
        info = self.filesystem.info(cloud_path._no_prefix)

        return os.stat_result(
            (
                info.get("mode"),  # mode
                None,  # ino
                cloud_path.cloud_prefix,  # dev,
                None,  # nlink,
                info.get("uid"),  # uid,
                info.get("gid"),  # gid,
                info.get("size"),  # size,
                None,  # atime,
                info.get("mtime"),  # mtime,
                None,  # ctime,
            )
        )

    def _touch(self, cloud_path: FsspecPath) -> None:
        self.filesystem.touch(cloud_path._no_prefix)

    def _upload_file(
        self, local_path: Union[str, os.PathLike], cloud_path: FsspecPath
    ) -> FsspecPath:
        self.filesystem.put_file(lpath=local_path, rpath=cloud_path._no_prefix)


fsspec_implementation._client_class = FsspecClient
