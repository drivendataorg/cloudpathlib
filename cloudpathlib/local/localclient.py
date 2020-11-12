import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import Iterable, Optional, Union

from ..client import Client
from .localpath import LocalPath


class LocalClient(Client):
    """Abstract client for accessing objects the local filesystem. Subclasses are as a monkeypatch
    substitutes for normal Client subclasses when writing tests."""

    def __init__(
        self,
        *args,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        local_storage_dir: Optional[Union[str, os.PathLike]] = None,
        **kwargs,
    ):
        # setup caching and local versions of file and track if it is a tmp dir
        self._storage_tmp_dir = None
        if local_storage_dir is None:
            self._storage_tmp_dir = TemporaryDirectory()
            local_storage_dir = self._storage_tmp_dir.name

        self._local_storage_dir = Path(local_storage_dir)

        super().__init__(local_cache_dir=local_cache_dir)

    def __del__(self) -> None:
        # make sure temporary local_storage_dir is cleaned up if we created it
        if self._storage_tmp_dir is not None:
            self._storage_tmp_dir.cleanup()

    def _get_local_storage_path(self, cloud_path: "LocalPath") -> Path:
        return self._local_cache_dir / cloud_path._no_prefix

    def _download_file(
        self, cloud_path: "LocalPath", local_path: Union[str, os.PathLike]
    ) -> Union[str, os.PathLike]:
        Path(local_path).parent.mkdir(exist_ok=True, parents=True)
        shutil.copy2(self._get_local_storage_path(cloud_path), local_path)
        return local_path

    def _exists(self, cloud_path: "LocalPath") -> bool:
        return self._get_local_storage_path(cloud_path).exists()

    def _is_dir(self, cloud_path: "LocalPath") -> bool:
        return self._get_local_storage_path(cloud_path).is_dir()

    def _is_file(self, cloud_path: "LocalPath") -> bool:
        return self._get_local_storage_path(cloud_path).is_file()

    def _list_dir(self, cloud_path: "LocalPath", recursive=False) -> Iterable["LocalPath"]:
        if recursive:
            return (
                self.CloudPath(obj)
                for obj in self._get_local_storage_path(cloud_path).glob("**/*")
            )
        return (self.CloudPath(obj) for obj in self._get_local_storage_path(cloud_path).iterdir())

    def _move_file(self, src: "LocalPath", dst: "LocalPath") -> "LocalPath":
        self._get_local_storage_path(src).replace(self._get_local_storage_path(dst))
        return dst

    def _remove(self, cloud_path: "LocalPath") -> None:
        local_storage_path = self._get_local_storage_path(cloud_path)
        if local_storage_path.is_file():
            local_storage_path.unlink()
        elif local_storage_path.is_dir():
            shutil.rmtree(local_storage_path)

    def _stat(self, cloud_path: "LocalPath") -> os.stat_result:
        stat_result = self._get_local_storage_path(cloud_path).stat()

        return os.stat_result(
            (
                None,  # mode
                None,  # ino
                cloud_path.cloud_prefix,  # dev,
                None,  # nlink,
                None,  # uid,
                None,  # gid,
                stat_result.st_size,  # size,
                None,  # atime,
                stat_result.st_mtime,  # mtime,
                None,  # ctime,
            )
        )

    def _touch(self, cloud_path: "LocalPath") -> None:
        local_storage_path = self._get_local_storage_path(cloud_path)
        local_storage_path.parent.mkdir(exist_ok=True, parents=True)
        local_storage_path.touch()

    def _upload_file(
        self, local_path: Union[str, os.PathLike], cloud_path: "LocalPath"
    ) -> "LocalPath":
        shutil.copy(local_path, self._get_local_storage_path(cloud_path))
        return cloud_path
