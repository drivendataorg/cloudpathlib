import atexit
from hashlib import md5
import os
from pathlib import Path, PurePosixPath
import shutil
from tempfile import TemporaryDirectory
from typing import Iterable, List, Optional, Union

from ..client import Client
from .localpath import LocalPath


class LocalClient(Client):
    """Abstract client for accessing objects the local filesystem. Subclasses are as a monkeypatch
    substitutes for normal Client subclasses when writing tests."""

    _default_storage_temp_dir = None

    def __init__(
        self,
        *args,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        local_storage_dir: Optional[Union[str, os.PathLike]] = None,
        **kwargs,
    ):
        # setup caching and local versions of file. use default temp dir if not provided
        if local_storage_dir is None:
            local_storage_dir = self.get_default_storage_dir()
        self._local_storage_dir = Path(local_storage_dir)

        super().__init__(local_cache_dir=local_cache_dir)

    @classmethod
    def get_default_storage_dir(cls) -> Path:
        if cls._default_storage_temp_dir is None:
            cls._default_storage_temp_dir = TemporaryDirectory()
            _temp_dirs_to_clean.append(cls._default_storage_temp_dir)
        return Path(cls._default_storage_temp_dir.name)

    @classmethod
    def reset_default_storage_dir(cls) -> Path:
        cls._default_storage_temp_dir = None
        return cls.get_default_storage_dir()

    def _cloud_path_to_local(self, cloud_path: "LocalPath") -> Path:
        return self._local_storage_dir / cloud_path._no_prefix

    def _local_to_cloud_path(self, local_path: Union[str, os.PathLike]) -> "LocalPath":
        local_path = Path(local_path)
        cloud_prefix = self._cloud_meta.path_class.cloud_prefix
        return self.CloudPath(
            f"{cloud_prefix}{PurePosixPath(local_path.relative_to(self._local_storage_dir))}"
        )

    def _download_file(self, cloud_path: "LocalPath", local_path: Union[str, os.PathLike]) -> Path:
        local_path = Path(local_path)
        local_path.parent.mkdir(exist_ok=True, parents=True)
        shutil.copyfile(self._cloud_path_to_local(cloud_path), local_path)
        return local_path

    def _exists(self, cloud_path: "LocalPath") -> bool:
        return self._cloud_path_to_local(cloud_path).exists()

    def _is_dir(self, cloud_path: "LocalPath") -> bool:
        return self._cloud_path_to_local(cloud_path).is_dir()

    def _is_file(self, cloud_path: "LocalPath") -> bool:
        return self._cloud_path_to_local(cloud_path).is_file()

    def _list_dir(self, cloud_path: "LocalPath", recursive=False) -> Iterable["LocalPath"]:
        if recursive:
            return (
                self._local_to_cloud_path(obj)
                for obj in self._cloud_path_to_local(cloud_path).glob("**/*")
            )
        return (
            self._local_to_cloud_path(obj)
            for obj in self._cloud_path_to_local(cloud_path).iterdir()
        )

    def _md5(self, cloud_path: "LocalPath") -> str:
        return md5(self._cloud_path_to_local(cloud_path).read_bytes()).hexdigest()

    def _move_file(
        self, src: "LocalPath", dst: "LocalPath", remove_src: bool = True
    ) -> "LocalPath":
        self._cloud_path_to_local(dst).parent.mkdir(exist_ok=True, parents=True)

        if remove_src:
            self._cloud_path_to_local(src).replace(self._cloud_path_to_local(dst))
        else:
            shutil.copy(self._cloud_path_to_local(src), self._cloud_path_to_local(dst))
        return dst

    def _remove(self, cloud_path: "LocalPath") -> None:
        local_storage_path = self._cloud_path_to_local(cloud_path)
        if local_storage_path.is_file():
            local_storage_path.unlink()
        elif local_storage_path.is_dir():
            shutil.rmtree(local_storage_path)

    def _stat(self, cloud_path: "LocalPath") -> os.stat_result:
        stat_result = self._cloud_path_to_local(cloud_path).stat()

        return os.stat_result(
            (  # type: ignore
                None,  # type: ignore # mode
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
        local_storage_path = self._cloud_path_to_local(cloud_path)
        local_storage_path.parent.mkdir(exist_ok=True, parents=True)
        local_storage_path.touch()

    def _upload_file(
        self, local_path: Union[str, os.PathLike], cloud_path: "LocalPath"
    ) -> "LocalPath":
        dst = self._cloud_path_to_local(cloud_path)
        dst.parent.mkdir(exist_ok=True, parents=True)
        shutil.copy(local_path, dst)
        return cloud_path


_temp_dirs_to_clean: List[TemporaryDirectory] = []


@atexit.register
def clean_temp_dirs():
    for temp_dir in _temp_dirs_to_clean:
        temp_dir.cleanup()
