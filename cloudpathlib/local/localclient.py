import atexit
from hashlib import md5
import mimetypes
import os
from pathlib import Path, PurePosixPath
import shutil
from tempfile import TemporaryDirectory
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

from ..client import Client
from ..enums import FileCacheMode
from .localpath import LocalPath


class LocalClient(Client):
    """Abstract client for accessing objects the local filesystem. Subclasses are as a monkeypatch
    substitutes for normal Client subclasses when writing tests."""

    _default_storage_temp_dir = None

    def __init__(
        self,
        *args,
        local_storage_dir: Optional[Union[str, os.PathLike]] = None,
        file_cache_mode: Optional[Union[str, FileCacheMode]] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        content_type_method: Optional[Callable] = mimetypes.guess_type,
        **kwargs,
    ):
        # setup caching and local versions of file. use default temp dir if not provided
        if local_storage_dir is None:
            local_storage_dir = self.get_default_storage_dir()
        self._local_storage_dir = Path(local_storage_dir)

        super().__init__(
            local_cache_dir=local_cache_dir,
            content_type_method=content_type_method,
            file_cache_mode=file_cache_mode,
        )

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

    def _list_dir(
        self, cloud_path: "LocalPath", recursive=False
    ) -> Iterable[Tuple["LocalPath", bool]]:
        if recursive:
            return (
                (self._local_to_cloud_path(obj), obj.is_dir())
                for obj in self._cloud_path_to_local(cloud_path).glob("**/*")
            )
        return (
            (self._local_to_cloud_path(obj), obj.is_dir())
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

    def _remove(self, cloud_path: "LocalPath", missing_ok: bool = True) -> None:
        local_storage_path = self._cloud_path_to_local(cloud_path)
        if not missing_ok and not local_storage_path.exists():
            raise FileNotFoundError(f"File does not exist: {cloud_path}")

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

    def _touch(self, cloud_path: "LocalPath", exist_ok: bool = True) -> None:
        local_storage_path = self._cloud_path_to_local(cloud_path)
        if local_storage_path.exists() and not exist_ok:
            raise FileExistsError(f"File exists: {cloud_path}")
        local_storage_path.parent.mkdir(exist_ok=True, parents=True)
        local_storage_path.touch()

    def _upload_file(
        self, local_path: Union[str, os.PathLike], cloud_path: "LocalPath"
    ) -> "LocalPath":
        dst = self._cloud_path_to_local(cloud_path)
        dst.parent.mkdir(exist_ok=True, parents=True)
        shutil.copy(local_path, dst)
        return cloud_path

    def _get_metadata(self, cloud_path: "LocalPath") -> Dict:
        # content_type is the only metadata we test currently
        if self.content_type_method is None:
            content_type_method = lambda x: (None, None)
        else:
            content_type_method = self.content_type_method

        return {
            "content_type": content_type_method(str(self._cloud_path_to_local(cloud_path)))[0],
        }


_temp_dirs_to_clean: List[TemporaryDirectory] = []


@atexit.register
def clean_temp_dirs():
    for temp_dir in _temp_dirs_to_clean:
        temp_dir.cleanup()
