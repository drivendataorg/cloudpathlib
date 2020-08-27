import abc
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generic, Callable, Iterable, Optional, TypeVar, Union

from ..cloudpath import CloudImplementation, CloudPath, implementation_registry

BoundedCloudPath = TypeVar("BoundedCloudPath", bound=CloudPath)


def register_backend_class(key: str) -> Callable:
    def decorator(cls: type) -> type:
        if not issubclass(cls, Backend):
            raise TypeError("Only subclasses of Backend can be registered.")
        implementation_registry[key]._backend_class = cls
        cls._cloud_meta = implementation_registry[key]
        return cls

    return decorator


class Backend(abc.ABC, Generic[BoundedCloudPath]):
    _cloud_meta: CloudImplementation
    default_backend = None

    def __init__(self, local_cache_dir: Optional[Union[str, os.PathLike]] = None):
        # setup caching and local versions of file and track if it is a tmp dir
        self._cache_tmp_dir = None
        if local_cache_dir is None:
            self._cache_tmp_dir = TemporaryDirectory()
            local_cache_dir = self._cache_tmp_dir.name

        self._local_cache_dir = Path(local_cache_dir)

    def __del__(self) -> None:
        # make sure temp is cleaned up if we created it
        if self._cache_tmp_dir is not None:
            self._cache_tmp_dir.cleanup()

    @classmethod
    def get_default_backend(cls) -> "Backend":
        if cls.default_backend is None:
            cls.default_backend = cls()
        return cls.default_backend

    def CloudPath(self, cloud_path: Union[str, BoundedCloudPath]) -> BoundedCloudPath:
        return self._cloud_meta.path_class(cloud_path=cloud_path, backend=self)

    @abc.abstractmethod
    def _download_file(
        self, cloud_path: BoundedCloudPath, local_path: Union[str, os.PathLike]
    ) -> Union[str, os.PathLike]:
        pass

    @abc.abstractmethod
    def _exists(self, cloud_path: BoundedCloudPath) -> bool:
        pass

    @abc.abstractmethod
    def _list_dir(
        self, cloud_path: BoundedCloudPath, recursive: bool
    ) -> Iterable[BoundedCloudPath]:
        """List all the files and folders in a directory.

        Parameters
        ----------
        cloud_path : CloudPath
            The folder to start from.
        recursive : bool
            Whether or not to list recursively.
        """
        pass

    @abc.abstractmethod
    def _move_file(self, src: BoundedCloudPath, dst: BoundedCloudPath) -> BoundedCloudPath:
        pass

    @abc.abstractmethod
    def _remove(self, path: BoundedCloudPath) -> None:
        """Remove a file or folder from the server.

        Parameters
        ----------
        path : CloudPath
            The file or folder to remove.
        """
        pass

    @abc.abstractmethod
    def _upload_file(
        self, local_path: Union[str, os.PathLike], cloud_path: BoundedCloudPath
    ) -> BoundedCloudPath:
        pass
