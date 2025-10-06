import abc
from functools import wraps
import mimetypes
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import Generic, Callable, Iterable, Optional, Tuple, TypeVar, Union

from .cloudpath import CloudImplementation, CloudPath, implementation_registry
from .enums import FileCacheMode
from .exceptions import InvalidConfigurationException


BoundedCloudPath = TypeVar("BoundedCloudPath", bound=CloudPath)


def register_client_class(key: str) -> Callable:
    def decorator(cls: type) -> type:
        if not issubclass(cls, Client):
            raise TypeError("Only subclasses of Client can be registered.")
        implementation_registry[key]._client_class = cls
        implementation_registry[key].name = key
        cls._cloud_meta = implementation_registry[key]
        return cls

    return decorator


def use_file_cache(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        cache_key = (method.__name__, args, tuple(sorted(kwargs.items())))
        result = self.file_cache.get(cache_key)
        if result is None:
            result = method(self, *args, **kwargs)
            self.file_cache.put(cache_key, result)
        return result
    return wrapper


def use_metadata_cache(method):
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        cache_key = (method.__name__, args, tuple(sorted(kwargs.items())))
        result = self.metadata_cache.get(cache_key)
        if result is None:
            result = method(self, *args, **kwargs)
            self.metadata_cache.put(cache_key, result)
        return result
    return wrapper


class Client(abc.ABC, Generic[BoundedCloudPath]):
    _cloud_meta: CloudImplementation
    _default_client = None

    def __init__(
        self,
        file_cache: Optional[FileCache] = FileSystemCache(),
        metadata_cache: Optional[MetadataCache] = FileSystemMetadataCache(),
        content_type_method: Optional[Callable] = mimetypes.guess_type,
    ):
        self._cloud_meta.validate_completeness()
        self.file_cache = file_cache
        self.metadata_cache = metadata_cache

    def __del__(self) -> None:
        # send signal that the client is being deleted so the caches can decide what to do
        self.file_cache.signal("client_del")
        self.metadata_cache.signal("client_del")

    @classmethod
    def get_default_client(cls) -> "Client":
        """Get the default client, which the one that is used when instantiating a cloud path
        instance for this cloud without a client specified.
        """
        if cls._default_client is None:
            cls._default_client = cls()
        return cls._default_client

    def set_as_default_client(self) -> None:
        """Set this client instance as the default one used when instantiating cloud path
        instances for this cloud without a client specified."""
        self.__class__._default_client = self

    def CloudPath(self, cloud_path: Union[str, BoundedCloudPath]) -> BoundedCloudPath:
        return self._cloud_meta.path_class(cloud_path=cloud_path, client=self)  # type: ignore

    def clear_cache(self):
        """Explicitly clears the contents of the file and metadata caches.
        """
        self.file_cache.clear()
        self.metadata_cache.clear()

    @abc.abstractmethod
    def _download_file(
        self, cloud_path: BoundedCloudPath, local_path: Union[str, os.PathLike]
    ) -> Path:
        pass

    @abc.abstractmethod
    def _exists(self, cloud_path: BoundedCloudPath) -> bool:
        pass

    @abc.abstractmethod
    def _list_dir(
        self, cloud_path: BoundedCloudPath, recursive: bool
    ) -> Iterable[Tuple[BoundedCloudPath, bool]]:
        """List all the files and folders in a directory.

        Parameters
        ----------
        cloud_path : CloudPath
            The folder to start from.
        recursive : bool
            Whether or not to list recursively.

        Returns
        -------
        contents : Iterable[Tuple]
            Of the form [(CloudPath, is_dir), ...] for every child of the dir.
        """
        pass

    @abc.abstractmethod
    def _move_file(
        self, src: BoundedCloudPath, dst: BoundedCloudPath, remove_src: bool = True
    ) -> BoundedCloudPath:
        pass

    @abc.abstractmethod
    def _remove(self, path: BoundedCloudPath, missing_ok: bool = True) -> None:
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

    @abc.abstractmethod
    def _get_public_url(self, cloud_path: BoundedCloudPath) -> str:
        pass

    @abc.abstractmethod
    def _generate_presigned_url(
        self, cloud_path: BoundedCloudPath, expire_seconds: int = 60 * 60
    ) -> str:
        pass
