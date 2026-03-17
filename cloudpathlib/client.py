import abc
import mimetypes
import os
from pathlib import Path
from queue import Queue
import shutil
from tempfile import TemporaryDirectory
from threading import Thread
from typing import Generic, Callable, Iterable, Iterator, Optional, Tuple, TypeVar, Union

from .cloudpath import CloudImplementation, CloudPath, implementation_registry
from .enums import FileCacheMode
from .exceptions import InvalidConfigurationException

BoundedCloudPath = TypeVar("BoundedCloudPath", bound=CloudPath)

_SENTINEL = object()


def _prefetch_iterator(iterable: Iterable, buffer_size: int = 1) -> Iterator:
    """Wrap an iterable so the next item is fetched in a background thread.

    This overlaps the producer (e.g. network page fetch + XML parsing) with
    the consumer (e.g. yielding items from the current page).  A *buffer_size*
    of 1 means one item is fetched ahead; increase for deeper pipelines.
    """
    q: Queue = Queue(maxsize=buffer_size + 1)

    def _producer():
        try:
            for item in iterable:
                q.put(item)
        except BaseException as exc:
            q.put(exc)
        finally:
            q.put(_SENTINEL)

    t = Thread(target=_producer, daemon=True)
    t.start()

    while True:
        item = q.get()
        if item is _SENTINEL:
            break
        if isinstance(item, BaseException):
            raise item
        yield item


def register_client_class(key: str) -> Callable:
    def decorator(cls: type) -> type:
        if not issubclass(cls, Client):
            raise TypeError("Only subclasses of Client can be registered.")
        implementation_registry[key]._client_class = cls
        implementation_registry[key].name = key
        cls._cloud_meta = implementation_registry[key]
        return cls

    return decorator


class Client(abc.ABC, Generic[BoundedCloudPath]):
    _cloud_meta: CloudImplementation
    _default_client = None

    def __init__(
        self,
        file_cache_mode: Optional[Union[str, FileCacheMode]] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        content_type_method: Optional[Callable] = mimetypes.guess_type,
    ):
        self.file_cache_mode = None
        self._cache_tmp_dir = None
        self._cloud_meta.validate_completeness()

        # convert strings passed to enum
        if isinstance(file_cache_mode, str):
            file_cache_mode = FileCacheMode(file_cache_mode)

        # if not explicitly passed to client, get from env var
        if file_cache_mode is None:
            file_cache_mode = FileCacheMode.from_environment()

        if local_cache_dir is None:
            local_cache_dir = os.environ.get("CLOUDPATHLIB_LOCAL_CACHE_DIR", None)

            # treat empty string as None to avoid writing cache in cwd; set to "." for cwd
            if local_cache_dir == "":
                local_cache_dir = None

        # explicitly passing a cache dir, so we set to persistent
        # unless user explicitly passes a different file cache mode
        if local_cache_dir and file_cache_mode is None:
            file_cache_mode = FileCacheMode.persistent

        if file_cache_mode == FileCacheMode.persistent and local_cache_dir is None:
            raise InvalidConfigurationException(
                f"If you use the '{FileCacheMode.persistent}' cache mode, you must pass a `local_cache_dir` when you instantiate the client."
            )

        # if no explicit local dir, setup caching in temporary dir
        if local_cache_dir is None:
            self._cache_tmp_dir = TemporaryDirectory()
            local_cache_dir = self._cache_tmp_dir.name

            if file_cache_mode is None:
                file_cache_mode = FileCacheMode.tmp_dir

        self._local_cache_dir = Path(local_cache_dir)
        self.content_type_method = content_type_method

        # Fallback: if not set anywhere, default to tmp_dir (for backwards compatibility)
        if file_cache_mode is None:
            file_cache_mode = FileCacheMode.tmp_dir

        self.file_cache_mode = file_cache_mode

    def __del__(self) -> None:
        # remove containing dir, even if a more aggressive strategy
        # removed the actual files
        if getattr(self, "file_cache_mode", None) in [
            FileCacheMode.tmp_dir,
            FileCacheMode.close_file,
            FileCacheMode.cloudpath_object,
        ]:
            self.clear_cache()

            if self._local_cache_dir.exists():
                self._local_cache_dir.rmdir()

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

    def CloudPath(self, cloud_path: Union[str, BoundedCloudPath], *parts: str) -> BoundedCloudPath:
        return self._cloud_meta.path_class(cloud_path, *parts, client=self)  # type: ignore

    def _make_cloudpath(self, uri_str: str) -> BoundedCloudPath:
        """Fast internal CloudPath constructor for trusted URI strings.

        Bypasses the metaclass dispatcher, is_valid_cloudpath,
        validate_completeness, and isinstance checks that are redundant
        when the URI originates from a backend listing operation.
        """
        obj = object.__new__(self._cloud_meta._path_class)
        obj._handle = None
        obj._client = self
        obj._str = uri_str
        obj._dirty = False
        return obj  # type: ignore

    def clear_cache(self):
        """Clears the contents of the cache folder.
        Does not remove folder so it can keep being written to.
        """
        if self._local_cache_dir.exists():
            for p in self._local_cache_dir.iterdir():
                if p.is_file():
                    p.unlink()
                else:
                    shutil.rmtree(p)

    @abc.abstractmethod
    def _download_file(
        self, cloud_path: BoundedCloudPath, local_path: Union[str, os.PathLike]
    ) -> Path:
        pass

    @abc.abstractmethod
    def _exists(self, cloud_path: BoundedCloudPath) -> bool:
        pass

    @abc.abstractmethod
    def _list_dir_raw(
        self,
        cloud_path: BoundedCloudPath,
        recursive: bool,
        include_dirs: bool = True,
        prefilter_pattern: Optional[str] = None,
    ) -> Iterable[Tuple[str, bool]]:
        """List files and folders, yielding raw URI strings.

        This is the low-level listing method that backends must implement.
        It yields ``(uri_string, is_dir)`` tuples where *uri_string* is
        the full cloud URI (e.g. ``"s3://bucket/key"``).

        Parameters
        ----------
        cloud_path : CloudPath
            The folder to start from.
        recursive : bool
            Whether or not to list recursively.
        include_dirs : bool
            When True (default), intermediate directories are inferred and
            yielded for recursive listings.  When False, only actual objects
            (files / blobs) are yielded, skipping the expensive parent-
            directory reconstruction.  Ignored for non-recursive listings.
        prefilter_pattern : str, optional
            A glob pattern (relative to *cloud_path*) that the backend may
            use for server-side filtering to reduce the number of results
            returned.  Backends that do not support server-side pattern
            matching should silently ignore this parameter.  Client-side
            regex matching in ``_glob`` still validates every result, so this
            is purely an optimisation hint.
        """
        pass

    def _list_dir(
        self,
        cloud_path: BoundedCloudPath,
        recursive: bool,
        include_dirs: bool = True,
        prefilter_pattern: Optional[str] = None,
    ) -> Iterable[Tuple[BoundedCloudPath, bool]]:
        """List files and folders, yielding ``(CloudPath, is_dir)`` tuples.

        Thin wrapper around :meth:`_list_dir_raw` that constructs CloudPath
        objects from the raw URI strings.
        """
        for raw_uri, is_dir in self._list_dir_raw(
            cloud_path, recursive, include_dirs, prefilter_pattern=prefilter_pattern
        ):
            yield self._make_cloudpath(raw_uri), is_dir

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
