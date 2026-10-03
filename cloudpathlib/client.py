import abc
import mimetypes
import os
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import (
    Any,
    BinaryIO,
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from . import env
from .cloud_io import _CloudStorageRaw
from .cloudpath import CloudImplementation, CloudPath, implementation_registry
from .enums import FileCacheMode
from .exceptions import (
    CloudPathFileNotFoundError,
    InvalidConfigurationException,
    NoStatError,
)

BoundedCloudPath = TypeVar("BoundedCloudPath", bound=CloudPath)
_UploadPart = Dict[str, Any]


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
    _default_client: ClassVar[Optional["Client[BoundedCloudPath]"]] = None

    # Streaming I/O (`FileCacheMode.streaming`): the raw stream class `CloudPath.open` wraps,
    # or None when the provider has no streaming support. Multipart providers use
    # `_CloudMultipartStorageRaw` and the provider's hard limits below (S3's by default).
    # The part size actually used starts at the minimum (overridable per client with
    # `CLOUDPATHLIB_<PROVIDER>_STREAMING_PART_SIZE`, see `env.py`) and grows for very large
    # streams so the part-count limit is never reached.
    _streaming_raw_class: ClassVar[Optional[Type[_CloudStorageRaw]]] = None
    _multipart_min_part_size: ClassVar[int] = 5 * env.MiB  # smallest non-final part (5 MiB)
    _multipart_max_part_size: ClassVar[int] = 5 * 1024 * env.MiB  # largest part (5 GiB)
    _multipart_max_parts: ClassVar[int] = 10_000

    def __init__(
        self,
        file_cache_mode: Optional[Union[str, FileCacheMode]] = None,
        local_cache_dir: Optional[Union[str, os.PathLike]] = None,
        content_type_method: Optional[Callable] = mimetypes.guess_type,
        *,
        streaming_max_concurrency: Optional[int] = None,
    ) -> None:
        self.file_cache_mode = None
        self._cache_tmp_dir = None
        self._cloud_meta.validate_completeness()

        # concurrent requests per open streaming file (part uploads / read-ahead);
        # 1 means fully sequential I/O
        if streaming_max_concurrency is None:
            streaming_max_concurrency = env.streaming_max_concurrency()
        if streaming_max_concurrency < 1:
            raise ValueError("streaming_max_concurrency must be at least 1")
        self.streaming_max_concurrency = streaming_max_concurrency
        # multipart part size for streaming writes: the provider minimum unless overridden
        self._multipart_part_size = env.streaming_part_size(
            getattr(self._cloud_meta, "name", None), self._multipart_min_part_size
        )

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
            # streaming avoids the cache except for append/update fallbacks, which
            # should not outlive the client
            FileCacheMode.streaming,
        ]:
            self.clear_cache()

            if self._local_cache_dir.exists():
                self._local_cache_dir.rmdir()

    @classmethod
    def get_default_client(cls) -> "Client[BoundedCloudPath]":
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

    def _guess_content_type(
        self, cloud_path: BoundedCloudPath
    ) -> Tuple[Optional[str], Optional[str]]:
        """`(content_type, content_encoding)` for an upload, from `content_type_method` applied
        to the object name; `(None, None)` when guessing is disabled."""
        if self.content_type_method is None:
            return None, None
        return self.content_type_method(str(cloud_path))

    @abc.abstractmethod
    def _get_public_url(self, cloud_path: BoundedCloudPath) -> str:
        pass

    @abc.abstractmethod
    def _generate_presigned_url(
        self, cloud_path: BoundedCloudPath, expire_seconds: int = 60 * 60
    ) -> str:
        pass

    def _range_download(
        self, cloud_path: BoundedCloudPath, start: int, end: Optional[int] = None
    ) -> bytes:
        """Download the inclusive byte range `start`-`end`, or from `start` to the end of the
        object when `end` is None."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support streaming I/O (_range_download). "
            "Implement this method or use a non-streaming file_cache_mode."
        )

    def _get_content_length(self, cloud_path: BoundedCloudPath) -> Optional[int]:
        """Object size without downloading it, or None when the provider cannot say.

        `stat()` is one metadata request on every provider (HEAD-equivalent through
        `_get_metadata`), so this is as cheap as a dedicated size call would be.
        """
        try:
            return cloud_path.stat().st_size
        except NoStatError as e:
            raise CloudPathFileNotFoundError(f"Object not found: {cloud_path}") from e

    def _initiate_multipart_upload(self, cloud_path: BoundedCloudPath) -> str:
        """Start a multipart upload."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support streaming I/O (_initiate_multipart_upload)."
        )

    def _upload_part(
        self, cloud_path: BoundedCloudPath, upload_id: str, part_number: int, data: bytes
    ) -> _UploadPart:
        """Upload one part."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support streaming I/O (_upload_part)."
        )

    def _complete_multipart_upload(
        self, cloud_path: BoundedCloudPath, upload_id: str, parts: Sequence[_UploadPart]
    ) -> None:
        """Complete a multipart upload."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support streaming I/O (_complete_multipart_upload)."
        )

    def _abort_multipart_upload(self, cloud_path: BoundedCloudPath, upload_id: str) -> None:
        """Abort a multipart upload."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support streaming I/O (_abort_multipart_upload)."
        )

    def _put_object(self, cloud_path: BoundedCloudPath, data: BinaryIO) -> None:
        """Upload a whole object in one request from a readable binary stream."""
        raise NotImplementedError(
            f"{type(self).__name__} does not support streaming I/O (_put_object)."
        )
