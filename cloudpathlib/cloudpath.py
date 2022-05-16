import abc
from collections import defaultdict
import collections.abc
from contextlib import contextmanager
import os
from pathlib import (  # type: ignore
    Path,
    PosixPath,
    PurePosixPath,
    WindowsPath,
    _make_selector,
    _posix_flavour,
    _PathParents,
)
from typing import Any, IO, Iterable, Dict, Optional, TYPE_CHECKING, Union
from urllib.parse import urlparse
from warnings import warn

from . import anypath

from .exceptions import (
    ClientMismatchError,
    CloudPathFileExistsError,
    CloudPathIsADirectoryError,
    CloudPathNotADirectoryError,
    CloudPathNotImplementedError,
    DirectoryNotEmptyError,
    IncompleteImplementationError,
    InvalidPrefixError,
    MissingDependenciesError,
    NoStatError,
    OverwriteDirtyFileError,
    OverwriteNewerCloudError,
    OverwriteNewerLocalError,
)


if TYPE_CHECKING:
    from .client import Client


class CloudImplementation:
    def __init__(self):
        self.name = None
        self.dependencies_loaded = True
        self._client_class = None
        self._path_class = None

    def validate_completeness(self):
        expected = ["client_class", "path_class"]
        missing = [cls for cls in expected if getattr(self, f"_{cls}") is None]
        if missing:
            raise IncompleteImplementationError(
                f"Implementation is missing registered components: {missing}"
            )
        if not self.dependencies_loaded:
            raise MissingDependenciesError(
                f"Missing dependencies for {self._client_class.__name__}. You can install them "
                f"with 'pip install cloudpathlib[{self.name}]'."
            )

    @property
    def client_class(self):
        self.validate_completeness()
        return self._client_class

    @property
    def path_class(self):
        self.validate_completeness()
        return self._path_class


implementation_registry: defaultdict = defaultdict(CloudImplementation)


def register_path_class(key: str):
    def decorator(cls: type):
        if not issubclass(cls, CloudPath):
            raise TypeError("Only subclasses of CloudPath can be registered.")
        global implementation_registry
        implementation_registry[key]._path_class = cls
        cls._cloud_meta = implementation_registry[key]
        return cls

    return decorator


class CloudPathMeta(abc.ABCMeta):
    def __call__(cls, cloud_path, *args, **kwargs):
        # cls is a class that is the instance of this metaclass, e.g., CloudPath

        # Dispatch to subclass if base CloudPath
        if cls == CloudPath:
            for implementation in implementation_registry.values():
                path_class = implementation._path_class
                if path_class is not None and path_class.is_valid_cloudpath(
                    cloud_path, raise_on_error=False
                ):
                    # Instantiate path_class instance
                    new_obj = path_class.__new__(path_class, cloud_path, *args, **kwargs)
                    if isinstance(new_obj, path_class):
                        path_class.__init__(new_obj, cloud_path, *args, **kwargs)
                    return new_obj
            valid = [
                impl._path_class.cloud_prefix
                for impl in implementation_registry.values()
                if impl._path_class is not None
            ]
            raise InvalidPrefixError(
                f"Path {cloud_path} does not begin with a known prefix " f"{valid}."
            )

        # Otherwise instantiate as normal
        new_obj = cls.__new__(cls, cloud_path, *args, **kwargs)
        if isinstance(new_obj, cls):
            cls.__init__(new_obj, cloud_path, *args, **kwargs)
        return new_obj

    def __init__(cls, name, bases, dic):
        # Copy docstring from pathlib.Path
        for attr in dir(cls):
            if (
                not attr.startswith("_")
                and hasattr(Path, attr)
                and hasattr(getattr(Path, attr), "__doc__")
            ):
                docstring = getattr(Path, attr).__doc__ + " _(Docstring copied from pathlib.Path)_"
                getattr(cls, attr).__doc__ = docstring
                if isinstance(getattr(cls, attr), property):
                    # Properties have __doc__ duplicated under fget, and at least some parsers
                    # read it from there.
                    getattr(cls, attr).fget.__doc__ = docstring


# Abstract base class
class CloudPath(metaclass=CloudPathMeta):
    """Base class for cloud storage file URIs, in the style of the Python standard library's
    [`pathlib` module](https://docs.python.org/3/library/pathlib.html). Instances represent a path
    in cloud storage with filesystem path semantics, and convenient methods allow for basic
    operations like joining, reading, writing, iterating over contents, etc. `CloudPath` almost
    entirely mimics the [`pathlib.Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path)
    interface, so most familiar properties and methods should be available and behave in the
    expected way.

    Analogous to the way `pathlib.Path` works, instantiating `CloudPath` will instead create an
    instance of an appropriate subclass that implements a particular cloud storage service, such as
    [`S3Path`](../s3path). This dispatching behavior is based on the URI scheme part of a cloud
    storage URI (e.g., `"s3://"`).
    """

    _cloud_meta: CloudImplementation
    cloud_prefix: str

    def __init__(self, cloud_path: Union[str, "CloudPath"], client: Optional["Client"] = None):
        self.is_valid_cloudpath(cloud_path, raise_on_error=True)

        # versions of the raw string that provide useful methods
        self._str = str(cloud_path)
        self._url = urlparse(self._str)
        self._path = PurePosixPath(f"/{self._no_prefix}")

        # setup client
        if client is None:
            if isinstance(cloud_path, CloudPath):
                client = cloud_path.client
            else:
                client = self._cloud_meta.client_class.get_default_client()
        if not isinstance(client, self._cloud_meta.client_class):
            raise ClientMismatchError(
                f"Client of type [{client.__class__}] is not valid for cloud path of type "
                f"[{self.__class__}]; must be instance of [{self._cloud_meta.client_class}], or "
                f"None to use default client for this cloud path class."
            )
        self.client: Client = client

        # track if local has been written to, if so it may need to be uploaded
        self._dirty = False

        # handle if local file gets opened
        self._handle = None

    def __del__(self):
        # make sure that file handle to local path is closed
        if self._handle is not None:
            self._handle.close()

    def __getstate__(self):
        state = self.__dict__.copy()

        # don't pickle client
        del state["client"]

        return state

    def __setstate__(self, state):
        client = self._cloud_meta.client_class.get_default_client()
        state["client"] = client
        return self.__dict__.update(state)

    @property
    def _no_prefix(self) -> str:
        return self._str[len(self.cloud_prefix) :]

    @property
    def _no_prefix_no_drive(self) -> str:
        return self._str[len(self.cloud_prefix) + len(self.drive) :]

    @classmethod
    def is_valid_cloudpath(cls, path: Union[str, "CloudPath"], raise_on_error=False) -> bool:
        valid = str(path).lower().startswith(cls.cloud_prefix.lower())

        if raise_on_error and not valid:
            raise InvalidPrefixError(
                f"'{path}' is not a valid path since it does not start with '{cls.cloud_prefix}'"
            )

        return valid

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self}')"

    def __str__(self) -> str:
        return self._str

    def __hash__(self) -> int:
        return hash((type(self).__name__, str(self)))

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and str(self) == str(other)

    def __fspath__(self):
        if self.is_file():
            self._refresh_cache(force_overwrite_from_cloud=False)
        return str(self._local)

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.parts < other.parts

    def __le__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.parts <= other.parts

    def __gt__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.parts > other.parts

    def __ge__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.parts >= other.parts

    # ====================== NOT IMPLEMENTED ======================
    # absolute - no cloud equivalent; all cloud paths are absolute already
    # as_posix - no cloud equivalent; not needed since we assume url separator
    # chmod - permission changing should be explicitly done per client with methods
    #           that make sense for the client permission options
    # cwd - no cloud equivalent
    # expanduser - no cloud equivalent
    # group - should be implemented with client-specific permissions
    # home - no cloud equivalent
    # is_absolute - no cloud equivalent; all cloud paths are absolute already
    # is_block_device - no cloud equivalent
    # is_char_device - no cloud equivalent
    # is_fifo - no cloud equivalent
    # is_mount - no cloud equivalent
    # is_reserved - no cloud equivalent
    # is_socket - no cloud equivalent
    # is_symlink - no cloud equivalent
    # lchmod - no cloud equivalent
    # lstat - no cloud equivalent
    # owner - no cloud equivalent
    # relative to - cloud paths are absolute
    # resolve - all cloud paths are absolute, so no resolving
    # root - drive already has the bucket and anchor/prefix has the scheme, so nothing to store here
    # symlink_to - no cloud equivalent

    # ====================== REQUIRED, NOT GENERIC ======================
    # Methods that must be implemented, but have no generic application
    @property
    @abc.abstractmethod
    def drive(self) -> str:
        """For example "bucket" on S3 or "container" on Azure; needs to be defined for each class"""
        pass

    @abc.abstractmethod
    def is_dir(self) -> bool:
        """Should be implemented without requiring a dir is downloaded"""
        pass

    @abc.abstractmethod
    def is_file(self) -> bool:
        """Should be implemented without requiring that the file is downloaded"""
        pass

    @abc.abstractmethod
    def mkdir(self, parents: bool = False, exist_ok: bool = False):
        """Should be implemented using the client API without requiring a dir is downloaded"""
        pass

    @abc.abstractmethod
    def touch(self):
        """Should be implemented using the client API to create and update modified time"""
        pass

    # ====================== IMPLEMENTED FROM SCRATCH ======================
    # Methods with their own implementations that work generically
    def __rtruediv__(self, other):
        raise ValueError(
            "Cannot change a cloud path's root since all paths are absolute; create a new path instead."
        )

    @property
    def anchor(self) -> str:
        return self.cloud_prefix

    def as_uri(self) -> str:
        return str(self)

    def exists(self) -> bool:
        return self.client._exists(self)

    @property
    def fspath(self) -> str:
        return self.__fspath__()

    def _glob_checks(self, pattern):
        if ".." in pattern:
            raise CloudPathNotImplementedError(
                "Relative paths with '..' not supported in glob patterns."
            )

        if pattern.startswith(self.cloud_prefix) or pattern.startswith("/"):
            raise CloudPathNotImplementedError("Non-relative patterns are unsupported")

    def _glob(self, selector):
        root = _CloudPathSelectable(
            PurePosixPath(self._no_prefix_no_drive),
            {
                PurePosixPath(c._no_prefix_no_drive): is_dir
                for c, is_dir in self.client._list_dir(self, recursive=True)
            },
            is_dir=True,
            exists=True,
        )

        for p in selector.select_from(root):
            yield self.client.CloudPath(f"{self.cloud_prefix}{self.drive}{p}")

    def glob(self, pattern):
        self._glob_checks(pattern)

        pattern_parts = PurePosixPath(pattern).parts
        selector = _make_selector(tuple(pattern_parts), _posix_flavour)

        yield from self._glob(selector)

    def rglob(self, pattern):
        self._glob_checks(pattern)

        pattern_parts = PurePosixPath(pattern).parts
        selector = _make_selector(("**",) + tuple(pattern_parts), _posix_flavour)

        yield from self._glob(selector)

    def iterdir(self) -> Iterable["CloudPath"]:
        for f, _ in self.client._list_dir(self, recursive=False):
            if f != self:  # iterdir does not include itself in pathlib
                yield f

    def open(
        self,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        force_overwrite_from_cloud=False,  # extra kwarg not in pathlib
        force_overwrite_to_cloud=False,  # extra kwarg not in pathlib
    ) -> IO:
        # if trying to call open on a directory that exists
        if self.exists() and not self.is_file():
            raise CloudPathIsADirectoryError(
                f"Cannot open directory, only files. Tried to open ({self})"
            )

        if mode == "x" and self.exists():
            raise CloudPathFileExistsError(f"Cannot open existing file ({self}) for creation.")

        # TODO: consider streaming from client rather than DLing entire file to cache
        self._refresh_cache(force_overwrite_from_cloud=force_overwrite_from_cloud)

        # create any directories that may be needed if the file is new
        if not self._local.exists():
            self._local.parent.mkdir(parents=True, exist_ok=True)
            original_mtime = 0
        else:
            original_mtime = self._local.stat().st_mtime

        buffer = self._local.open(
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )

        # write modes need special on closing the buffer
        if any(m in mode for m in ("w", "+", "x", "a")):
            # dirty, handle, patch close
            original_close = buffer.close

            # since we are pretending this is a cloud file, upload it to the cloud
            # when the buffer is closed
            def _patched_close(*args, **kwargs):
                original_close(*args, **kwargs)

                # original mtime should match what was in the cloud; because of system clocks or rounding
                # by the cloud provider, the new version in our cache is "older" than the original version;
                # explicitly set the new modified time to be after the original modified time.
                if self._local.stat().st_mtime < original_mtime:
                    new_mtime = original_mtime + 1
                    os.utime(self._local, times=(new_mtime, new_mtime))

                self._upload_local_to_cloud(force_overwrite_to_cloud=force_overwrite_to_cloud)

            buffer.close = _patched_close

            # keep reference in case we need to close when __del__ is called on this object
            self._handle = buffer

            # opened for write, so mark dirty
            self._dirty = True

        return buffer

    def replace(self, target: "CloudPath") -> "CloudPath":
        if type(self) != type(target):
            raise TypeError(
                f"The target based to rename must be an instantiated class of type: {type(self)}"
            )

        if target.exists():
            target.unlink()

        self.client._move_file(self, target)
        return target

    def rename(self, target: "CloudPath") -> "CloudPath":
        # for cloud services replace == rename since we don't just rename,
        # we actually move files
        return self.replace(target)

    def rmdir(self):
        if self.is_file():
            raise CloudPathNotADirectoryError(
                f"Path {self} is a file; call unlink instead of rmdir."
            )
        try:
            next(self.iterdir())
            raise DirectoryNotEmptyError(
                f"Directory not empty: '{self}'. Use rmtree to delete recursively."
            )
        except StopIteration:
            pass
        self.client._remove(self)

    def samefile(self, other_path: "CloudPath") -> bool:
        # all cloud paths are absolute and the paths are used for hash
        return self == other_path

    def unlink(self):
        if self.is_dir():
            raise CloudPathIsADirectoryError(
                f"Path {self} is a directory; call rmdir instead of unlink."
            )
        self.client._remove(self)

    def write_bytes(self, data: bytes):
        """Open the file in bytes mode, write to it, and close the file.

        NOTE: vendored from pathlib since we override open
        https://github.com/python/cpython/blob/3.8/Lib/pathlib.py#L1235-L1242
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        with self.open(mode="wb") as f:
            return f.write(view)

    def write_text(self, data: str, encoding=None, errors=None):
        """Open the file in text mode, write to it, and close the file.

        NOTE: vendored from pathlib since we override open
        https://github.com/python/cpython/blob/3.8/Lib/pathlib.py#L1244-L1252
        """
        if not isinstance(data, str):
            raise TypeError("data must be str, not %s" % data.__class__.__name__)
        with self.open(mode="w", encoding=encoding, errors=errors) as f:
            return f.write(data)

    # ====================== DISPATCHED TO POSIXPATH FOR PURE PATHS ======================
    # Methods that are dispatched to exactly how pathlib.PurePosixPath would calculate it on
    # self._path for pure paths (does not matter if file exists);
    # see the next session for ones that require a real file to exist
    def _dispatch_to_path(self, func, *args, **kwargs):
        """Some functions we can just dispatch to the pathlib version
        We want to do this explicitly so we don't have to support all
        of pathlib and subclasses can override individually if necessary.
        """
        path_version = self._path.__getattribute__(func)

        # Path functions should be called so the results are calculated
        if callable(path_version):
            path_version = path_version(*args, **kwargs)

        # Paths should always be resolved and then converted to the same client + class as this one
        if isinstance(path_version, PurePosixPath):
            # always resolve since cloud paths must be absolute
            path_version = _resolve(path_version)
            return self._new_cloudpath(path_version)

        # When sequence of PurePosixPath, we want to convert to sequence of CloudPaths
        if (
            isinstance(path_version, collections.abc.Sequence)
            and len(path_version) > 0
            and isinstance(path_version[0], PurePosixPath)
        ):
            sequence_class = (
                type(path_version) if not isinstance(path_version, _PathParents) else tuple
            )
            return sequence_class(
                self._new_cloudpath(_resolve(p)) for p in path_version if _resolve(p) != p.root
            )

        # when pathlib something else, we probably just want that thing
        # cases this should include: str, empty sequence, sequence of str, ...
        else:
            return path_version

    def __truediv__(self, other):
        if not isinstance(other, (str,)):
            raise TypeError(f"Can only join path {repr(self)} with strings.")

        return self._dispatch_to_path("__truediv__", other)

    def joinpath(self, *args):
        return self._dispatch_to_path("joinpath", *args)

    @property
    def name(self):
        return self._dispatch_to_path("name")

    def match(self, path_pattern):
        # strip scheme from start of pattern before testing
        if path_pattern.startswith(self.anchor + self.drive + "/"):
            path_pattern = path_pattern[len(self.anchor + self.drive + "/") :]

        return self._dispatch_to_path("match", path_pattern)

    @property
    def parent(self):
        return self._dispatch_to_path("parent")

    @property
    def parents(self):
        return self._dispatch_to_path("parents")

    @property
    def parts(self):
        parts = self._dispatch_to_path("parts")
        if parts[0] == "/":
            parts = parts[1:]

        return (self.anchor, *parts)

    @property
    def stem(self):
        return self._dispatch_to_path("stem")

    @property
    def suffix(self):
        return self._dispatch_to_path("suffix")

    @property
    def suffixes(self):
        return self._dispatch_to_path("suffixes")

    def with_name(self, name):
        return self._dispatch_to_path("with_name", name)

    def with_suffix(self, suffix):
        return self._dispatch_to_path("with_suffix", suffix)

    # ====================== DISPATCHED TO LOCAL CACHE FOR CONCRETE PATHS ======================
    # Items that can be executed on the cached file on the local filesystem
    def _dispatch_to_local_cache_path(self, func, *args, **kwargs):
        self._refresh_cache()

        path_version = self._local.__getattribute__(func)

        # Path functions should be called so the results are calculated
        if callable(path_version):
            path_version = path_version(*args, **kwargs)

        # Paths should always be resolved and then converted to the same client + class as this one
        if isinstance(path_version, (PosixPath, WindowsPath)):
            # always resolve since cloud paths must be absolute
            path_version = path_version.resolve()
            return self._new_cloudpath(path_version)

        # when pathlib returns a string, etc. we probably just want that thing
        else:
            return path_version

    @property
    def stat(self):
        """Note: for many clients, we may want to override so we don't incur
        network costs since many of these properties are available as
        API calls.
        """
        warn(
            f"stat not implemented as API call for {self.__class__} so file must be downloaded to "
            f"calculate stats; this may take a long time depending on filesize"
        )
        return self._dispatch_to_local_cache_path("stat")

    def read_bytes(self):
        return self._dispatch_to_local_cache_path("read_bytes")

    def read_text(self):
        return self._dispatch_to_local_cache_path("read_text")

    # ===========  public cloud methods, not in pathlib ===============
    def download_to(self, destination: Union[str, os.PathLike]) -> Path:
        destination = Path(destination)
        if self.is_file():
            if destination.is_dir():
                destination = destination / self.name
            return self.client._download_file(self, destination)
        else:
            destination.mkdir(exist_ok=True)
            for f in self.iterdir():
                rel = str(self)
                if not rel.endswith("/"):
                    rel = rel + "/"

                rel_dest = str(f)[len(rel) :]
                f.download_to(destination / rel_dest)

            return destination

    def rmtree(self):
        """Delete an entire directory tree."""
        if self.is_file():
            raise CloudPathNotADirectoryError(
                f"Path {self} is a file; call unlink instead of rmtree."
            )
        self.client._remove(self)

    def upload_from(
        self, source: Union[str, os.PathLike], force_overwrite_to_cloud: bool = False
    ) -> "CloudPath":
        """Upload a file or directory to the cloud path."""
        source = Path(source)

        if source.is_dir():
            for p in source.iterdir():
                (self / p.name).upload_from(p, force_overwrite_to_cloud=force_overwrite_to_cloud)

            return self

        else:
            if self.exists() and self.is_dir():
                dst = self / source.name
            else:
                dst = self

            dst._upload_file_to_cloud(source, force_overwrite_to_cloud=force_overwrite_to_cloud)

            return dst

    def copy(
        self,
        destination: Union[str, os.PathLike, "CloudPath"],
        force_overwrite_to_cloud: bool = False,
    ) -> Union[Path, "CloudPath"]:
        """Copy self to destination folder of file, if self is a file."""
        if not self.exists() or not self.is_file():
            raise ValueError(
                f"Path {self} should be a file. To copy a directory tree use the method copytree."
            )

        # handle string version of cloud paths + local paths
        if isinstance(destination, (str, os.PathLike)):
            destination = anypath.to_anypath(destination)

        if not isinstance(destination, CloudPath):
            return self.download_to(destination)

        # if same client, use cloud-native _move_file on client to avoid downloading
        elif self.client is destination.client:
            if destination.exists() and destination.is_dir():
                destination: CloudPath = destination / self.name  # type: ignore

            if (
                not force_overwrite_to_cloud
                and destination.exists()
                and destination.stat().st_mtime >= self.stat().st_mtime
            ):
                raise OverwriteNewerCloudError(
                    f"File ({destination}) is newer than ({self}). "
                    f"To overwrite "
                    f"pass `force_overwrite_to_cloud=True`."
                )

            return self.client._move_file(self, destination, remove_src=False)

        else:
            if not destination.exists() or destination.is_file():
                return destination.upload_from(
                    self.fspath, force_overwrite_to_cloud=force_overwrite_to_cloud
                )
            else:
                return (destination / self.name).upload_from(
                    self.fspath, force_overwrite_to_cloud=force_overwrite_to_cloud
                )

    def copytree(
        self,
        destination: Union[str, os.PathLike, "CloudPath"],
        force_overwrite_to_cloud: bool = False,
    ) -> Union[Path, "CloudPath"]:
        """Copy self to a directory, if self is a directory."""
        if not self.is_dir():
            raise CloudPathNotADirectoryError(
                f"Origin path {self} must be a directory. To copy a single file use the method copy."
            )

        # handle string version of cloud paths + local paths
        if isinstance(destination, (str, os.PathLike)):
            destination = anypath.to_anypath(destination)

        if destination.exists() and destination.is_file():
            raise CloudPathFileExistsError(
                "Destination path {destination} of copytree must be a directory."
            )

        destination.mkdir(parents=True, exist_ok=True)

        for subpath in self.iterdir():
            if subpath.is_file():
                subpath.copy(
                    destination / subpath.name, force_overwrite_to_cloud=force_overwrite_to_cloud
                )
            elif subpath.is_dir():
                subpath.copytree(
                    destination / subpath.name, force_overwrite_to_cloud=force_overwrite_to_cloud
                )

        return destination

    # ===========  private cloud methods ===============
    @property
    def _local(self):
        """Cached local version of the file."""
        return self.client._local_cache_dir / self._no_prefix

    def _new_cloudpath(self, path):
        """Use the scheme, client, cache dir of this cloudpath to instantiate
        a new cloudpath of the same type with the path passed.

        Used to make results of iterdir and joins have a unified client + cache.
        """
        path = str(path)

        # strip initial "/" if path has one
        if path.startswith("/"):
            path = path[1:]

        # add prefix/anchor if it is not already
        if not path.startswith(self.cloud_prefix):
            path = f"{self.cloud_prefix}{path}"

        return self.client.CloudPath(path)

    def _refresh_cache(self, force_overwrite_from_cloud=False):
        try:
            stats = self.stat()
        except NoStatError:
            # nothing to cache if the file does not exist; happens when creating
            # new files that will be uploaded
            return

        # if not exist or cloud newer
        if (
            not self._local.exists()
            or (self._local.stat().st_mtime < stats.st_mtime)
            or force_overwrite_from_cloud
        ):
            # ensure there is a home for the file
            self._local.parent.mkdir(parents=True, exist_ok=True)
            self.download_to(self._local)

            # force cache time to match cloud times
            os.utime(self._local, times=(stats.st_mtime, stats.st_mtime))

        if self._dirty:
            raise OverwriteDirtyFileError(
                f"Local file ({self._local}) for cloud path ({self}) has been changed by your code, but "
                f"is being requested for download from cloud. Either (1) push your changes to the cloud, "
                f"(2) remove the local file, or (3) pass `force_overwrite_from_cloud=True` to "
                f"overwrite."
            )

        # if local newer but not dirty, it was updated
        # by a separate process; do not overwrite unless forced to
        if self._local.stat().st_mtime > stats.st_mtime:
            raise OverwriteNewerLocalError(
                f"Local file ({self._local}) for cloud path ({self}) is newer on disk, but "
                f"is being requested for download from cloud. Either (1) push your changes to the cloud, "
                f"(2) remove the local file, or (3) pass `force_overwrite_from_cloud=True` to "
                f"overwrite."
            )

    def _upload_local_to_cloud(self, force_overwrite_to_cloud: bool = False):
        """Uploads cache file at self._local to the cloud"""
        # We should never try to be syncing entire directories; we should only
        # cache and upload individual files.
        if self._local.is_dir():
            raise ValueError("Only individual files can be uploaded to the cloud")

        uploaded = self._upload_file_to_cloud(
            self._local, force_overwrite_to_cloud=force_overwrite_to_cloud
        )

        # force cache time to match cloud times
        stats = self.stat()
        os.utime(self._local, times=(stats.st_mtime, stats.st_mtime))

        # reset dirty and handle now that this is uploaded
        self._dirty = False
        self._handle = None

        return uploaded

    def _upload_file_to_cloud(self, local_path, force_overwrite_to_cloud: bool = False):
        """Uploads file at `local_path` to the cloud if there is not a newer file
        already there.
        """
        try:
            stats = self.stat()
        except NoStatError:
            stats = None

        # if cloud does not exist or local is newer or we are overwriting, do the upload
        if (
            not stats  # cloud does not exist
            or (local_path.stat().st_mtime > stats.st_mtime)
            or force_overwrite_to_cloud
        ):
            self.client._upload_file(
                local_path,
                self,
            )

            return self

        # cloud is newer and we are not overwriting
        raise OverwriteNewerCloudError(
            f"Local file ({self._local}) for cloud path ({self}) is newer in the cloud disk, but "
            f"is being requested to be uploaded to the cloud. Either (1) redownload changes from the cloud or "
            f"(2) pass `force_overwrite_to_cloud=True` to "
            f"overwrite."
        )

    # ===========  pydantic integration special methods ===============
    @classmethod
    def __get_validators__(cls):
        """Pydantic special method. See
        https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
        yield cls._validate

    @classmethod
    def _validate(cls, value: Any):
        """Used as a Pydantic validator. See
        https://pydantic-docs.helpmanual.io/usage/types/#custom-data-types"""
        return cls(value)


# The function resolve is not available on Pure paths because it removes relative
# paths and symlinks. We _just_ want the relative path resolution for
# cloud paths, so the other logic is removed.  Also, we can assume that
# cloud paths are absolute.
#
# Based on resolve from pathlib:
# https://github.com/python/cpython/blob/3.8/Lib/pathlib.py#L316-L359
def _resolve(path: PurePosixPath) -> str:
    sep = "/"

    # rebuild path from parts
    newpath = ""
    for name in str(path).split(sep):
        if not name or name == ".":
            # current dir, nothing to add
            continue
        if name == "..":
            # parent dir, drop right-most part
            newpath, _, _ = newpath.rpartition(sep)
            continue
        newpath = newpath + sep + name

    return newpath or sep


# These objects are used to wrap CloudPaths in a context where we can use
# the python pathlib implementations for `glob` and `rglob`, which depend
# on the Selector created by the `_make_selector` method being passed
# an object like the below when `select_from` is called. We implement these methods
# in a simple wrapper to use the same glob recursion and pattern logic without
# rolling our own.
#
# Designed to be compatible when used by these selector implementations from pathlib:
# https://github.com/python/cpython/blob/3.10/Lib/pathlib.py#L385-L500
class _CloudPathSelectableAccessor:
    def __init__(self, scandir_func):
        self.scandir = scandir_func


class _CloudPathSelectable:
    def __init__(
        self,
        relative_cloud_path: PurePosixPath,
        children: Dict[PurePosixPath, bool],
        is_dir: bool,
        exists: bool,
    ):
        self._path = relative_cloud_path
        self._all_children = children

        self._accessor = _CloudPathSelectableAccessor(self.scandir)

        self._is_dir = is_dir
        self._exists = exists

    def __repr__(self):
        return str(self._path)

    def is_dir(self):
        return self._is_dir

    def exists(self):
        return self._exists

    def is_symlink(self):
        return False

    @property
    def name(self):
        return self._path.name

    @staticmethod
    @contextmanager
    def scandir(root):
        yield (
            root._make_child_relpath(c.name)
            for c, _ in root._all_children.items()
            if c.parent == root._path
        )

    def _filter_children(self, rel_to):
        return {
            c: is_dir
            for c, is_dir in self._all_children.items()
            if self._is_relative_to(c, rel_to)
        }

    @staticmethod
    def _is_relative_to(maybe_child, maybe_parent):
        try:
            maybe_child.relative_to(maybe_parent)
            return True
        except ValueError:
            return False

    def _make_child_relpath(self, part):
        child = self._path / part
        filtered_children = self._filter_children(child)

        return _CloudPathSelectable(
            child,
            filtered_children,
            is_dir=self._all_children.get(child, False),
            exists=child in self._all_children,
        )
