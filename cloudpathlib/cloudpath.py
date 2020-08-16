import abc
import fnmatch
import os
from pathlib import Path, PosixPath, PurePosixPath, WindowsPath
from tempfile import TemporaryDirectory
from urllib.parse import urlparse
from warnings import warn

from ._vendored import resolve


PurePosixPath.resolve = resolve


# Custom Exceptions
class BackendMismatch(ValueError):
    pass


class InvalidPrefix(ValueError):
    pass


class OverwriteDirtyFile(Exception):
    pass


class OverwriteNewerCloud(Exception):
    pass


class OverwriteNewerLocal(Exception):
    pass


# Abstract base class
class CloudPath(abc.ABC):
    class Meta:
        cloud_prefix = None
        backend_class = None

    def __init__(self, cloud_path, backend=None, local_cache_dir=None):
        self.is_valid_cloudpath(cloud_path, raise_on_error=True)

        # versions of the raw string that provide useful methods
        self._str = str(cloud_path)
        self._url = urlparse(self._str)
        self._path = PurePosixPath(f"/{self._no_prefix}")

        # setup backend connection
        if backend is None:
            # instantiate with defaults
            backend = self.backend_class()

        if type(backend) != self.backend_class:
            raise BackendMismatch(
                f"Backend of type ({backend.__class__}) is not valid for cloud path of type "
                f"({self.__class__}); must be instantiation of ({self.backend_class}) or None "
                f"to be instantiated with defaults for that backend."
            )

        self.backend = backend

        # setup caching and local versions of file and track if it is a tmp dir
        self._cache_tmp_dir = None
        if local_cache_dir is None:
            self._cache_tmp_dir = TemporaryDirectory()
            local_cache_dir = self._cache_tmp_dir.name

        self._local_cache_dir = Path(local_cache_dir)

        # track if local has been written to, if so it may need to be uploaded
        self._dirty = False

        # handle if local file gets opened
        self._handle = None

    def __del__(self):
        # make sure that file handle to local path is closed
        if self._handle is not None:
            self._handle.close()

        # make sure temp is cleaned up if we created it
        if self._cache_tmp_dir is not None:
            self._cache_tmp_dir.cleanup()

    @property
    def _no_prefix(self):
        return self._str[len(self.cloud_prefix) :]

    @property
    def _no_prefix_no_drive(self):
        return self._str[len(self.cloud_prefix) + len(self.drive) :]

    @classmethod
    def is_valid_cloudpath(cls, path, raise_on_error=False):
        valid = str(path).lower().startswith(cls.cloud_prefix.lower())

        if raise_on_error and not valid:
            raise InvalidPrefix(
                f"'{path}' is not a valid path since it does not start with '{cls.cloud_prefix}'"
            )

        return valid

    def __repr__(self):
        return f"{self.__class__.__name__}('{self}')"

    def __str__(self):
        return self._str

    def __hash__(self):
        # use repr for type and resolved path to assess if paths are the same
        return hash(self.__repr__)

    def __eq__(self, other):
        return repr(self) == repr(other)

    # ====================== NOT IMPLEMENTED ======================
    # absolute - no cloud equivalent; all cloud paths are absolute already
    # as_posix - no cloud equivalent; not needed since we assume url separator
    # chmod - permission changing should be explicitly done per backend with methods
    #           that make sense for the backend permission options
    # cwd - no cloud equivalent
    # expanduser - no cloud equivalent
    # group - should be implemented with backend-specific permissions
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
    # resolve - all cloud paths are absolute, no no resolving
    # root - drive already has the bucket and anchor/prefix has the scheme, so nothing to store here
    # symlink_to - no cloud equivalent

    # ====================== REQUIRED, NOT GENERIC ======================
    # Methods that must be implemented, but have no generic application
    @abc.abstractproperty
    def drive(self):
        """ For example "bucket" on S3 or "container" on Azure; needs to be defined for each class
        """
        pass

    @abc.abstractmethod
    def is_dir(self):
        """ Should be implemented without requiring a dir is downloaded
        """
        pass

    @abc.abstractmethod
    def is_file(self):
        """ Should be implemented without requiring that the file is downloaded
        """
        pass

    @abc.abstractmethod
    def mkdir(self, parents=False, exist_ok=False):
        """ Should be implemented using the backend API without requiring a dir is downloaded
        """
        pass

    @abc.abstractmethod
    def touch(self):
        """ Should be implemented using the backend API to create and update modified time
        """
        pass

    # ====================== IMPLEMENTED FROM SCRATCH ======================
    # Methods with their own implementations that work generically
    def __rtruediv__(self, other):
        raise ValueError(
            "Cannot change a cloud path's root since all paths are absolute; create a new path instead."
        )

    @property
    def anchor(self):
        return self.cloud_prefix

    def as_uri(self):
        return str(self)

    def exists(self):
        return self.backend.exists(self)

    def glob(self, pattern):
        """ Should be implemented using the backend API without requiring a dir is downloaded
        """
        # strip cloud prefix from pattern if it is included
        if pattern.startswith(self.cloud_prefix):
            pattern = pattern[len(self.cloud_prefix) :]

        # strip "drive" from pattern if it is included
        if pattern.startswith(self.drive):
            pattern = pattern[len(self.drive) :]

        # identify if pattern is recursive or not
        recursive = False
        if pattern.startswith("**/"):
            pattern = pattern.split("/", 1)[-1]
            recursive = True

        for f in self.backend.list_dir(self, recursive=recursive):
            if fnmatch.fnmatch(f._no_prefix_no_drive, pattern):
                yield f

    def iterdir(self):
        for f in self.backend.list_dir(self):
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
    ):
        # if trying to call open on a direcotry that exists
        if self.exists() and not self.is_file():
            raise ValueError(f"Cannot open directory, only files. Tried to open ({self})")

        if mode == "x" and self.exists():
            raise ValueError(f"Cannot open existing file ({self}) for creation.")

        # TODO: consider streaming from backend rather than DLing entire file to cache
        self._refresh_cache(force_overwrite_from_cloud=force_overwrite_from_cloud)

        # create any directories that may be needed if the file is new
        if not self._local.exists():
            self._local.parent.mkdir(parents=True, exist_ok=True)

        buffer = self._local.open(
            mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline,
        )

        # write modes need special on closing the buffer
        if any(m in mode for m in ("w", "+", "x", "a")):
            # dirty, handle, patch close

            original_close = buffer.close

            # since we are pretending this is a cloud file, upload it to the cloud
            # when the buffer is closed
            def _patched_close(*args, **kwargs):
                original_close(*args, **kwargs)
                self._upload_local_to_cloud(force_overwrite_to_cloud=force_overwrite_to_cloud)

            buffer.close = _patched_close

            # keep reference in case we need to close when __del__ is called on this object
            self._handle = buffer

            # opened for write, so mark dirty
            self._dirty = True

        return buffer

    def replace(self, target):
        if type(self) != type(target):
            raise ValueError(
                f"The target based to rename must be an instantiated class of type: {type(self)}"
            )

        if target.exists():
            target.unlink()

        self.backend.move_file(self, target)
        return target

    def rename(self, target):
        # for cloud services replace == rename since we don't just rename,
        # we actually move files
        return self.replace(target)

    def rglob(self, pattern):
        return self.glob("**/" + pattern)

    def rmdir(self):
        self.backend.remove(self)

    def samepath(self, other_path):
        # all cloud paths are absolute and the paths are used for hash
        return self == other_path

    def unlink(self):
        self.backend.remove(self)

    def write_bytes(self, data):
        """ Open the file in bytes mode, write to it, and close the file.

            NOTE: vendored from pathlib since we override open
            https://github.com/python/cpython/blob/3.8/Lib/pathlib.py#L1235-L1242
        """
        # type-check for the buffer interface before truncating the file
        view = memoryview(data)
        with self.open(mode="wb") as f:
            return f.write(view)

    def write_text(self, data, encoding=None, errors=None):
        """ Open the file in text mode, write to it, and close the file.

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
        """ Some functions we can just dispatch to the pathlib version
            We want to do this explicitly so we don't have to support all
            of pathlib and subclasses can override individually if necessary.
        """
        path_version = self._path.__getattribute__(func)

        # Path functions should be called so the results are calculated
        if callable(path_version):
            path_version = path_version(*args, **kwargs)

        # Paths should always be resolved and then converted to the same backend + class as this one
        if isinstance(path_version, PurePosixPath):
            # always resolve since cloud paths must be absolute
            path_version = path_version.resolve()
            return self._new_cloudpath(path_version)

        # when pathlib returns a string, etc. we probably just want that thing
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
        if path_pattern.startswith(self.anchor):
            path_pattern = path_pattern[len(self.anchor) :]

            # if we started with the anchor assume we want to
            # match "rootness" of the patter in the posix version
            path_pattern = "/" + path_pattern

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

    def relative_to(self, *other):
        self._dispatch_to_path("relative_to", *other)

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

        # Paths should always be resolved and then converted to the same backend + class as this one
        if isinstance(path_version, (PosixPath, WindowsPath)):
            # always resolve since cloud paths must be absolute
            path_version = path_version.resolve()
            return self._new_cloudpath(path_version)

        # when pathlib returns a string, etc. we probably just want that thing
        else:
            return path_version

    @property
    def stat(self):
        """ Note: for many backends, we may want to override so we don't incur
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
    def download_to(self, destination):
        destination = Path(destination)
        if self.is_file():
            self.backend.download_file(self, destination)
        else:
            for f in self.iterdir():
                rel_dest = f.relative_to(self)
                f.download_to(rel_dest)

    # ===========  private cloud methods ===============
    @property
    def _local(self):
        """ Cached local version of the file.
        """
        return self._local_cache_dir / self._no_prefix

    def _new_cloudpath(self, path):
        """ Use the scheme, backend, cache dir of this cloudpath to instantiate
            a new cloudpath of the same type with the path passed.

            Used to make results of iterdir and joins have a unified backend + cache.
        """
        path = str(path)

        # strip initial "/" if path has one
        if path.startswith("/"):
            path = path[1:]

        # add prefix/anchor if it is not already
        if not path.startswith(self.cloud_prefix):
            path = f"{self.cloud_prefix}{path}"

        return self.__class__(path, backend=self.backend, local_cache_dir=self._local_cache_dir)

    def _refresh_cache(self, force_overwrite_from_cloud=False):
        # nothing to cache if the file does not exist; happens when creating
        # new files that will be uploaded
        if not self.exists():
            return

        if self.is_dir():
            raise ValueError("Only individual files can be cached")

        # if not exist or cloud newer
        if (
            not self._local.exists()
            or (self._local.stat().st_mtime < self.stat().st_mtime)
            or force_overwrite_from_cloud
        ):
            # ensure there is a home for the file
            self._local.parent.mkdir(parents=True, exist_ok=True)
            self.download_to(self._local)

            # force cache time to match cloud times
            os.utime(self._local, times=(self.stat().st_mtime, self.stat().st_mtime,))

        if self._dirty:
            raise OverwriteDirtyFile(
                f"Local file ({self._local}) for cloud path ({self}) has been changed by your code, but "
                f"is being requested for download from cloud. Either (1) push your changes to the cloud, "
                f"(2) remove the local file, or (3) pass `force_overwrite_from_cloud=True` to "
                f"overwrite."
            )

        # if local newer but not dirty, it was updated
        # by a separate process; do not overwrite unless forced to
        if self._local.stat().st_mtime > self.stat().st_mtime:
            raise OverwriteNewerLocal(
                f"Local file ({self._local}) for cloud path ({self}) is newer on disk, but "
                f"is being requested for download from cloud. Either (1) push your changes to the cloud, "
                f"(2) remove the local file, or (3) pass `force_overwrite_from_cloud=True` to "
                f"overwrite."
            )

    def _upload_local_to_cloud(self, force_overwrite_to_cloud=False):
        # We should never try to be syncing entire directories; we should only
        # cache and upload individual files.
        if self._local.is_dir():
            raise ValueError("Only individual files can be uploaded to the cloud")

        # if cloud does not exist or local is newer or we are overwriting, do the upload
        if (
            not self.exists()  # cloud does not exist
            or (self._local.stat().st_mtime > self.stat().st_mtime)
            or force_overwrite_to_cloud
        ):
            self.backend.upload_file(
                self._local, self,
            )
            return self

        # cloud is newer and we are not overwriting
        raise OverwriteNewerCloud(
            f"Local file ({self._local}) for cloud path ({self}) is newer in the cloud disk, but "
            f"is being requested to be uploaded to the cloud. Either (1) redownload changes from the cloud or "
            f"(2) pass `force_overwrite_to_cloud=True` to "
            f"overwrite."
        )
