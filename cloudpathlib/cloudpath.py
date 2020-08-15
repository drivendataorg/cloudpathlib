import abc
from pathlib import Path, PurePath, PosixPath
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

# research what needs to be implemented; for a path to count
# how much of pathlib can we just use?
# how much of urllib can we just use?

class InvalidPrefix(ValueError):
    pass

class BackendMismatch(ValueError):
    pass


# Abstract base class
class CloudPath(abc.ABC):
    class Meta:
        path_prefix = None
        backend_class = None

    def __init__(self, cloud_path, backend=None, local_cache_dir=None):
        self.is_valid_cloudpath(cloud_path, raise_on_error=True)
        
        # versions of the raw string that provide useful methods
        self._str = str(cloud_path)
        self._url = urlparse(self._str)
        self._path = PosixPath(f"/{self._no_prefix}")

        # setup backend connection
        if backend is None:
            # instantiate with defaults
            backend = self.backend_class()
        
        if not isinstance(backend, self.backend_class):
            raise BackendMismatch(
                f"Backend of type ({backend.__class__}) is not valid for cloud path of type "
                f"({self.__class__}); must be instantiation of ({self.backend_class}) or None "
                f"to be instantiated with defaults for that backend."
            )

        self.backend = backend

        # setup caching and local versions of file and track if it is a tmp dir
        self._local_cache_dir_is_tmp = not bool(local_cache_dir)
        if local_cache_dir is None:
            local_cache_dir = TemporaryDirectory().name

        self._local_cache_dir = Path(local_cache_dir)

        # track if local has been written to, if so it may need to be uploaded
        self._dirty = False 

        # handle if local file gets opened
        self._handle = None

    def __del__(self):
        # make sure that file handle to local path is closed
        self._handle.close()

        # make sure temp is cleaned up if we created it
        if self._local_cache_dir_is_tmp:
            shutil.rmtree(self._local_cache_dir)

    @property
    def _no_prefix(self):
        return self._str[len(self.path_prefix):]

    @classmethod
    def is_valid_cloudpath(cls, path, raise_on_error=False):
        valid = str(path).lower().startswith(cls.path_prefix.lower())

        if raise_on_error and not valid:
            raise InvalidPrefix(
                f"'{path}' is not a valid path since it does not start with '{cls.path_prefix}'"
            )
        
        return valid

    def __repr__(self):
        return f"{self.__class__.__name__}('{self}')"

    def __str__(self):
        return self._str

    def __hash__(self):
        # use repr for type and resolved path to assess if paths are the same
        return self.__repr__

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

    # ====================== REQUIRED, NOT GENERIC ====================== 
    # Methods that must be implemented, but have no generic application
    @abc.abstractproperty
    def drive(self):
        """ For example "bucket" on S3 or "container" on Azure; needs to be defined for each class
        """
        pass

    @abc.abstractproperty
    def exists(self):
        """ Should be implemented without requiring that the file is downloaded
        """
        pass

    @abc.abstractmethod
    def glob(self, pattern):
        """ Should be implemented using the backend API with requiring a dir is downloaded
        """
        pass

    @abc.abstractmethod
    def is_dir(self):
        """ Should be implemented without requiring that the file is downloaded
        """
        pass

    @abc.abstractmethod
    def is_file(self):
        """ Should be implemented without requiring that the file is downloaded
        """
        pass

    @abc.abstractmethod
    def iterdir(self, pattern):
        """ Should be implemented using the backend API with requiring a dir is downloaded
        """
        pass

    @abc.abstractmethod
    def mkdir(self, parents=False, exist_ok=False):
        """ Should be implemented using the backend API with requiring a dir is downloaded
        """
        pass

    # ====================== IMPLEMENTED FROM SCRATCH ====================== 
    # Methods with their own implementations that work generically
    @property
    def anchor(self):
        return self.path_prefix

    def as_uri(self):
        return str(self)

    def __truediv__(self, other):
        if not isinstance(other, (str, )):
            raise TypeError(f"Can only join path {repr(self)} with strings.")

        # handles relative path resolution
        path_result = str((self._path / other), resolve

        return self.__class__(f"{self._url.scheme}://{path_result}", backend=self.backend)

    def __rtruediv__(self, other):
        raise ValueError("Cannot change a cloud path's root since all paths are absolute; create a new path instead.")


    # ====================== DISPATCHED TO POSIXPATH FOR PURE PATHS ====================== 
    # Methods that are dispatched to exactly how pathlib.PosixPath would calculate it on
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
        if isinstance(path_version, PosixPath):
            path_version = path_version.resolve()
            return self._new_cloudpath(path_version)

        # when pathlib returns a string, etc. we probably just want that thing
        else:
            return path_version

    def joinpath(self, *args):
        return._dispatch_to_path('joinpath', *args)

    @property
    def name(self):
        return self._dispatch_to_path('name')

    def match(self, path_pattern):
        # strip scheme from start of pattern before testing
        if path_pattern.startswith(self.anchor):
            path_pattern = path_pattern[len(self.anchor):]

            # if we started with the anchor assume we want to 
            # match "rootness" of the patter in the posix version
            path_pattern = "/" + path_pattern
        
        return self._dispatch_to_path('match', path_pattern)

    @property
    def parent(self):
        return self._dispatch_to_path('parent')

    @property
    def parents(self):
        return self._dispatch_to_path('parents')

    def relative_to(sle,f)

    @property
    def stem(self):
        return self._dispatch_to_path('stem')

    @property
    def suffix(self):
        return self._dispatch_to_path('suffix')

    @property
    def with_name(self, name):
        return self._dispatch_to_path('with_suffix', suffix)

    @property
    def with_suffix(self, suffix):
        return self._dispatch_to_path('with_suffix', suffix)

    # ====================== DISPATCHED TO LOCAL CACHE FOR INSTANTIATED PATHS ====================== 
    # Items that can be executed on the cached file on the local filesystem
    def _dispatch_to_local_cache_path(self, func):
        pass

    @property
    def stat(self):
        ''' Note: for many backends, we may want to override so we don't incur
            network costs since many of these properties are available as
            API calls.
        '''
        pass

    def open(
        self,
        mode='r',
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
    ):
        self._refresh_cache()

        pass

    # ===========  public cloud methods ===============
    def download_to(self, destination):
        destination = Path(destination)
        if self.is_file():
            self.backend.download_file(
                self,
                destination
            )
        else:
            for f in self.iterdir():
                rel_dest = f.relative_to(self)
                f.download_to(
                    rel_dest
                )

    # ===========  private cloud methods ===============
    def _refresh_cache(self):
        # if not exist or cloud newer
        if not self._local.exists() or self._local.stat.mtime < self.stat.mtime:
            self.download_to(
                self._local_cache_dir
            )

        # if local newer
        raise
        
    
    def _new_cloudpath(self, path):
        """ Use the scheme and backend of this cloudpath to instantiate
            a new cloudpath.
        """
        path = str(path)

        # strip initial "/" if path has one
        if path.startswith("/"):
            path = path[1:]

        # strip scheme so that we can add it explicitly
        if not path.startswith(self.anchor):
            path = f"{self.anchor}{path}"

        return self.__class__(path, backend=self.backend, local_cache_dir=self._local_cache_dir)

    @property
    def _local(self):
        """ Cached local version of the file.
        """
        return self._local_cache_dir / self._no_prefix

    # check if dirty
