import abc
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterable, Optional, Union

from ..cloudpath import CloudImplementation


class Backend(abc.ABC):
    cloud_meta: CloudImplementation
    default_backend = None

    def __init__(self, local_cache_dir: Optional[Union[str, os.PathLike]] = None):
        # setup caching and local versions of file and track if it is a tmp dir
        self._cache_tmp_dir = None
        if local_cache_dir is None:
            self._cache_tmp_dir = TemporaryDirectory()
            local_cache_dir = self._cache_tmp_dir.name

        self._local_cache_dir = Path(local_cache_dir)

    def __del__(self):
        # make sure temp is cleaned up if we created it
        if self._cache_tmp_dir is not None:
            self._cache_tmp_dir.cleanup()

    @classmethod
    def get_default_backend(cls):
        if cls.default_backend is None:
            cls.default_backend = cls()
        return cls.default_backend

    def CloudPath(self, cloud_path):
        return self.cloud_meta.path_class(cloud_path=cloud_path, backend=self)

    @abc.abstractmethod
    def download_file(self, cloud_path, local_path):
        pass

    @abc.abstractmethod
    def exists(self, cloud_path):
        pass

    @abc.abstractmethod
    def list_dir(self, cloud_path, recursive: bool) -> Iterable[str]:
        """ List all the files and folders in a directory.

        Parameters
        ----------
        cloud_path : CloudPath
            The folder to start from.
        recursive : bool
            Whether or not to list recursively.
        """
        pass

    @abc.abstractmethod
    def move_file(self, src, dst):
        pass

    @abc.abstractmethod
    def remove(self, path):
        """Remove a file or folder from the server.

        Parameters
        ----------
        path : CloudPath
            The file or folder to remove.
        """
        pass

    @abc.abstractmethod
    def upload_file(self, local_path, cloud_path):
        pass
