import abc


class Backend(abc.ABC):
    class Meta:
        path_class = None

    @abc.abstractmethod
    def download_file(self, cloud_path, local_path):
        pass

    @abc.abstractmethod
    def exists(self, cloud_path):
        pass

    @abc.abstractmethod
    def list_dir(self, cloud_path, recursive):
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
