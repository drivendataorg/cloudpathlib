class CloudPathException(Exception):
    """Base exception for all cloudpathlib custom exceptions."""


class ClientMismatchError(CloudPathException, ValueError):
    pass


class CloudPathFileExistsError(CloudPathException, FileExistsError):
    pass


class CloudPathIsADirectoryError(CloudPathException, IsADirectoryError):
    pass


class CloudPathNotADirectoryError(CloudPathException, NotADirectoryError):
    pass


class DirectoryNotEmptyError(CloudPathException):
    pass


class IncompleteImplementationError(CloudPathException, NotImplementedError):
    pass


class InvalidPrefixError(CloudPathException, ValueError):
    pass


class MissingCredentialsError(CloudPathException):
    pass


class MissingDependenciesError(CloudPathException, ModuleNotFoundError):
    pass


class NoStatError(CloudPathException):
    """Used if stats cannot be retrieved; e.g., file does not exist
    or for some backends path is a directory (which doesn't have
    stats available).
    """


class OverwriteDirtyFileError(CloudPathException):
    pass


class OverwriteNewerCloudError(CloudPathException):
    pass


class OverwriteNewerLocalError(CloudPathException):
    pass
