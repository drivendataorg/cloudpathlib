class CloudPathException(Exception):
    """Base exception for all cloudpathlib custom exceptions."""


class AccessError(CloudPathException):
    pass


class ClientMismatch(CloudPathException, ValueError):
    pass


class CloudPathFileExistsError(CloudPathException, FileExistsError):
    pass


class CloudPathIsADirectoryError(CloudPathException, IsADirectoryError):
    pass


class CloudPathNotADirectoryError(CloudPathException, NotADirectoryError):
    pass


class DirectoryNotEmpty(CloudPathException):
    pass


class IncompleteImplementation(CloudPathException, NotImplementedError):
    pass


class InvalidPrefix(CloudPathException, ValueError):
    pass


class MissingCredentialsError(CloudPathException):
    pass


class MissingDependencies(CloudPathException, ModuleNotFoundError):
    pass


class NoStat(CloudPathException):
    """Used if stats cannot be retrieved; e.g., file does not exist
    or for some backends path is a directory (which doesn't have
    stats available).
    """


class OverwriteDirtyFile(CloudPathException):
    pass


class OverwriteNewerCloud(CloudPathException):
    pass


class OverwriteNewerLocal(CloudPathException):
    pass
