import sys

from .azure.azblobclient import AzureBlobClient
from .azure.azblobpath import AzureBlobPath
from .cloudpath import CloudPath
from .s3.s3client import S3Client
from .gs.gspath import GSPath
from .gs.gsclient import GSClient
from .s3.s3path import S3Path


if sys.version_info[:2] >= (3, 8):
    import importlib.metadata as importlib_metadata
else:
    import importlib_metadata


__version__ = importlib_metadata.version(__name__.split(".", 1)[0])


# exceptions
from .cloudpath import (
    ClientMismatch,
    DirectoryNotEmpty,
    InvalidPrefix,
    MissingDependencies,
    OverwriteDirtyFile,
    OverwriteNewerLocal,
)

__all__ = [
    "AzureBlobClient",
    "AzureBlobPath",
    "ClientMismatch",
    "CloudPath",
    "DirectoryNotEmpty",
    "InvalidPrefix",
    "GSClient",
    "GSPath",
    "MissingDependencies",
    "OverwriteDirtyFile",
    "OverwriteNewerLocal",
    "S3Client",
    "S3Path",
]
