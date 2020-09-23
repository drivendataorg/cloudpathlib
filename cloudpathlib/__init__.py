from .cloudpath import CloudPath

# exceptions
from .cloudpath import (
    ClientMismatch,
    DirectoryNotEmpty,
    InvalidPrefix,
    MissingDependencies,
    NoImplementations,
    OverwriteDirtyFile,
    OverwriteNewerLocal,
)

__all__ = [
    "ClientMismatch",
    "CloudPath",
    "DirectoryNotEmpty",
    "InvalidPrefix",
    "MissingDependencies",
    "NoImplementations",
    "OverwriteDirtyFile",
    "OverwriteNewerLocal",
]

try:
    from .azure.azblobclient import AzureBlobClient
    from .azure.azblobpath import AzureBlobPath

    __all__.extend(obj.__name__ for obj in [AzureBlobClient, AzureBlobPath])
except MissingDependencies:
    pass

try:
    from .s3.s3client import S3Client
    from .s3.s3path import S3Path

    __all__.extend(obj.__name__ for obj in [S3Client, S3Path])
except MissingDependencies:
    pass
