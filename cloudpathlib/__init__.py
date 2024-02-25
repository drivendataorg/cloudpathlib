import os
import sys

from .anypath import AnyPath
from .azure.azblobclient import AzureBlobClient
from .azure.azblobpath import AzureBlobPath
from .cloudpath import CloudPath, implementation_registry
from .patches import patch_open, patch_os_functions, patch_glob
from .s3.s3client import S3Client
from .gs.gspath import GSPath
from .gs.gsclient import GSClient
from .s3.s3path import S3Path


if sys.version_info[:2] >= (3, 8):
    import importlib.metadata as importlib_metadata
else:
    import importlib_metadata


__version__ = importlib_metadata.version(__name__.split(".", 1)[0])


__all__ = [
    "AnyPath",
    "AzureBlobClient",
    "AzureBlobPath",
    "CloudPath",
    "implementation_registry",
    "GSClient",
    "GSPath",
    "patch_glob",
    "patch_open",
    "patch_os_functions",
    "S3Client",
    "S3Path",
]


if bool(os.environ.get("CLOUDPATHLIB_PATCH_OPEN", "")):
    patch_open()

if bool(os.environ.get("CLOUDPATHLIB_PATCH_OS", "")):
    patch_os_functions()

if bool(os.environ.get("CLOUDPATHLIB_PATCH_GLOB", "")):
    patch_glob()

if bool(os.environ.get("CLOUDPATHLIB_PATCH_ALL", "")):
    patch_open()
    patch_os_functions()
    patch_glob
