import os
import sys
from typing import TYPE_CHECKING

# Lazy imports for cloud providers to avoid loading heavy SDKs at import time
# Google Cloud SDK alone adds ~200ms to import time

if TYPE_CHECKING:
    from .anypath import AnyPath as AnyPath
    from .azure.azblobclient import AzureBlobClient as AzureBlobClient
    from .azure.azblobpath import AzureBlobPath as AzureBlobPath
    from .cloudpath import (
        CloudPath as CloudPath,
        implementation_registry as implementation_registry,
    )
    from .patches import (
        patch_open as patch_open,
        patch_os_functions as patch_os_functions,
        patch_glob as patch_glob,
        patch_all_builtins as patch_all_builtins,
    )
    from .gs.gsclient import GSClient as GSClient
    from .gs.gspath import GSPath as GSPath
    from .http.httpclient import HttpClient as HttpClient, HttpsClient as HttpsClient
    from .http.httppath import HttpPath as HttpPath, HttpsPath as HttpsPath
    from .s3.s3client import S3Client as S3Client
    from .s3.s3path import S3Path as S3Path

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
    "HttpClient",
    "HttpsClient",
    "HttpPath",
    "HttpsPath",
    "patch_open",
    "patch_glob",
    "patch_os_functions",
    "patch_all_builtins",
    "S3Client",
    "S3Path",
]


# Lazy loading implementation
_LAZY_IMPORTS = {
    # Core
    "AnyPath": ".anypath",
    "CloudPath": ".cloudpath",
    "implementation_registry": ".cloudpath",
    # Patches
    "patch_open": ".patches",
    "patch_os_functions": ".patches",
    "patch_glob": ".patches",
    "patch_all_builtins": ".patches",
    # S3
    "S3Client": ".s3.s3client",
    "S3Path": ".s3.s3path",
    # GCS
    "GSClient": ".gs.gsclient",
    "GSPath": ".gs.gspath",
    # Azure
    "AzureBlobClient": ".azure.azblobclient",
    "AzureBlobPath": ".azure.azblobpath",
    # HTTP
    "HttpClient": ".http.httpclient",
    "HttpsClient": ".http.httpclient",
    "HttpPath": ".http.httppath",
    "HttpsPath": ".http.httppath",
}


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        import importlib

        module_path = _LAZY_IMPORTS[name]
        module = importlib.import_module(module_path, __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return __all__


# Handle environment-variable-based patching
# These need to be checked at import time, so we do them lazily only if env vars are set
if bool(os.environ.get("CLOUDPATHLIB_PATCH_OPEN", "")):
    from .patches import patch_open as _patch_open

    _patch_open()

if bool(os.environ.get("CLOUDPATHLIB_PATCH_OS", "")):
    from .patches import patch_os_functions as _patch_os_functions

    _patch_os_functions()

if bool(os.environ.get("CLOUDPATHLIB_PATCH_GLOB", "")):
    from .patches import patch_glob as _patch_glob

    _patch_glob()

if bool(os.environ.get("CLOUDPATHLIB_PATCH_ALL", "")):
    from .patches import patch_all_builtins as _patch_all_builtins

    _patch_all_builtins()
