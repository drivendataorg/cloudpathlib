import sys
from warnings import warn

from .anypath import AnyPath
from .azure.azblobclient import AzureBlobClient
from .azure.azblobpath import AzureBlobPath
from .cloudpath import CloudPath, implementation_registry
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
    "S3Client",
    "S3Path",
]


class PydanticVersionWarning(UserWarning):
    message = (
        "This version of cloudpathlib is only compatible with pydantic<2.0.0. "
        "You can ignore this warning if none of your pydantic models are "
        "annotated with cloudpathlib types."
    )


try:
    import pydantic
    from packaging.version import parse

    if parse(pydantic.__version__) >= parse("2.0.0"):
        warn(PydanticVersionWarning(PydanticVersionWarning.message))

except ImportError:
    pass
