from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .azblobclient import AzureBlobClient as AzureBlobClient
    from .azblobpath import AzureBlobPath as AzureBlobPath

__all__ = [
    "AzureBlobClient",
    "AzureBlobPath",
]


def __getattr__(name: str):
    if name == "AzureBlobClient":
        from .azblobclient import AzureBlobClient

        return AzureBlobClient
    if name == "AzureBlobPath":
        from .azblobpath import AzureBlobPath

        return AzureBlobPath
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
