from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .s3client import S3Client as S3Client
    from .s3path import S3Path as S3Path

__all__ = [
    "S3Client",
    "S3Path",
]


def __getattr__(name: str):
    if name == "S3Client":
        from .s3client import S3Client

        return S3Client
    if name == "S3Path":
        from .s3path import S3Path

        return S3Path
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
