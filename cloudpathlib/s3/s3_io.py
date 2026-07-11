"""S3 streaming I/O."""

from ..cloud_io import _CloudMultipartStorageRaw
from ..cloudpath import register_raw_io_class


@register_raw_io_class("s3")
class _S3StorageRaw(_CloudMultipartStorageRaw):
    """S3 range reads and multipart writes."""

    # S3 requires non-final parts of at least 5 MiB.
    _INITIAL_PART_SIZE = 5 * 1024 * 1024
    _MIN_PART_SIZE = _INITIAL_PART_SIZE
    _MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
    _MAX_PARTS = 10_000
    _PARTS_PER_SIZE_TIER = 1_000
    _PROVIDER_NAME = "S3 multipart"
