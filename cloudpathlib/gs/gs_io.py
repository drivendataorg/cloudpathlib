"""Google Cloud Storage streaming I/O."""

from ..cloud_io import _CloudMultipartStorageRaw
from ..cloudpath import register_raw_io_class


@register_raw_io_class("gs")
class _GSStorageRaw(_CloudMultipartStorageRaw):
    """GCS range reads and XML multipart-upload writes."""

    # GCS XML multipart uploads require non-final parts of at least 5 MiB.
    _INITIAL_PART_SIZE = 5 * 1024 * 1024
    _MAX_PART_SIZE = 5 * 1024 * 1024 * 1024
    _MAX_PARTS = 10_000
    _PARTS_PER_SIZE_TIER = 1_000
    _PROVIDER_NAME = "GCS multipart"
