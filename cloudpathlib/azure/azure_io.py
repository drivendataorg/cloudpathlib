"""Azure Blob Storage streaming I/O."""

from ..cloud_io import _CloudMultipartStorageRaw
from ..cloudpath import register_raw_io_class


@register_raw_io_class("azure")
class _AzureBlobStorageRaw(_CloudMultipartStorageRaw):
    """Azure range reads and block writes."""

    # Azure permits at most 50,000 committed blocks.
    _INITIAL_PART_SIZE = 4 * 1024 * 1024
    _BLOCK_SIZE = _INITIAL_PART_SIZE
    _MAX_PART_SIZE = 4_000 * 1024 * 1024
    _MAX_BLOCK_SIZE = _MAX_PART_SIZE
    _MAX_PARTS = 50_000
    _BLOCKS_PER_SIZE_TIER = 1_000
    _PARTS_PER_SIZE_TIER = _BLOCKS_PER_SIZE_TIER
    _PROVIDER_NAME = "Azure block"

    @classmethod
    def _block_size_for_number(cls, block_number: int) -> int:
        return cls._part_size_for_number(block_number)
