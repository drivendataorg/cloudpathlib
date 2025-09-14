from functools import lru_cache
import sys
from typing import TYPE_CHECKING

if sys.version_info <= (3, 14):
    from pathlib_abc import PathInfo
else:
    from pathlib.types import PathInfo

if TYPE_CHECKING:
    from .cloudpath import CloudPath


class CloudPathInfo(PathInfo):
    """Implementation of `PathInfo` protocol for `CloudPath`.

    Caches the results of the methods for efficient re-use.
    """

    def __init__(self, cloud_path: "CloudPath") -> None:
        self.cloud_path: "CloudPath" = cloud_path

    @lru_cache
    def exists(self, *, follow_symlinks: bool = True) -> bool:
        return self.cloud_path.exists()

    @lru_cache
    def is_dir(self, *, follow_symlinks: bool = True) -> bool:
        return self.cloud_path.is_dir(follow_symlinks=follow_symlinks)

    @lru_cache
    def is_file(self, *, follow_symlinks: bool = True) -> bool:
        return self.cloud_path.is_file(follow_symlinks=follow_symlinks)

    def is_symlink(self) -> bool:
        return False
