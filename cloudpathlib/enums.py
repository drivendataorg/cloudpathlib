from enum import Enum
import warnings
import os
from typing import Optional


class FileCacheMode(str, Enum):
    """Enumeration of the modes available for for the cloudpathlib file cache.

    Attributes:
        persistent (str): Cache is not removed by `cloudpathlib`.
        tmp_dir (str): Cache is stored in a
            [`TemporaryDirectory`](https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory)
            which is removed when the Client object is garbage collected (or by the OS at some point if not).
        cloudpath_object (str): Cache for a `CloudPath` object is removed when `__del__` for that object is
            called by Python garbage collection.
        close_file (str): Cache for a `CloudPath` file is removed as soon as the file is closed. Note: you must
            use `CloudPath.open` whenever opening the file for this method to function.

    Modes can be set by passing them to the Client or by setting the `CLOUDPATHLIB_FILE_CACHE_MODE`
    environment variable.

    For more detail, see the [caching documentation page](../../caching).
    """

    persistent = "persistent"  # cache stays as long as dir on OS does
    tmp_dir = "tmp_dir"  # DEFAULT: handled by deleting client, Python, or OS (usually on machine restart)
    cloudpath_object = "cloudpath_object"  # __del__ called on the CloudPath object
    close_file = "close_file"  # cache is cleared when file is closed

    @classmethod
    def from_environment(cls) -> Optional["FileCacheMode"]:
        """Parses the environment variable `CLOUDPATHLIB_FILE_CACHE_MODE` into
        an instance of this Enum.

        Returns:
            FileCacheMode enum value if the env var is defined, else None.
        """

        env_string = os.environ.get("CLOUDPATHLIB_FILE_CACHE_MODE", "").lower()
        env_string_typo = os.environ.get("CLOUPATHLIB_FILE_CACHE_MODE", "").lower()

        if env_string_typo:
            warnings.warn(
                "envvar CLOUPATHLIB_FILE_CACHE_MODE has been renamed to "
                "CLOUDPATHLIB_FILE_CACHE_MODE. Reading from the old value "
                "will become deprecated in version 0.20.0",
                DeprecationWarning,
            )

        if env_string and env_string_typo and env_string != env_string_typo:
            warnings.warn(
                "CLOUDPATHLIB_FILE_CACHE_MODE and CLOUPATHLIB_FILE_CACHE_MODE "
                "envvars set to different values. Disregarding old value and "
                f"using CLOUDPATHLIB_FILE_CACHE_MODE = {env_string}",
                RuntimeWarning,
            )

        env_string = env_string or env_string_typo
        if not env_string:
            return None
        else:
            return cls(env_string)
