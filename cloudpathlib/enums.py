from enum import Enum
import os


class FileCacheMode(Enum):
    persistent = "persistent"  # cache stays as long as dir on OS does
    tmp_dir = "tmp_dir"  # DEFAULT: handled by deleting client, Python, or OS (usually on machine restart)
    cloudpath_object = "cloudpath_object"  # __del__ called on the CloudPath object
    close_file = "close_file"  # cache is cleared when file is closed

    @classmethod
    def from_environment(cls):
        env_string = os.environ.get("CLOUPATHLIB_FILE_CACHE_MODE", "").lower()

        if not env_string:
            return None
        else:
            return cls(env_string)
