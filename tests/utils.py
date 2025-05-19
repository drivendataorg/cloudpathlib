import platform
import os
import time


def _sync_filesystem():
    """Try to force sync of the filesystem to stabilize tests.

    On Windows, give the filesystem a moment to catch up since sync is not available.
    """
    if platform.system() != "Windows":
        os.sync()
    else:
        # On Windows, give the filesystem a moment to catch up
        time.sleep(0.05)
