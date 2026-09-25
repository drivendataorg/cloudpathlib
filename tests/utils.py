import platform
import os
from pathlib import Path
import time
from typing import Any, Callable


def _sync_filesystem():
    """Try to force sync of the filesystem to stabilize tests.

    On Windows, give the filesystem a moment to catch up since sync is not available.
    """
    if platform.system() != "Windows":
        os.sync()
    else:
        # On Windows, give the filesystem a moment to catch up
        time.sleep(0.05)


def _mtime_of(reference: Any) -> float:
    if isinstance(reference, (int, float)):
        return float(reference)
    return reference.stat().st_mtime


def make_local_older(local_path: Path, than: Any, delta: float = 2.0) -> None:
    """Set a local file's mtime before a cloud or local reference, including coarse timestamps."""
    mtime = _mtime_of(than) - delta
    os.utime(local_path, times=(mtime, mtime))


def make_local_newer(local_path: Path, than: Any, delta: float = 2.0) -> None:
    """Set an upload source's mtime after a cloud or local reference."""
    mtime = _mtime_of(than) + delta
    os.utime(local_path, times=(mtime, mtime))


def rewrite_until_newer(
    cloud_path,
    baseline: float,
    action: Callable[[], Any],
    timeout: float = 5.0,
) -> None:
    """Repeat an operation until the cloud reports a newer mtime, or fail after a timeout."""
    start = time.monotonic()
    while True:
        action()
        if cloud_path.stat().st_mtime > baseline:
            return
        if time.monotonic() - start >= timeout:
            raise AssertionError(
                f"Modified time of {cloud_path} did not advance within {timeout}s"
            )
        time.sleep(0.05)
