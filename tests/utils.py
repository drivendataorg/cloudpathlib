import gc
import os
from pathlib import Path
import platform
import time
from typing import Any, Callable, Iterable, List, Sequence, Tuple
import weakref


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


def wait_until(
    predicate: Callable[[], bool],
    timeout: float = 5.0,
    interval: float = 0.05,
    message: str = "",
) -> None:
    """Poll `predicate` until it is true, and raise if it is still false after `timeout` seconds.

    Use this instead of `sleep` whenever a test needs to wait for something that we can observe
    directly; it is both faster in the common case and bounded in the pathological one.
    """
    start = time.monotonic()

    while not predicate():
        if time.monotonic() - start >= timeout:
            raise AssertionError(message or f"Condition not met within {timeout} seconds")
        time.sleep(interval)


def weakrefs_to(*objs: Any) -> List[Tuple[str, "weakref.ref"]]:
    """Labelled weak references to objects a test is about to `del` and then assert cleanup for.

    Pair with `assert_collected_and_cleaned`; the labels are only used in failure messages.
    """
    return [(f"{type(o).__name__} at 0x{id(o):x}", weakref.ref(o)) for o in objs]


def _describe_referrers(obj: Any) -> str:
    referrers = [r for r in gc.get_referrers(obj) if r is not None]
    descriptions = []

    for referrer in referrers:
        # frames of this module are ours, not the leak we are looking for
        if getattr(referrer, "f_globals", {}).get("__name__") == __name__:
            continue
        descriptions.append(f"{type(referrer).__name__}: {repr(referrer)[:120]}")

    return "; ".join(descriptions) or "no referrers found (likely part of a reference cycle)"


def assert_collected_and_cleaned(
    refs: Sequence[Tuple[str, "weakref.ref"]],
    gone: Iterable[Path] = (),
    timeout: float = 5.0,
) -> None:
    """Assert that weakly referenced objects have been collected, then that cleanup happened.

    Cleanup in cloudpathlib is driven by `__del__`, so an assertion about cache files disappearing
    is really two assertions. Checking them separately means a failure names its own cause: a live
    weak reference is a reference leak in the test or the library (retrying will not help), while a
    dead weak reference with lingering files is filesystem lag (bounded wait) or a genuine cleanup
    bug.
    """
    gc.collect()  # collect cycles, which refcounting alone will not reclaim

    alive = [(label, ref()) for label, ref in refs if ref() is not None]
    if alive:
        details = "\n".join(
            f"  {label} referenced by {_describe_referrers(obj)}" for label, obj in alive
        )
        raise AssertionError(
            "Objects were not collected after gc.collect(), so their `__del__` cleanup could not "
            f"have run yet. Find the reference that is keeping them alive:\n{details}"
        )

    for path in gone:
        wait_until(
            lambda p=path: not p.exists(),
            timeout=timeout,
            message=f"{path} still exists after the object responsible for cleaning it up was "
            "collected; either the filesystem is lagging or cleanup is broken",
        )
