from datetime import datetime
import os
from pathlib import Path, PurePosixPath
from shutil import rmtree
import sys
from time import sleep

import pytest
from cloudpathlib import CloudPath

from cloudpathlib.cloudpath import _ensure_local_path_within_base
from cloudpathlib.exceptions import (
    CloudPathLocalPathTraversalError,
    CloudPathNotExistsError,
    CloudPathIsADirectoryError,
    CloudPathNotImplementedError,
    DirectoryNotEmptyError,
    NoStatError,
)
from cloudpathlib.enums import FileCacheMode
from cloudpathlib.local import LocalS3Client, LocalS3Path
from cloudpathlib.http.httpclient import HttpClient, HttpsClient
from cloudpathlib.http.httppath import HttpPath, HttpsPath


def test_file_discovery(rig):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    assert p.exists()

    p2 = rig.create_cloud_path("dir_0/not_a_file_yet.file")
    assert not p2.exists()
    p2.touch()
    assert p2.exists()

    if rig.client_class not in [HttpClient, HttpsClient]:  # not supported to touch existing
        p2.touch(exist_ok=True)
    else:
        with pytest.raises(NotImplementedError):
            p2.touch(exist_ok=True)

    with pytest.raises(FileExistsError):
        p2.touch(exist_ok=False)
    p2.unlink(missing_ok=False)
    p2.unlink(missing_ok=True)
    with pytest.raises(FileNotFoundError):
        p2.unlink(missing_ok=False)

    p3 = rig.create_cloud_path("dir_0/")
    assert p3.exists()
    assert len(list(p3.iterdir())) == 3
    assert len(list(p3.glob("**/*"))) == 3

    with pytest.raises(CloudPathIsADirectoryError):
        p3.unlink()

    if not getattr(rig, "is_adls_gen2", False):
        with pytest.raises(CloudPathIsADirectoryError):
            p3.rename(rig.create_cloud_path("dir_2/"))

    with pytest.raises(DirectoryNotEmptyError):
        p3.rmdir()
    p3.rmtree()
    assert not p3.exists()

    p4 = rig.create_cloud_path("")
    assert p4.exists()

    assert len(list(p4.iterdir())) == 1  # only bucket/dir_1/ should still exist
    assert len(list(p4.glob("**/*"))) == 4

    assert list(p4.glob("**/*")) == list(p4.rglob("*"))

    # try rmtree on dir_1, which has a nested directory
    p5 = rig.create_cloud_path("dir_1/")
    assert p5.exists()
    assert len(list(p5.glob("**/*"))) == 3
    with pytest.raises(CloudPathIsADirectoryError):
        p5.unlink()

    with pytest.raises(DirectoryNotEmptyError):
        p5.rmdir()

    p5.rmtree()
    assert not p5.exists()


@pytest.fixture
def glob_test_dirs(rig, tmp_path):
    """Sets up a local directory and a cloud directory that matches the
    tests used for glob in the CPython codebase. So we can make sure
    our implementation matches CPython glob.

    Adapted from this test setup ()
    https://github.com/python/cpython/blob/7ffe7ba30fc051014977c6f393c51e57e71a6648/Lib/test/test_pathlib.py#L1378-L1395
    """

    def _make_glob_directory(root):
        (root / "dirB").mkdir()
        (root / "dirB" / "fileB.txt").write_text("fileB")
        (root / "dirC").mkdir()
        (root / "dirC" / "dirD").mkdir()
        (root / "dirC" / "dirD" / "fileD.txt").write_text("fileD")
        (root / "dirC" / "fileC.txt").write_text("fileC")
        (root / "fileA.txt").write_text("fileA")

    cloud_root = rig.create_cloud_path("glob-tests/")
    cloud_root.mkdir()

    _make_glob_directory(cloud_root)

    local_root = tmp_path / "glob-tests/"
    local_root.mkdir()

    _make_glob_directory(local_root)

    yield cloud_root, local_root

    cloud_root.rmtree()
    rmtree(local_root)


def _lstrip_path_root(path, root):
    rel_path = str(path)[len(str(root)) :]
    return rel_path.strip("/")


def _assert_glob_results_match(cloud_results, local_results, cloud_root, local_root):
    local_results_no_root = [_lstrip_path_root(c.as_posix(), local_root) for c in local_results]
    cloud_results_no_root = [_lstrip_path_root(c, cloud_root) for c in cloud_results]

    # same number of items listed
    assert len(cloud_results_no_root) == len(local_results_no_root)

    # check that contents are the same regardless of order
    assert set(local_results_no_root) == set(cloud_results_no_root)


def _assert_walk_results_match(cloud_results, local_results, cloud_root, local_root):
    # order not guaranteed, so strip use top as keys for matching
    cloud_results = {
        _lstrip_path_root(top, cloud_root): [dirs, files] for top, dirs, files in cloud_results
    }
    local_results = {
        _lstrip_path_root(Path(top).as_posix(), local_root): [dirs, files]
        for top, dirs, files in local_results
    }

    assert set(cloud_results.keys()) == set(local_results.keys())

    for top in local_results:
        local_dirs, local_files = local_results[top]
        cloud_dirs, cloud_files = cloud_results[top]

        assert set(cloud_dirs) == set(local_dirs)  # order not guaranteed
        assert set(local_files) == set(cloud_files)  # order not guaranteed


def test_iterdir(glob_test_dirs):
    cloud_root, local_root = glob_test_dirs

    # iterdir tests
    _assert_glob_results_match(cloud_root.iterdir(), local_root.iterdir(), cloud_root, local_root)
    _assert_glob_results_match(
        (cloud_root / "dirC").iterdir(), (local_root / "dirC").iterdir(), cloud_root, local_root
    )
    _assert_glob_results_match(
        (cloud_root / "dirB").iterdir(), (local_root / "dirB").iterdir(), cloud_root, local_root
    )
    _assert_glob_results_match(
        (cloud_root / "dirC" / "dirD").iterdir(),
        (local_root / "dirC" / "dirD").iterdir(),
        cloud_root,
        local_root,
    )


def test_walk(glob_test_dirs):
    cloud_root, local_root = glob_test_dirs

    # walk only natively available in python 3.12+
    local_results = local_root.walk() if hasattr(local_root, "walk") else os.walk(local_root)

    _assert_walk_results_match(cloud_root.walk(), local_results, cloud_root, local_root)

    local_results = (
        local_root.walk(top_down=False)
        if hasattr(local_root, "walk")
        else os.walk(local_root, topdown=False)
    )

    _assert_walk_results_match(
        cloud_root.walk(top_down=False), local_results, cloud_root, local_root
    )


def _walk_collect(root, **kwargs):
    """Collect walk results keyed by root-relative path, with dirs/files sorted
    since cloud listing order is not guaranteed."""
    results = {}
    for top, dirs, files in root.walk(**kwargs):
        results[_lstrip_path_root(top, root)] = (sorted(dirs), sorted(files))
    return results


@pytest.mark.parametrize("top_down", [True, False])
def test_walk_lazy_matches_eager(glob_test_dirs, top_down):
    """Without pruning, lazy=True yields the same results as the default eager
    walk (and as os.walk) for both traversal orders."""
    cloud_root, local_root = glob_test_dirs

    eager = _walk_collect(cloud_root, top_down=top_down, lazy=False)
    lazy = _walk_collect(cloud_root, top_down=top_down, lazy=True)
    assert eager == lazy

    local = {}
    for top, dirs, files in os.walk(local_root, topdown=top_down):
        local[_lstrip_path_root(Path(top).as_posix(), local_root)] = (
            sorted(dirs),
            sorted(files),
        )
    assert lazy == local


def test_walk_lazy_pruning_skips_subtree(glob_test_dirs):
    """With lazy=True and top_down, pruning dirnames in place skips the pruned
    subtree entirely -- it is neither yielded nor listed (issue #518).

    Directory structure used by glob_test_dirs:
        glob-tests/
          dirB/fileB.txt
          dirC/dirD/fileD.txt
          dirC/fileC.txt
          fileA.txt
    """
    from unittest.mock import patch

    cloud_root, local_root = glob_test_dirs

    listed = []
    original_list_dir = cloud_root.client._list_dir

    def recording_list_dir(path, recursive=False):
        listed.append(_lstrip_path_root(path, cloud_root))
        yield from original_list_dir(path, recursive=recursive)

    with patch.object(cloud_root.client, "_list_dir", recording_list_dir):
        cloud_results = {}
        for top, dirs, files in cloud_root.walk(lazy=True):
            cloud_results[_lstrip_path_root(top, cloud_root)] = (list(dirs), list(files))
            dirs[:] = [d for d in dirs if d != "dirC"]

    # Equivalent pruning with os.walk for the expected yielded structure
    local_results = {}
    for top, dirs, files in os.walk(local_root):
        local_results[_lstrip_path_root(Path(top).as_posix(), local_root)] = (
            list(dirs),
            list(files),
        )
        dirs[:] = [d for d in dirs if d != "dirC"]

    assert set(cloud_results.keys()) == set(local_results.keys())

    # dirC and its descendants are never yielded and never listed (no API calls)
    assert not any("dirC" in k for k in cloud_results)
    assert not any("dirC" in p for p in listed)

    # non-pruned dirB is still visited
    assert "dirB" in cloud_results


def test_walk_eager_pruning_does_not_save_listings(glob_test_dirs):
    """The default (eager) walk fetches the whole subtree up front with a
    recursive listing; pruning dirnames only changes what is yielded, not the
    listings performed."""
    from unittest.mock import patch

    cloud_root, _ = glob_test_dirs
    original_list_dir = cloud_root.client._list_dir

    def make_recorder(store):
        def recording_list_dir(path, recursive=False):
            store.append((_lstrip_path_root(path, cloud_root), recursive))
            yield from original_list_dir(path, recursive=recursive)

        return recording_list_dir

    # eager walk, pruning dirC
    pruned_calls = []
    with patch.object(cloud_root.client, "_list_dir", make_recorder(pruned_calls)):
        yielded = []
        for top, dirs, files in cloud_root.walk():  # default: lazy=False
            yielded.append(_lstrip_path_root(top, cloud_root))
            dirs[:] = [d for d in dirs if d != "dirC"]

    # eager walk, no pruning
    full_calls = []
    with patch.object(cloud_root.client, "_list_dir", make_recorder(full_calls)):
        for _ in cloud_root.walk():
            pass

    # Pruning removes dirC from the yielded output...
    assert not any("dirC" in k for k in yielded)
    # ...but the eager walk performed exactly the same listings either way
    # (i.e. pruning saved no API calls), using a recursive listing.
    assert pruned_calls == full_calls
    assert any(recursive for _, recursive in full_calls)


def test_walk_lazy_skips_self(glob_test_dirs):
    """The lazy walk does not include the directory being listed in its own
    dirnames, even when a backend's _list_dir yields the directory itself."""
    from unittest.mock import patch

    cloud_root, _ = glob_test_dirs

    original_list_dir = cloud_root.client._list_dir

    def list_dir_including_self(path, recursive=False):
        # Prepend the directory itself to mimic backends that include it.
        yield path, True
        yield from original_list_dir(path, recursive=recursive)

    with patch.object(cloud_root.client, "_list_dir", list_dir_including_self):
        top, dirs, files = next(iter(cloud_root.walk(lazy=True)))
        assert top == cloud_root
        assert cloud_root.name not in dirs


@pytest.mark.parametrize("lazy", [False, True])
def test_walk_on_error(glob_test_dirs, lazy):
    """on_error is invoked when a listing raises (both eager and lazy walks),
    and the error propagates when on_error is not provided."""
    from unittest.mock import patch

    cloud_root, _ = glob_test_dirs

    original_list_dir = cloud_root.client._list_dir
    expected_error = ValueError("simulated listing error")

    def failing_list_dir(path, recursive=False):
        if path == cloud_root:
            raise expected_error
        return original_list_dir(path, recursive=recursive)

    with patch.object(cloud_root.client, "_list_dir", failing_list_dir):
        # on_error should capture the error; walk should produce no results
        errors = []
        results = list(cloud_root.walk(on_error=errors.append, lazy=lazy))
        assert results == []
        assert errors == [expected_error]

        # Without on_error, the error should propagate
        with pytest.raises(ValueError, match="simulated listing error"):
            list(cloud_root.walk(lazy=lazy))


def test_list_buckets(rig):
    if rig.path_class in [HttpPath, HttpsPath]:
        return  # no bucket listing for HTTP

    # test we can list buckets
    buckets = list(rig.path_class(f"{rig.path_class.cloud_prefix}").iterdir())
    assert len(buckets) > 0

    for b in buckets:
        assert isinstance(b, rig.path_class)
        assert b.drive != ""
        assert b._no_prefix_no_drive == ""


def test_glob(glob_test_dirs):
    cloud_root, local_root = glob_test_dirs

    # cases adapted from CPython glob tests:
    #  https://github.com/python/cpython/blob/7ffe7ba30fc051014977c6f393c51e57e71a6648/Lib/test/test_pathlib.py#L1634-L1720

    def _check_glob(pattern, glob_method, **kwargs):
        local_pattern = kwargs.pop("local_pattern", None)

        _assert_glob_results_match(
            getattr(cloud_root, glob_method)(pattern, **kwargs),
            getattr(local_root, glob_method)(
                pattern if local_pattern is None else local_pattern, **kwargs
            ),
            cloud_root,
            local_root,
        )

    # glob_common
    _check_glob("**/*", "glob")
    _check_glob("*", "glob")
    _check_glob("fileA", "glob")
    _check_glob("fileB", "glob")
    _check_glob("dir*/file*", "glob")
    _check_glob("*A", "glob")
    _check_glob("*B/*", "glob")
    _check_glob("*/fileB", "glob")
    _check_glob(cloud_root / "**/*", "glob", local_pattern="**/*")

    if sys.version_info >= (3, 13):
        _check_glob(PurePosixPath("**/*"), "glob")

    # rglob_common
    _check_glob("*", "rglob")
    _check_glob("fileA", "rglob")
    _check_glob("fileB", "rglob")
    _check_glob("*/fileA", "rglob")
    _check_glob("*/fileB", "rglob")
    _check_glob("file*", "rglob")
    _check_glob(cloud_root / "*", "rglob", local_pattern="*")

    if sys.version_info >= (3, 13):
        _check_glob(PurePosixPath("*"), "rglob")

    dir_c_cloud = cloud_root / "dirC"
    dir_c_local = local_root / "dirC"
    _assert_glob_results_match(
        dir_c_cloud.rglob("file*"), dir_c_local.rglob("file*"), dir_c_cloud, dir_c_local
    )
    _assert_glob_results_match(
        dir_c_cloud.rglob("*/*"), dir_c_local.rglob("*/*"), dir_c_cloud, dir_c_local
    )

    # 3.12+ kwargs
    if sys.version_info >= (3, 12):
        _check_glob("dir*/FILE*", "glob", case_sensitive=False)
        _check_glob("dir*/file*", "glob", case_sensitive=True)
        _check_glob("dir*/FILE*", "rglob", case_sensitive=False)
        _check_glob("dir*/file*", "rglob", case_sensitive=True)

        # test case insensitive for cloud; sensitive different pattern for local
        _assert_glob_results_match(
            dir_c_cloud.glob("FILE*", case_sensitive=False),
            dir_c_local.glob("file*"),
            dir_c_cloud,
            dir_c_local,
        )


def test_glob_buckets(rig):
    # CloudPath("s3://").glob("*") results in error
    drive_level = rig.path_class(rig.path_class.cloud_prefix)

    with pytest.raises(CloudPathNotImplementedError):
        list(drive_level.glob("*"))

    # CloudPath("s3://bucket").glob("*") should work
    # bucket level glob returns correct results
    # regression test for #311
    bucket = rig.path_class(f"{rig.path_class.cloud_prefix}{rig.drive}")

    first_result = next(bucket.glob("*"))

    # assert all parts are unique
    assert first_result.drive == rig.drive
    assert len(first_result.parts) == len(set(first_result.parts))


def test_glob_many_open_files(rig):
    # test_glob_many_open_files
    #  Adapted from: https://github.com/python/cpython/blob/7ffe7ba30fc051014977c6f393c51e57e71a6648/Lib/test/test_pathlib.py#L1697-L1712
    depth = 30
    base = rig.create_cloud_path("deep")
    p = base / "/".join(["d"] * depth)
    (p / "file.txt").write_text("hello")  # create file so parent dirs exist
    pattern = "/".join(["*"] * depth)
    iters = [base.glob(pattern) for j in range(100)]
    for it in iters:
        print(it)
        assert next(it) == p
    iters = [base.rglob("d") for j in range(100)]
    p = base
    for i in range(depth):
        p = p / "d"
        for it in iters:
            assert next(it) == p


def test_glob_exceptions(rig):
    cp = rig.create_cloud_path("dir_0/")

    # relative path with ..
    with pytest.raises(CloudPathNotImplementedError, match="Relative paths with"):
        list(cp.glob("../hello"))

    with pytest.raises(CloudPathNotImplementedError, match="Relative paths with"):
        list(cp.rglob("../hello"))

    # non-relative paths
    with pytest.raises(CloudPathNotImplementedError, match="Non-relative patterns"):
        list(cp.glob(f"{rig.path_class.cloud_prefix}{rig.drive}/path/**/*.jpg"))

    with pytest.raises(CloudPathNotImplementedError, match="Non-relative patterns"):
        list(cp.glob("/path/**/*.jpg"))

    with pytest.raises(CloudPathNotImplementedError, match="Non-relative patterns"):
        list(cp.rglob(f"{rig.path_class.cloud_prefix}{rig.drive}/path/**/*.jpg"))

    with pytest.raises(CloudPathNotImplementedError, match="Non-relative patterns"):
        list(cp.rglob("/path/**/*.jpg"))


def test_is_dir_is_file(rig, tmp_path):
    # test on directories
    dir_slash = rig.create_cloud_path("dir_0/")
    dir_no_slash = rig.create_cloud_path("dir_0")
    dir_nested_slash = rig.create_cloud_path("dir_1/dir_1_0/")
    dir_nested_no_slash = rig.create_cloud_path("dir_1/dir_1_0")

    for test_case in [dir_slash, dir_no_slash, dir_nested_slash, dir_nested_no_slash]:
        # skip no-slash cases, which are interpreted as files for http paths
        if not str(test_case).endswith("/") and rig.path_class in [HttpPath, HttpsPath]:
            continue

        assert test_case.is_dir()
        assert not test_case.is_file()

    file = rig.create_cloud_path("dir_0/file0_0.txt")
    file_nested = rig.create_cloud_path("dir_1/dir_1_0/file_1_0_0.txt")

    for test_case in [file, file_nested]:
        assert test_case.is_file()
        assert not test_case.is_dir()

    # does not exist (same behavior as pathlib.Path that does not exist)
    non_existent = rig.create_cloud_path("dir_0/not_a_file")
    assert not non_existent.is_file()
    assert not non_existent.is_dir()


def test_stat_nonexistent(rig):
    # stat on a path that does not exist raises NoStatError on every backend
    non_existent = rig.create_cloud_path("dir_0/not_a_real_file.txt")
    assert not non_existent.exists()

    with pytest.raises(NoStatError):
        non_existent.stat()


def test_file_read_writes(rig, tmp_path):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    p2 = rig.create_cloud_path("dir_0/not_a_file.txt")
    p3 = rig.create_cloud_path("")

    text = "lalala" * 10_000

    p.write_text(text)
    assert p.read_text() == text
    p2.write_text(text)

    # sleep between writes to p to ensure different
    # modified times
    sleep(1)

    p.write_bytes(p2.read_bytes())
    assert p.read_text() == p2.read_text()

    before_touch = datetime.now()
    sleep(1)

    if rig.path_class not in [HttpPath, HttpsPath]:  # not supported to touch existing
        p.touch()

        if not getattr(rig, "is_custom_s3", False):
            # Our S3Path.touch implementation does not update mod time for MinIO
            assert datetime.fromtimestamp(p.stat().st_mtime) > before_touch

    # no-op
    if not getattr(rig, "is_adls_gen2", False):
        p.mkdir()

    if rig.path_class not in [HttpPath, HttpsPath]:  # not supported to touch existing
        assert p.etag is not None

    dest = rig.create_cloud_path("dir2/new_file0_0.txt")
    assert not dest.exists()
    p.rename(dest)
    assert dest.exists()
    assert not p.exists()

    dest_duplicate = rig.create_cloud_path("dir2/new_file0_0.txt")
    assert dest == dest_duplicate
    dest.rename(dest_duplicate)
    assert dest.exists()

    p.touch()
    dest.replace(p)
    assert p.exists()

    dl_file = tmp_path / "file"
    p.download_to(dl_file)
    assert dl_file.exists()
    assert p.read_text() == dl_file.read_text()

    dl_dir = tmp_path / "directory"
    dl_dir.mkdir(parents=True, exist_ok=True)
    p3.download_to(dl_dir)
    cloud_rel_paths = sorted(
        # CloudPath("prefix://drive/dir/file.txt")._no_prefix_no_drive = "/dir/file.txt"
        [p._no_prefix_no_drive[len(rig.test_dir) + 2 :] for p in p3.glob("**/*")]
    )
    dled_rel_paths = sorted(
        [str(PurePosixPath(p.relative_to(dl_dir))) for p in dl_dir.glob("**/*")]
    )
    assert cloud_rel_paths == dled_rel_paths

    with pytest.raises(CloudPathNotExistsError):
        (p / "not_exists_file").download_to(dl_file)


def test_filenames(rig):
    # test that we can handle filenames with special characters
    p = rig.create_cloud_path("dir_0/new_file.txt")  # real extension
    p.write_text("hello")
    assert p.read_text() == "hello"

    p2 = rig.create_cloud_path("dir_0/new_file")  # no extension
    p2.write_text("hello")
    assert p2.read_text() == "hello"

    p3 = rig.create_cloud_path("dir_0/new_file.textfile")  # long extension
    p3.write_text("hello")
    assert p3.read_text() == "hello"

    p4 = rig.create_cloud_path("dir_0/new_file.abc.def.txt")  # multiple suffixes
    p4.write_text("hello")
    assert p4.read_text() == "hello"


def test_dispatch_to_local_cache(rig):
    p = rig.create_cloud_path("dir_0/file0_1.txt")
    stat = p._dispatch_to_local_cache_path("stat")
    assert stat


def test_close_file_idempotent(rig):
    p = rig.create_cloud_path("dir_0/file0_1.txt")

    assert p.read_text() != "hello!"

    f = p.open("w")
    f.write("hello!")
    f.close()
    first_modified = p.stat().st_mtime

    # remove cache so we can be sure it can't be re-uploaded
    p._local.unlink()

    # would raise trying to upload missing cache if we weren't idempotent
    f.close()

    # re-open and ensure things work
    sleep(1)
    f = p.open("w")
    f.write("hello again!")
    f.close()

    # remove cache so we are sure stat is coming from the server
    p._local.unlink()

    assert p.stat().st_mtime > first_modified


def test_cloud_path_download_to(rig, tmp_path):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    dl_dir = tmp_path
    assert not (dl_dir / p.name).exists()
    p.download_to(dl_dir)
    assert (dl_dir / p.name).is_file()


def test_fspath(rig):
    p = rig.create_cloud_path("dir_0/")
    assert os.fspath(p) == p.fspath


def test_os_open(rig):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    with open(p, "r") as f:
        assert f.readable()


def test_drive_exists(rig):
    """Tests the exists call for top level bucket/container"""
    p = rig.create_cloud_path("dir_0/file0_0.txt")

    assert CloudPath(f"{rig.cloud_prefix}{p.drive}").exists()

    assert not CloudPath(f"{rig.cloud_prefix}totally-fake-not-existing-bucket-for-tests").exists()


# =================================================================================
# GHSA-r4f8-3xc4-c8vw: local path traversal via ".." segments in cloud object keys.
# Cloud keys are opaque and some backends (e.g. GCS) accept ".." segments; cloudpathlib
# must not let such a key map to a local path outside the cache dir / download destination.
# =================================================================================

# Two leading "..": the drive/bucket segment cancels one and the cache dir's own last segment the
# other, so the mapping lands exactly in the cache dir's parent — outside the base but inside the
# test tree, so a regression would actually write there and be caught (unlike "../" * 50, which
# clamps at the filesystem root where writes are checked in a location the write never reaches).
_MARKER = "GHSA_r4f8_ESCAPED.txt"
_TRAVERSAL = "../../" + _MARKER


def test_local_cache_path_traversal_blocked(rig):
    """A cloud key with ``..`` segments that escapes the local cache dir must raise rather than
    map outside it. Runs on every provider rig (and live under USE_LIVE_CLOUD)."""
    # construction still succeeds (no normalization at construction) ...
    p = rig.path_class(f"{rig.cloud_prefix}{rig.drive}/{_TRAVERSAL}")
    cache_dir = Path(p.client._local_cache_dir)

    # ... but mapping it to a local cache path is refused.
    with pytest.raises(CloudPathLocalPathTraversalError):
        _ = p._local

    # and nothing was materialized at the location the escape would have reached.
    assert not (cache_dir.parent / _MARKER).exists()


def test_local_cache_path_traversal_no_false_positive(rig):
    """A normal key, and a ``..`` that stays inside the cache, both map within the cache dir and
    are not blocked."""
    q = rig.create_cloud_path("dir_0/file0_0.txt")
    cache_dir = Path(q.client._local_cache_dir).resolve()

    assert Path(q._local).resolve().is_relative_to(cache_dir)  # normal key

    contained = rig.path_class(
        f"{rig.cloud_prefix}{rig.drive}/dir_0/../file0_0.txt", client=q.client
    )
    assert Path(contained._local).resolve().is_relative_to(cache_dir)  # contained ".."


def test_download_to_directory_traversal_blocked(tmp_path, monkeypatch):
    """download_to() on a directory must not write a listed ``..`` child outside the destination.
    Backend interaction is mocked, so provider identity is irrelevant — run once, not per rig."""
    client = LocalS3Client(local_cache_dir=str(tmp_path / "cache"))
    src = LocalS3Path("s3://bkt/src", client=client)
    # a child whose key contributes ".." segments relative to src, as a hostile backend could list
    child = LocalS3Path(f"s3://bkt/src/{_TRAVERSAL}", client=client)

    monkeypatch.setattr(src, "exists", lambda *a, **k: True)
    monkeypatch.setattr(src, "is_file", lambda *a, **k: False)
    monkeypatch.setattr(src, "iterdir", lambda *a, **k: iter([child]))

    dest = tmp_path / "a" / "b" / "dest"
    dest.mkdir(parents=True)
    with pytest.raises(CloudPathLocalPathTraversalError):
        src.download_to(dest)

    assert list(tmp_path.rglob(_MARKER)) == []


def test_download_to_file_name_traversal_blocked(tmp_path, monkeypatch):
    """Single-file download_to(dir) joins ``destination / self.name``; a key ending in ``..`` must
    raise rather than resolve to the destination's parent."""
    client = LocalS3Client(local_cache_dir=str(tmp_path / "cache"))
    src = LocalS3Path("s3://bkt/dir/..", client=client)
    monkeypatch.setattr(src, "exists", lambda *a, **k: True)
    monkeypatch.setattr(src, "is_file", lambda *a, **k: True)

    dest = tmp_path / "dest"
    dest.mkdir()
    with pytest.raises(CloudPathLocalPathTraversalError):
        src.download_to(dest)


@pytest.mark.skipif(
    sys.platform != "win32", reason="backslashes are path separators only on Windows"
)
def test_download_to_file_name_backslash_traversal_blocked_windows(tmp_path, monkeypatch):
    r"""On Windows, backslashes in a cloud key act as path separators when joined onto a local
    destination, so a key whose last ``/``-separated component contains ``..\`` escapes
    ``destination / self.name``; the guard must catch it."""
    client = LocalS3Client(local_cache_dir=str(tmp_path / "cache"))
    src = LocalS3Path(r"s3://bkt/dir/..\..\..\GHSA_r4f8_ESCAPED.txt", client=client)
    monkeypatch.setattr(src, "exists", lambda *a, **k: True)
    monkeypatch.setattr(src, "is_file", lambda *a, **k: True)

    dest = tmp_path / "dest"
    dest.mkdir()
    with pytest.raises(CloudPathLocalPathTraversalError):
        src.download_to(dest)

    assert not (tmp_path / "GHSA_r4f8_ESCAPED.txt").exists()


def test_copytree_local_traversal_blocked(tmp_path, monkeypatch):
    """copytree() to a local directory must not let a ``..`` surfaced as a listing entry escape the
    destination. A delimiter-based listing (e.g. GCS) surfaces the intermediate ``..`` of a key like
    ``dir/../ESCAPED.txt`` as a directory entry named ``..``, so the recursion's destination would
    otherwise walk outside the base. copy/copy_into/move on a directory route through copytree, so
    they are covered too."""
    client = LocalS3Client(local_cache_dir=str(tmp_path / "cache"))
    src = LocalS3Path("s3://bkt/dir", client=client)
    mid = LocalS3Path("s3://bkt/dir/..", client=client)
    leaf = LocalS3Path("s3://bkt/dir/../ESCAPED.txt", client=client)

    monkeypatch.setattr(src, "is_dir", lambda *a, **k: True)
    monkeypatch.setattr(src, "is_file", lambda *a, **k: False)
    monkeypatch.setattr(src, "exists", lambda *a, **k: True)
    monkeypatch.setattr(src, "iterdir", lambda *a, **k: iter([mid]))
    monkeypatch.setattr(mid, "is_dir", lambda *a, **k: True)
    monkeypatch.setattr(mid, "is_file", lambda *a, **k: False)
    monkeypatch.setattr(mid, "exists", lambda *a, **k: True)
    monkeypatch.setattr(mid, "iterdir", lambda *a, **k: iter([leaf]))
    monkeypatch.setattr(leaf, "is_dir", lambda *a, **k: False)
    monkeypatch.setattr(leaf, "is_file", lambda *a, **k: True)
    monkeypatch.setattr(leaf, "exists", lambda *a, **k: True)

    def fake_download(cloud_path, local_path):
        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text("attacker bytes")
        return local_path

    monkeypatch.setattr(client, "_download_file", fake_download)

    dest = tmp_path / "a" / "b" / "dest"
    with pytest.raises(CloudPathLocalPathTraversalError):
        src.copytree(dest)

    assert list(tmp_path.rglob("ESCAPED.txt")) == []


def test_clear_cache_no_raise_on_unmappable_key(tmp_path):
    """An un-mappable (``..``-escaping) key can never have been cached, so clear_cache() is a no-op
    rather than raising — and __del__, which calls it under cloudpath_object cache mode, must not
    raise on merely constructing and dropping such a path."""
    import gc

    client = LocalS3Client(
        local_cache_dir=str(tmp_path / "cache"),
        local_storage_dir=str(tmp_path / "storage"),
        file_cache_mode=FileCacheMode.cloudpath_object,
    )
    p = LocalS3Path("s3://bkt/../../evil.txt", client=client)

    assert p.clear_cache() is None  # no-op, does not raise

    del p
    gc.collect()  # exercises __del__ -> clear_cache; must not raise


def test_mock_storage_side_traversal_blocked(tmp_path):
    """The local mock's simulated cloud is the real filesystem, so its storage-side mapping must
    also refuse escaping keys — otherwise the mock backend itself reads/writes/deletes outside
    the simulated storage directory (via exists/stat/unlink/upload_from/download_to)."""
    storage = tmp_path / "deep" / "storage"
    storage.mkdir(parents=True)
    victim = tmp_path / "deep" / "victim.txt"
    victim.write_text("do not touch")
    client = LocalS3Client(local_storage_dir=str(storage), local_cache_dir=str(tmp_path / "cache"))
    p = LocalS3Path("s3://bkt/../../victim.txt", client=client)

    src_file = tmp_path / "src.txt"
    src_file.write_text("attacker bytes")

    for op in (
        lambda: p.exists(),
        lambda: p.stat(),
        lambda: p.unlink(),
        lambda: p.upload_from(src_file),
        lambda: p.download_to(tmp_path / "out"),
    ):
        with pytest.raises(CloudPathLocalPathTraversalError):
            op()

    assert victim.read_text() == "do not touch"


def test_cache_traversal_blocked_without_object_mock(tmp_path):
    """On the LocalS3 mock (offline): ops that materialize the cache — write, open("wb"), fspath,
    and the ``_local`` mapping itself — are blocked for a ``..`` key, and nothing is written
    outside the cache dir."""
    cache = tmp_path / "a" / "b" / "c" / "cache"
    cache.mkdir(parents=True)
    client = LocalS3Client(local_cache_dir=str(cache))
    p = LocalS3Path("s3://bkt/../../GHSA_r4f8_ESCAPED.txt", client=client)

    for op in (
        lambda: p._local,
        lambda: p.write_text("pwned"),
        lambda: p.write_bytes(b"pwned"),
        lambda: p.open("wb"),
        lambda: os.fspath(p),
    ):
        with pytest.raises(CloudPathLocalPathTraversalError):
            op()

    # bkt/../../<name> lands in the cache dir's parent; confirm nothing was written there
    assert not (cache.parent / "GHSA_r4f8_ESCAPED.txt").exists()


def test_cache_traversal_blocked_with_existing_object_mock(tmp_path):
    """On the LocalS3 mock (offline): reading an EXISTING object whose key contains ``..`` is
    blocked. The file is planted directly at the traversed backing location (out of band, as a
    hostile tenant would), since cloudpathlib's writer and the mock backend are both guarded."""
    storage = tmp_path / "s1" / "s2" / "s3" / "storage"
    storage.mkdir(parents=True)
    cache = tmp_path / "c1" / "c2" / "c3" / "cache"
    cache.mkdir(parents=True)
    client = LocalS3Client(local_storage_dir=str(storage), local_cache_dir=str(cache))
    p = LocalS3Path("s3://bkt/../../GHSA_r4f8_READ.txt", client=client)

    # where the (formerly unguarded) mapping storage/bkt/../../<name> would have pointed
    backing = storage.parent / "GHSA_r4f8_READ.txt"
    backing.write_bytes(b"secret-from-outside-the-cache")

    for op in (
        lambda: p.exists(),
        lambda: p.read_bytes(),
        lambda: p.read_text(),
        lambda: p.open("rb"),
    ):
        with pytest.raises(CloudPathLocalPathTraversalError):
            op()

    assert not (cache.parent / "GHSA_r4f8_READ.txt").exists()


def test_cache_traversal_contained_dotdot_allowed_mock(tmp_path):
    """A ``..`` that stays inside the cache dir (e.g. ``a/../b``) is NOT blocked — the guard
    targets only escapes, not every ``..``."""
    client = LocalS3Client(local_cache_dir=str(tmp_path / "cache"))
    p = LocalS3Path("s3://bkt/dir/../file.txt", client=client)
    local = p._local  # must not raise
    assert Path(local).resolve().is_relative_to((tmp_path / "cache").resolve())


def test_ensure_local_path_within_base_unit(tmp_path):
    """Unit test for the containment helper used by both sinks."""
    base = tmp_path / "base"
    base.mkdir()
    sentinel = LocalS3Path("s3://bkt/key.txt", client=LocalS3Client(local_cache_dir=str(tmp_path)))

    # contained -> returned unchanged
    inside = base / "sub" / "file.txt"
    assert _ensure_local_path_within_base(inside, base, sentinel) == inside
    # contained internal ".." that stays inside -> allowed
    contained = base / "sub" / ".." / "file.txt"
    assert _ensure_local_path_within_base(contained, base, sentinel) == contained
    # escaping -> raises
    with pytest.raises(CloudPathLocalPathTraversalError):
        _ensure_local_path_within_base(base / ".." / ".." / "evil.txt", base, sentinel)

    # lexical mode (resolve=False, used for the cache mapping) behaves the same
    assert _ensure_local_path_within_base(contained, base, sentinel, resolve=False) == contained
    with pytest.raises(CloudPathLocalPathTraversalError):
        _ensure_local_path_within_base(base / ".." / "evil.txt", base, sentinel, resolve=False)
