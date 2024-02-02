from datetime import datetime
import os
from pathlib import Path, PurePosixPath
import pickle
from shutil import rmtree
import sys
from time import sleep

import pytest
from cloudpathlib import CloudPath

from cloudpathlib.exceptions import (
    CloudPathIsADirectoryError,
    CloudPathNotImplementedError,
    DirectoryNotEmptyError,
)


def test_file_discovery(rig):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    assert p.exists()

    p2 = rig.create_cloud_path("dir_0/not_a_file")
    assert not p2.exists()
    p2.touch()
    assert p2.exists()
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
        (root / "dirB" / "fileB").write_text("fileB")
        (root / "dirC").mkdir()
        (root / "dirC" / "dirD").mkdir()
        (root / "dirC" / "dirD" / "fileD").write_text("fileD")
        (root / "dirC" / "fileC").write_text("fileC")
        (root / "fileA").write_text("fileA")

    cloud_root = rig.create_cloud_path("glob-tests")
    cloud_root.mkdir()

    _make_glob_directory(cloud_root)

    local_root = tmp_path / "glob-tests"
    local_root.mkdir()

    _make_glob_directory(local_root)

    yield cloud_root, local_root

    cloud_root.rmtree()
    rmtree(local_root)


def _lstrip_path_root(path, root):
    rel_path = str(path)[len(str(root)) :]
    return rel_path.rstrip("/")  # agnostic to trailing slash


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


def test_list_buckets(rig):
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
        _assert_glob_results_match(
            getattr(cloud_root, glob_method)(pattern, **kwargs),
            getattr(local_root, glob_method)(pattern, **kwargs),
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

    # rglob_common
    _check_glob("*", "rglob")
    _check_glob("fileA", "rglob")
    _check_glob("fileB", "rglob")
    _check_glob("*/fileA", "rglob")
    _check_glob("*/fileB", "rglob")
    _check_glob("file*", "rglob")

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
        assert test_case.is_dir()
        assert not test_case.is_file()

    file = rig.create_cloud_path("dir_0/file0_0.txt")
    file_nested = rig.create_cloud_path("dir_1/dir_1_0/file_1_0_0.txt")

    for test_case in [file, file_nested]:
        assert test_case.is_file()
        assert not test_case.is_dir()

    # does not exist (same behavior as pathlib.Path that does not exist)
    non_existant = rig.create_cloud_path("dir_0/not_a_file")
    assert not non_existant.is_file()
    assert not non_existant.is_dir()


def test_file_read_writes(rig, tmp_path):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    p2 = rig.create_cloud_path("dir_0/not_a_file")
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
    p.touch()
    if not getattr(rig, "is_custom_s3", False):
        # Our S3Path.touch implementation does not update mod time for MinIO
        assert datetime.fromtimestamp(p.stat().st_mtime) > before_touch

    # no-op
    p.mkdir()

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
    p = rig.create_cloud_path("dir_0")
    assert os.fspath(p) == p.fspath


def test_os_open(rig):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    with open(p, "r") as f:
        assert f.readable()


def test_pickle(rig, tmpdir):
    p = rig.create_cloud_path("dir_0/file0_0.txt")

    with (tmpdir / "test.pkl").open("wb") as f:
        pickle.dump(p, f)

    with (tmpdir / "test.pkl").open("rb") as f:
        pickled = pickle.load(f)

    # test a call to the network
    assert pickled.exists()

    # check we unpickled, and that client is the default client
    assert str(pickled) == str(p)
    assert pickled.client == p.client
    assert rig.client_class._default_client == pickled.client


def test_drive_exists(rig):
    """Tests the exists call for top level bucket/container"""
    p = rig.create_cloud_path("dir_0/file0_0.txt")

    assert CloudPath(f"{rig.cloud_prefix}{p.drive}").exists()

    assert not CloudPath(f"{rig.cloud_prefix}totally-fake-not-existing-bucket-for-tests").exists()
