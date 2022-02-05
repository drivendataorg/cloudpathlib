from datetime import datetime
import os
from pathlib import PurePosixPath
from shutil import rmtree
from time import sleep

import pytest

from cloudpathlib.exceptions import CloudPathIsADirectoryError, DirectoryNotEmptyError


def test_file_discovery(rig):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    assert p.exists()

    p2 = rig.create_cloud_path("dir_0/not_a_file")
    assert not p2.exists()
    p2.touch()
    assert p2.exists()
    p2.unlink()

    p3 = rig.create_cloud_path("dir_0/")
    assert p3.exists()
    assert len(list(p3.iterdir())) == 3
    assert len(list(p3.glob("**/*"))) == 3

    with pytest.raises(CloudPathIsADirectoryError):
        p3.unlink()

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
        (root / "dirE").mkdir()
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


def _assert_glob_results_match(cloud_results, local_results, cloud_root, local_root):
    def _lstrip_path_root(path, root):
        return str(path)[len(str(root)) :]

    local_results_no_root = {_lstrip_path_root(c, local_root) for c in local_results}
    cloud_results_no_root = {_lstrip_path_root(c, cloud_root) for c in cloud_results}
    assert local_results_no_root == cloud_results_no_root


def test_glob(glob_test_dirs):
    cloud_root, local_root = glob_test_dirs

    # cases adapted from CPython glob tests:
    #  https://github.com/python/cpython/blob/7ffe7ba30fc051014977c6f393c51e57e71a6648/Lib/test/test_pathlib.py#L1634-L1720

    def _check_glob(pattern, glob_method):
        _assert_glob_results_match(
            getattr(cloud_root, glob_method)(pattern),
            getattr(local_root, glob_method)(pattern),
            cloud_root,
            local_root,
        )

    # glob_common
    _check_glob("fileA", "glob")
    _check_glob("fileB", "glob")
    _check_glob("dir*/file*", "glob")
    _check_glob("*A", "glob")
    _check_glob("*B/*", "glob")
    _check_glob("*/fileB", "glob")

    # rglob_common
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

    # test_glob_many_open_files
    depth = 30
    base = cloud_root / "deep"
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
    with pytest.raises(NotImplementedError, match="Relative paths with"):
        list(cp.glob("../hello"))

    with pytest.raises(NotImplementedError, match="Relative paths with"):
        list(cp.rglob("../hello"))

    # non-relative paths
    with pytest.raises(NotImplementedError, match="Non-relative patterns"):
        list(cp.glob(f"{rig.path_class.cloud_prefix}bucket/path/**/*.jpg"))

    with pytest.raises(NotImplementedError, match="Non-relative patterns"):
        list(cp.glob("/path/**/*.jpg"))

    with pytest.raises(NotImplementedError, match="Non-relative patterns"):
        list(cp.rglob(f"{rig.path_class.cloud_prefix}bucket/path/**/*.jpg"))

    with pytest.raises(NotImplementedError, match="Non-relative patterns"):
        list(cp.rglob("/path/**/*.jpg"))

    # file is root of glob call
    file = rig.create_cloud_path("dir_0/file0_0.txt")
    with pytest.raises(NotImplementedError, match="file as the root"):
        list(file.glob("*"))

    with pytest.raises(NotImplementedError, match="file as the root"):
        list(file.rglob("*"))


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
