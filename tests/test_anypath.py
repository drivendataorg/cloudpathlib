import os
from pathlib import Path, PosixPath, WindowsPath

import pytest

from cloudpathlib.anypath import AnyPath
from cloudpathlib.cloudpath import CloudPath
from cloudpathlib.exceptions import AnyPathTypeError


def test_anypath_path():
    path = Path("a/b/c")
    assert AnyPath(path) == path
    assert AnyPath(str(path)) == path

    assert isinstance(path, AnyPath)
    assert not isinstance(str(path), AnyPath)

    assert issubclass(Path, AnyPath)
    assert issubclass(PosixPath, AnyPath)
    assert issubclass(WindowsPath, AnyPath)
    assert not issubclass(str, AnyPath)

    # test `file:` scheme (only works with absolute paths; needs .absolute() on Windows)
    assert AnyPath(path.absolute().resolve().as_uri()) == path.absolute().resolve()

    # test file:// + multi arg
    assert AnyPath(*path.absolute().resolve().as_uri().rsplit("/", 2)) == path.absolute().resolve()

    # test no hostname
    assert Path("/foo/bar") == AnyPath("file:/foo/bar")
    assert Path("/foo/bar") == AnyPath("file:///foo/bar")

    # windows tests
    if os.name == "nt":
        assert Path("c:\\hello\\test.txt") == AnyPath("file:/c:/hello/test.txt")
        assert Path("c:\\hello\\test.txt") == AnyPath("file://c:/hello/test.txt")
        assert Path("c:\\hello\\test.txt") == AnyPath("file:///c:/hello/test.txt")
        assert Path("c:\\hello\\test.txt") == AnyPath("file://c%3A//hello/test.txt")
        assert Path("c:\\hello\\test.txt") == AnyPath("file://localhost/c%3a/hello/test.txt")
        assert Path("c:\\WINDOWS\\clock.avi") == AnyPath("file://localhost/c|/WINDOWS/clock.avi")
        assert Path("c:\\WINDOWS\\clock.avi") == AnyPath("file:///c|/WINDOWS/clock.avi")
        assert Path("c:\\hello\\test space.txt") == AnyPath(
            "file://localhost/c%3a/hello/test%20space.txt"
        )


def test_anypath_cloudpath(rig):
    cloudpath = rig.create_cloud_path("a/b/c")
    assert AnyPath(cloudpath) == cloudpath
    assert AnyPath(str(cloudpath)) == cloudpath

    assert isinstance(cloudpath, AnyPath)
    assert not isinstance(str(cloudpath), AnyPath)

    assert issubclass(CloudPath, AnyPath)
    assert issubclass(rig.path_class, AnyPath)


def test_anypath_bad_input():
    with pytest.raises(AnyPathTypeError):
        AnyPath(0)


def test_anypath_subclass_anypath():
    assert issubclass(AnyPath, AnyPath)
