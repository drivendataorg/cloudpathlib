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
