import importlib
import os

import pytest

import cloudpathlib
from cloudpathlib import patch_open


def test_patch_open(rig):
    cp = rig.create_cloud_path("dir_0/new_file.txt")

    with pytest.raises(FileNotFoundError):
        with open(cp, "w") as f:
            f.write("Hello!")

    # set via method call
    with patch_open():
        with open(cp, "w") as f:
            f.write("Hello!")

        assert cp.read_text() == "Hello!"

    # set via env var
    cp2 = rig.create_cloud_path("dir_0/new_file_two.txt")
    original_env_setting = os.environ.get("CLOUDPATHLIB_PATCH_OPEN", "")

    try:
        os.environ["CLOUDPATHLIB_PATCH_OPEN"] = "1"

        importlib.reload(cloudpathlib)

        with open(cp2, "w") as f:
            f.write("Hello!")

        assert cp2.read_text() == "Hello!"

    finally:
        os.environ["CLOUDPATHLIB_PATCH_OPEN"] = original_env_setting
        importlib.reload(cloudpathlib)

    # cp.write_text("Hello!")

    # # remove cache
    # cp._local.unlink()


def test_patches(rig):
    pass
