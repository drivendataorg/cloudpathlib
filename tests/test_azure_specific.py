import os

from cloudpathlib import AzureBlobPath


def test_azureblobpath_properties(azure_rig):
    p = AzureBlobPath("az://container")
    assert p.blob == ""
    assert p.container == "container"

    p2 = AzureBlobPath("az://container/")
    assert p2.blob == ""
    assert p2.container == "container"


def test_azureblobpath_is_path(azure_rig):
    p = AzureBlobPath("az://container")
    assert isinstance(p, os.PathLike)
