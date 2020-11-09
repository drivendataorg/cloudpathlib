import os

import pytest

from cloudpathlib import AzureBlobPath, CloudPath, InvalidPrefix, MissingDependencies, S3Path


@pytest.mark.parametrize(
    "path_class, cloud_path",
    [
        (AzureBlobPath, "az://b/k"),
        (AzureBlobPath, "AZ://b/k"),
        (AzureBlobPath, "Az://b/k"),
        (AzureBlobPath, "aZ://b/k"),
        (S3Path, "s3://b/k"),
        (S3Path, "S3://b/k"),
    ],
)
def test_dispatch(path_class, cloud_path, monkeypatch):
    """Test that CloudPath(...) appropriately dispatches to the correct cloud's implementation
    class.
    """
    if path_class == AzureBlobPath:
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "AccountName=fake;AccountKey=fake2;")

    assert isinstance(CloudPath(cloud_path), path_class)


def test_dispatch_error():
    with pytest.raises(InvalidPrefix):
        CloudPath("pp://b/k")


@pytest.mark.parametrize("path", ["b/k", "b/k", "b/k.file", "b/k", "b"])
def test_instantiation(rig, path):
    # check two cases of prefix
    for prefix in [rig.cloud_prefix.lower(), rig.cloud_prefix.upper()]:
        expected = prefix + path
        p = rig.path_class(expected)
        assert repr(p) == f"{rig.path_class.__name__}('{expected}')"
        assert str(p) == expected

        assert p._no_prefix == expected.split("://", 1)[-1]

        assert p._url.scheme == expected.split("://", 1)[0].lower()
        assert p._url.netloc == expected.split("://", 1)[-1].split("/")[0]

        assert str(p._path) == expected.split(":/", 1)[-1]


def test_instantiation_errors(rig):
    with pytest.raises(TypeError):
        rig.path_class()

    with pytest.raises(InvalidPrefix):
        rig.path_class("NOT_S3_PATH")


def test_idempotency(rig):
    rig.client_class._default_client = None

    client = rig.client_class()
    p = client.CloudPath(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt")

    p2 = CloudPath(p)
    assert p == p2
    assert p.client == p2.client


def test_dependencies_not_loaded(rig, monkeypatch):
    monkeypatch.setattr(rig.path_class._cloud_meta, "dependencies_loaded", False)
    with pytest.raises(MissingDependencies):
        CloudPath(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt")
    with pytest.raises(MissingDependencies):
        rig.create_cloud_path("dir_0/file0_0.txt")


def test_is_pathlike(rig):
    p = rig.create_cloud_path("dir_0")
    assert isinstance(p, os.PathLike)
