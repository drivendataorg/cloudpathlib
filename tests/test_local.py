import pytest

from inspect import signature

from cloudpathlib import AzureBlobClient, AzureBlobPath, GSClient, GSPath, S3Client, S3Path
from cloudpathlib.local import (
    LocalAzureBlobClient,
    LocalAzureBlobPath,
    LocalGSClient,
    LocalGSPath,
    LocalS3Client,
    LocalS3Path,
)


@pytest.mark.parametrize(
    "cloud_class,local_class",
    [
        (AzureBlobClient, LocalAzureBlobClient),
        (AzureBlobPath, LocalAzureBlobPath),
        (GSClient, LocalGSClient),
        (GSPath, LocalGSPath),
        (S3Client, LocalS3Client),
        (S3Path, LocalS3Path),
    ],
)
def test_interface(cloud_class, local_class):
    """Test that local class implements associated cloud class's interface"""

    cloud_attr_names = [attr for attr in dir(cloud_class) if not attr.startswith("_")]
    local_attr_names = [attr for attr in dir(local_class) if not attr.startswith("_")]

    assert set(cloud_attr_names).issubset(local_attr_names)

    for attr_name in cloud_attr_names:
        cloud_attr = getattr(cloud_class, attr_name)
        local_attr = getattr(local_class, attr_name)

        assert type(cloud_attr) == type(local_attr)
        if callable(cloud_attr):
            assert signature(cloud_attr).parameters == signature(local_attr).parameters


@pytest.mark.parametrize("client_class", [LocalAzureBlobClient, LocalGSClient, LocalS3Client])
def test_default_storage_dir(client_class, monkeypatch):
    """Test that local file storage for a LocalClient persists across client instantiations."""

    if client_class is LocalAzureBlobClient:
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "")

    cloud_prefix = client_class._cloud_meta.path_class.cloud_prefix

    p1 = client_class().CloudPath(f"{cloud_prefix}drive/file.txt")
    p2 = client_class().CloudPath(f"{cloud_prefix}drive/file.txt")

    assert not p1.exists()
    assert not p2.exists()

    p1.write_text("hello")
    assert p1.exists()
    assert p1.read_text() == "hello"
    assert p2.exists()
    assert p2.read_text() == "hello"

    # clean up
    client_class.reset_default_storage_dir()


@pytest.mark.parametrize("client_class", [LocalAzureBlobClient, LocalGSClient, LocalS3Client])
def test_reset_default_storage_dir(client_class, monkeypatch):
    """Test that LocalClient default storage reset changes the default temp directory."""

    if client_class is LocalAzureBlobClient:
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "")

    cloud_prefix = client_class._cloud_meta.path_class.cloud_prefix

    p1 = client_class().CloudPath(f"{cloud_prefix}drive/file.txt")
    client_class.reset_default_storage_dir()
    p2 = client_class().CloudPath(f"{cloud_prefix}drive/file.txt")

    assert not p1.exists()
    assert not p2.exists()

    p1.write_text("hello")
    assert p1.exists()
    assert p1.read_text() == "hello"
    assert not p2.exists()

    # clean up
    client_class.reset_default_storage_dir()
