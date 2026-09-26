from inspect import signature
from pathlib import Path
import shutil
from urllib.parse import parse_qs, urlsplit

import pytest

from cloudpathlib import AzureBlobClient, AzureBlobPath, GSClient, GSPath, S3Client, S3Path
from cloudpathlib.local import (
    LocalAzureBlobClient,
    LocalAzureBlobPath,
    LocalGSClient,
    LocalGSPath,
    LocalS3Client,
    LocalS3Path,
)
from cloudpathlib.local import localclient as localclient_mod


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

        assert type(cloud_attr) is type(local_attr)
        if callable(cloud_attr):
            # does not check type annotations, which can vary semantically, but are the same (e.g., Self != AzureBlobPath)
            assert all(
                a.name == b.name
                for a, b in zip(
                    signature(cloud_attr).parameters.values(),
                    signature(local_attr).parameters.values(),
                )
            )


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

    # p2 uses a new client, but the simulated "cloud" should be the same
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
    assert not p1.exists()
    p1.write_text("hello")
    assert p1.exists()
    assert p1.read_text() == "hello"

    client_class.reset_default_storage_dir()

    # We've reset the default storage directory, so the file should be gone
    assert not p1.exists()

    # Also should be gone for p2, which uses a new client that is still using default storage dir
    p2 = client_class().CloudPath(f"{cloud_prefix}drive/file.txt")
    assert not p2.exists()

    # clean up
    client_class.reset_default_storage_dir()


def test_reset_default_storage_dir_with_default_client():
    """Test that reset_default_storage_dir resets the storage used by all clients that are using
    the default storage directory, such as the default client.

    Regression test for https://github.com/drivendataorg/cloudpathlib/issues/414
    """
    # try default client instantiation
    from cloudpathlib.local import LocalS3Path, LocalS3Client

    s3p = LocalS3Path("s3://drive/file.txt")
    assert not s3p.exists()
    s3p.write_text("hello")
    assert s3p.exists()

    LocalS3Client.reset_default_storage_dir()
    s3p2 = LocalS3Path("s3://drive/file.txt")
    assert not s3p2.exists()


@pytest.mark.parametrize("client_class", [LocalAzureBlobClient, LocalGSClient, LocalS3Client])
def test_glob_matches(client_class, monkeypatch):
    if client_class is LocalAzureBlobClient:
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "")

    cloud_prefix = client_class._cloud_meta.path_class.cloud_prefix
    p = client_class().CloudPath(f"{cloud_prefix}drive/not/exist")
    p.mkdir(parents=True)

    # match CloudPath, which returns empty; not glob module, which raises
    assert list(p.glob("*")) == []


@pytest.mark.parametrize("client_class", [LocalAzureBlobClient, LocalGSClient, LocalS3Client])
def test_as_url_presign(client_class, monkeypatch):
    if client_class is LocalAzureBlobClient:
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "")

    cloud_prefix = client_class._cloud_meta.path_class.cloud_prefix
    p = client_class().CloudPath(f"{cloud_prefix}drive/file.txt")
    p.write_text("hello")

    expire_seconds = 123
    presigned_url = p.as_url(presign=True, expire_seconds=expire_seconds)
    parts = urlsplit(presigned_url)
    query_params = parse_qs(parts.query)

    assert parts.path.endswith("file.txt")
    assert query_params["expires"] == [str(expire_seconds)]
    assert query_params["signature"] == ["local"]


def _local_download_client(tmp_path):
    return LocalS3Client(
        local_storage_dir=str(tmp_path / "storage"),
        local_cache_dir=str(tmp_path / "cache"),
    )


def test_download_file_fails_immediately_if_source_missing(tmp_path, monkeypatch):
    client = _local_download_client(tmp_path)
    src = client.CloudPath("s3://drive/missing.txt")
    dest = tmp_path / "dest" / "file.txt"
    sleeps = []
    monkeypatch.setattr(localclient_mod, "sleep", sleeps.append)

    with pytest.raises(FileNotFoundError):
        client._download_file(src, dest)

    assert sleeps == []


def test_download_file_retries_when_destination_parent_vanishes(tmp_path, monkeypatch):
    client = _local_download_client(tmp_path)
    src = client.CloudPath("s3://drive/file.txt")
    src.write_text("hello")
    dest = tmp_path / "dest" / "file.txt"
    dest.parent.mkdir(parents=True)

    real_copyfile = shutil.copyfile
    calls = {"n": 0}

    def flaky_copyfile(src_path, dst_path):
        calls["n"] += 1
        if calls["n"] == 1:
            Path(dst_path).parent.rmdir()
            raise FileNotFoundError(dst_path)
        return real_copyfile(src_path, dst_path)

    monkeypatch.setattr(shutil, "copyfile", flaky_copyfile)
    sleeps = []
    monkeypatch.setattr(localclient_mod, "sleep", sleeps.append)

    result = client._download_file(src, dest)

    assert result.read_text() == "hello"
    assert sleeps == [0.05]


def test_download_file_gives_up_after_backoff(tmp_path, monkeypatch):
    client = _local_download_client(tmp_path)
    src = client.CloudPath("s3://drive/file.txt")
    src.write_text("hello")
    dest = tmp_path / "dest" / "file.txt"

    def always_missing(src_path, dst_path):
        raise FileNotFoundError(dst_path)

    monkeypatch.setattr(shutil, "copyfile", always_missing)
    sleeps = []
    monkeypatch.setattr(localclient_mod, "sleep", sleeps.append)

    with pytest.raises(FileNotFoundError):
        client._download_file(src, dest)

    assert sleeps == list(localclient_mod._DOWNLOAD_RETRY_DELAYS)
