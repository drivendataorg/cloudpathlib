import os

from azure.core.credentials import AzureNamedKeyCredential
from azure.storage.blob import (
    BlobServiceClient,
    StorageStreamDownloader,
)

from azure.storage.filedatalake import DataLakeServiceClient
import pytest

from urllib.parse import urlparse, parse_qs
from cloudpathlib import AzureBlobClient, AzureBlobPath
from cloudpathlib.exceptions import (
    CloudPathIsADirectoryError,
    DirectoryNotEmptyError,
    MissingCredentialsError,
)
from cloudpathlib.local import LocalAzureBlobClient, LocalAzureBlobPath

from .mock_clients.mock_azureblob import MockStorageStreamDownloader


@pytest.mark.parametrize("path_class", [AzureBlobPath, LocalAzureBlobPath])
def test_azureblobpath_properties(path_class, monkeypatch):
    if not os.getenv("AZURE_STORAGE_CONNECTION_STRING"):
        monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "AccountName=fake;AccountKey=fake2;")

    p = path_class("az://mycontainer")
    assert p.blob == ""
    assert p.container == "mycontainer"

    p2 = path_class("az://mycontainer/")
    assert p2.blob == ""
    assert p2.container == "mycontainer"


@pytest.mark.parametrize("client_class", [AzureBlobClient, LocalAzureBlobClient])
def test_azureblobpath_nocreds(client_class, monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    with pytest.raises(MissingCredentialsError):
        client_class()


def test_as_url(azure_rigs):
    p: AzureBlobPath = azure_rigs.create_cloud_path("dir_0/file0_0.txt")

    public_url = str(p.as_url())
    public_parts = urlparse(public_url)

    assert public_parts.path.endswith("file0_0.txt")

    presigned_url = p.as_url(presign=True)
    parts = urlparse(presigned_url)
    query_params = parse_qs(parts.query)
    assert parts.path.endswith("file0_0.txt")
    assert "se" in query_params
    assert "sp" in query_params
    assert "sr" in query_params
    assert "sig" in query_params


def test_partial_download(azure_rigs, monkeypatch):
    p: AzureBlobPath = azure_rigs.create_cloud_path("dir_0/file0_0.txt")

    # no partial after successful download
    p.read_text()  # downloads
    assert p._local.exists()
    assert not p.client._partial_filename(p._local).exists()

    # remove cache manually
    p._local.unlink()
    assert not p._local.exists()

    # no partial after failed download
    with monkeypatch.context() as m:

        def _patched(self, buffer):
            buffer.write(b"partial")
            raise Exception("boom")

        if azure_rigs.live_server:
            m.setattr(StorageStreamDownloader, "readinto", _patched)
        else:
            m.setattr(MockStorageStreamDownloader, "readinto", _patched)

        with pytest.raises(Exception):
            p.read_text()  # downloads; should raise

        assert not p._local.exists()
        assert not p.client._partial_filename(p._local).exists()


def test_client_instantiation(azure_rigs, monkeypatch):
    # don't use creds from env vars for these tests
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING")

    if not azure_rigs.live_server:
        return

    bsc = BlobServiceClient.from_connection_string(azure_rigs.connection_string)
    dlsc = DataLakeServiceClient.from_connection_string(azure_rigs.connection_string)

    def _check_access(az_client, gen2=False):
        """Check API access by listing."""
        assert len(list(az_client.service_client.list_containers())) > 0

        if gen2:
            assert len(list(az_client.data_lake_client.list_file_systems())) > 0

    # test just BlobServiceClient passed
    cl = azure_rigs.client_class(blob_service_client=bsc)
    _check_access(cl, gen2=azure_rigs.is_adls_gen2)

    cl = azure_rigs.client_class(data_lake_client=dlsc)
    _check_access(cl, gen2=azure_rigs.is_adls_gen2)

    cl = azure_rigs.client_class(blob_service_client=bsc, data_lake_client=dlsc)
    _check_access(cl, gen2=azure_rigs.is_adls_gen2)

    cl = azure_rigs.client_class(
        account_url=bsc.url,
        credential=AzureNamedKeyCredential(
            bsc.credential.account_name, bsc.credential.account_key
        ),
    )
    _check_access(cl, gen2=azure_rigs.is_adls_gen2)

    cl = azure_rigs.client_class(
        account_url=dlsc.url,
        credential=AzureNamedKeyCredential(
            bsc.credential.account_name, bsc.credential.account_key
        ),
    )
    _check_access(cl, gen2=azure_rigs.is_adls_gen2)


def test_adls_gen2_mkdir(azure_gen2_rig):
    """Since directories can be created on gen2, we should test mkdir, rmdir, rmtree, and unlink
    all work as expected.
    """
    p = azure_gen2_rig.create_cloud_path("new_dir")

    # mkdir
    p.mkdir()
    assert p.exists() and p.is_dir()
    # rmdir does not throw
    p.rmdir()

    # mkdir
    p.mkdir()
    p.mkdir(exist_ok=True)  # ensure not raises

    with pytest.raises(FileExistsError):
        p.mkdir(exist_ok=False)

    # touch file
    (p / "file.txt").write_text("content")
    # rmdir throws - not empty
    with pytest.raises(DirectoryNotEmptyError):
        p.rmdir()

    # rmtree works
    p.rmtree()
    assert not p.exists()

    # mkdir
    p2 = p / "nested"

    with pytest.raises(FileNotFoundError):
        p2.mkdir()

    p2.mkdir(parents=True)
    assert p2.exists()

    with pytest.raises(CloudPathIsADirectoryError):
        p2.unlink()


def test_adls_gen2_rename(azure_gen2_rig):
    # rename file
    p = azure_gen2_rig.create_cloud_path("file.txt")
    p.write_text("content")
    p2 = p.rename(azure_gen2_rig.create_cloud_path("file2.txt"))
    assert not p.exists()
    assert p2.exists()

    # rename dir
    p = azure_gen2_rig.create_cloud_path("dir")
    p.mkdir()
    p2 = p.rename(azure_gen2_rig.create_cloud_path("dir2"))
    assert not p.exists()
    assert p2.exists()
