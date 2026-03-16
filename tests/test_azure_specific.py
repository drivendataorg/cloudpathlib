import os

from azure.core.credentials import AzureNamedKeyCredential
from azure.identity import DefaultAzureCredential
from azure.storage.blob import (
    BlobServiceClient,
    StorageStreamDownloader,
)

from azure.storage.filedatalake import DataLakeServiceClient
import pytest

import cloudpathlib.azure.azblobclient
from urllib.parse import urlparse, parse_qs
from cloudpathlib import AzureBlobClient, AzureBlobPath
from cloudpathlib.exceptions import (
    CloudPathIsADirectoryError,
    DirectoryNotEmptyError,
    MissingCredentialsError,
)
from cloudpathlib.local import LocalAzureBlobClient, LocalAzureBlobPath

from .mock_clients.mock_azureblob import MockBlobServiceClient, MockStorageStreamDownloader
from .mock_clients.mock_adls_gen2 import MockedDataLakeServiceClient


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
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    monkeypatch.setattr(
        "cloudpathlib.azure.azblobclient.DefaultAzureCredential", None
    )
    with pytest.raises(MissingCredentialsError):
        client_class()


def _mock_azure_clients(monkeypatch):
    """Monkeypatch BlobServiceClient and DataLakeServiceClient with mocks."""
    monkeypatch.setattr(
        cloudpathlib.azure.azblobclient, "BlobServiceClient", MockBlobServiceClient
    )
    monkeypatch.setattr(
        cloudpathlib.azure.azblobclient, "DataLakeServiceClient", MockedDataLakeServiceClient
    )


def test_default_credential_used_with_account_url(monkeypatch):
    """DefaultAzureCredential is used when account_url is provided without credential."""
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    _mock_azure_clients(monkeypatch)

    client = AzureBlobClient(account_url="https://myaccount.blob.core.windows.net")

    assert isinstance(client.service_client, MockBlobServiceClient)
    assert client.service_client._account_url == "https://myaccount.blob.core.windows.net"
    assert isinstance(client.service_client._credential, DefaultAzureCredential)

    assert isinstance(client.data_lake_client, MockedDataLakeServiceClient)
    assert client.data_lake_client._account_url == "https://myaccount.dfs.core.windows.net"
    assert isinstance(client.data_lake_client._credential, DefaultAzureCredential)


def test_no_default_credential_when_explicit_credential(monkeypatch):
    """DefaultAzureCredential is NOT used when an explicit credential is provided."""
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    _mock_azure_clients(monkeypatch)

    explicit_cred = "my-explicit-credential"
    client = AzureBlobClient(
        account_url="https://myaccount.blob.core.windows.net",
        credential=explicit_cred,
    )

    assert client.service_client._credential == explicit_cred
    assert not isinstance(client.service_client._credential, DefaultAzureCredential)


def test_fallback_when_azure_identity_not_installed(monkeypatch):
    """When azure-identity is not installed, credential=None is passed through."""
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    monkeypatch.setattr(
        cloudpathlib.azure.azblobclient, "DefaultAzureCredential", None
    )
    _mock_azure_clients(monkeypatch)

    client = AzureBlobClient(account_url="https://myaccount.blob.core.windows.net")

    assert client.service_client._credential is None


def test_account_url_env_var_blob(monkeypatch):
    """AZURE_STORAGE_ACCOUNT_URL env var with .blob. URL creates both clients."""
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.setenv(
        "AZURE_STORAGE_ACCOUNT_URL", "https://myaccount.blob.core.windows.net"
    )
    _mock_azure_clients(monkeypatch)

    client = AzureBlobClient()

    assert isinstance(client.service_client, MockBlobServiceClient)
    assert client.service_client._account_url == "https://myaccount.blob.core.windows.net"
    assert isinstance(client.service_client._credential, DefaultAzureCredential)

    assert isinstance(client.data_lake_client, MockedDataLakeServiceClient)
    assert client.data_lake_client._account_url == "https://myaccount.dfs.core.windows.net"
    assert isinstance(client.data_lake_client._credential, DefaultAzureCredential)


def test_account_url_env_var_dfs(monkeypatch):
    """AZURE_STORAGE_ACCOUNT_URL env var with .dfs. URL creates both clients."""
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.setenv(
        "AZURE_STORAGE_ACCOUNT_URL", "https://myaccount.dfs.core.windows.net"
    )
    _mock_azure_clients(monkeypatch)

    client = AzureBlobClient()

    assert client.service_client._account_url == "https://myaccount.blob.core.windows.net"
    assert client.data_lake_client._account_url == "https://myaccount.dfs.core.windows.net"


def test_missing_creds_error_no_env_vars(monkeypatch):
    """MissingCredentialsError is still raised when nothing is configured."""
    monkeypatch.delenv("AZURE_STORAGE_CONNECTION_STRING", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_URL", raising=False)
    monkeypatch.setattr(
        cloudpathlib.azure.azblobclient, "DefaultAzureCredential", None
    )
    with pytest.raises(MissingCredentialsError):
        AzureBlobClient()


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

    # discover and use credentials for service principal by having set:
    # AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
    credential = DefaultAzureCredential()
    cl: AzureBlobClient = azure_rigs.client_class(credential=credential, account_url=bsc.url)
    _check_access(cl, gen2=azure_rigs.is_adls_gen2)

    # add basic checks for gen2 to exercise limited-privilege access scenarios
    p = azure_rigs.create_cloud_path("new_dir/new_file.txt", client=cl)
    assert cl._check_hns(p) == azure_rigs.is_adls_gen2
    p.write_text("Hello")
    assert p.exists()
    assert p.read_text() == "Hello"
    assert list(p.parent.iterdir()) == [p]


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


def test_batched_rmtree_no_hns(azure_rig):
    p = azure_rig.create_cloud_path("new_dir")

    p.mkdir()
    for i in range(400):
        (p / f"{i}.txt").write_text("content")
    p.rmtree()
    assert not p.exists()
