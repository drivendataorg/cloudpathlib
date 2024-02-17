import os

import pytest

from urllib.parse import urlparse, parse_qs
from cloudpathlib import AzureBlobClient, AzureBlobPath
from cloudpathlib.exceptions import MissingCredentialsError
from cloudpathlib.local import LocalAzureBlobClient, LocalAzureBlobPath


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


def test_as_url(azure_rig):
    p: AzureBlobPath = azure_rig.create_cloud_path("dir_0/file0_0.txt")

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
