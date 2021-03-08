import os

import pytest

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
