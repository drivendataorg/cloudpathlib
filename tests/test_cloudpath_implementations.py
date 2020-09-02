import pytest
from cloudpathlib import AzureBlobClient, AzureBlobPath, S3Client, S3Path
import cloudpathlib.azure.azblobclient
import cloudpathlib.s3.s3client
from .mock_clients.mock_azureblob import MockBlobServiceClient
from .mock_clients.mock_s3 import MockBoto3Session

from types import ModuleType
from typing import Any, Tuple


class CloudProviderTestRig:
    """Class that holds together the components needed to test a cloud implementation."""

    def __init__(
        self, path_class: type, client_class: type, monkeypatch_args: Tuple[ModuleType, str, Any]
    ):
        """
        Args:
            path_class (type): CloudPath subclass
            client_class (type): Client subclass
            monkeypatch_args (Tuple[ModuleType, str, Any]): tuple of (module to patch, name
            of object to patch, object to patch in)
        """
        self.path_class = path_class
        self.client_class = client_class
        self.monkeypatch_args = monkeypatch_args

    def create_cloud_path(self, path: str):
        """CloudPath constructor that appends cloud prefix and drive. Use this to instantiate
        cloud path instances with generic paths."""
        return self.path_class(cloud_path=self.path_class.cloud_prefix + "drive/" + path)


test_rigs = {
    "azure": CloudProviderTestRig(
        path_class=AzureBlobPath,
        client_class=AzureBlobClient,
        monkeypatch_args=(
            cloudpathlib.azure.azblobclient,
            "BlobServiceClient",
            MockBlobServiceClient,
        ),
    ),
    "s3": CloudProviderTestRig(
        path_class=S3Path,
        client_class=S3Client,
        monkeypatch_args=(cloudpathlib.s3.s3client, "Session", MockBoto3Session),
    ),
}


@pytest.fixture(params=test_rigs.values(), ids=test_rigs.keys())
def rig(request, monkeypatch):
    """Parametrized pytest fixture that sets up for generic tests."""
    rig = request.param

    monkeypatch.setattr(*rig.monkeypatch_args)  # monkeypatch cloud SDK
    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client


def test_file_io(rig):
    p2 = rig.create_cloud_path("dir_0/not_a_file")
    assert not p2.exists()
    p2.touch()
    assert p2.exists()
    p2.unlink()
    assert not p2.exists()
