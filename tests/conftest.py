from pytest_cases import fixture, fixture_union

from cloudpathlib import AzureBlobClient, AzureBlobPath, S3Client, S3Path
import cloudpathlib.azure.azblobclient
import cloudpathlib.s3.s3client
from .mock_clients.mock_azureblob import MockBlobServiceClient
from .mock_clients.mock_s3 import MockBoto3Session


class CloudProviderTestRig:
    """Class that holds together the components needed to test a cloud implementation."""

    def __init__(self, path_class: type, client_class: type):
        """
        Args:
            path_class (type): CloudPath subclass
            client_class (type): Client subclass
        """
        self.path_class = path_class
        self.client_class = client_class

    @property
    def cloud_prefix(self):
        return self.path_class.cloud_prefix

    def create_cloud_path(self, path: str):
        """CloudPath constructor that appends cloud prefix. Use this to instantiate
        cloud path instances with generic paths."""
        return self.path_class(cloud_path=self.path_class.cloud_prefix + path)


@fixture()
def azure_rig(request, monkeypatch):
    # Mock cloud SDK
    monkeypatch.setattr(
        cloudpathlib.azure.azblobclient,
        "BlobServiceClient",
        MockBlobServiceClient,
    )

    rig = CloudProviderTestRig(path_class=AzureBlobPath, client_class=AzureBlobClient)

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client


@fixture()
def s3_rig(request, monkeypatch):
    # Mock cloud SDK
    monkeypatch.setattr(
        cloudpathlib.s3.s3client,
        "Session",
        MockBoto3Session,
    )

    rig = CloudProviderTestRig(path_class=S3Path, client_class=S3Client)

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client


rig = fixture_union(
    "rig",
    [
        azure_rig,
        s3_rig,
    ],
)
