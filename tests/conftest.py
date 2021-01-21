import os
from pathlib import Path, PurePosixPath

from azure.storage.blob import BlobServiceClient
import boto3
from dotenv import find_dotenv, load_dotenv
from google.cloud import storage as google_storage
from pytest_cases import fixture, fixture_union
from shortuuid import uuid

from cloudpathlib import AzureBlobClient, AzureBlobPath, GSClient, GSPath, S3Client, S3Path
import cloudpathlib.azure.azblobclient
import cloudpathlib.s3.s3client
from .mock_clients.mock_azureblob import mocked_client_class_factory
from .mock_clients.mock_gs import mocked_client_class_factory as mocked_gsclient_class_factory
from .mock_clients.mock_s3 import mocked_session_class_factory


load_dotenv(find_dotenv())


SESSION_UUID = uuid()

# ignore these files when uploading test assets
UPLOAD_IGNORE_LIST = [
    ".DS_Store",  # macOS cruft
]


@fixture()
def assets_dir() -> Path:
    """Path to test assets directory."""
    return Path(__file__).parent / "assets"


class CloudProviderTestRig:
    """Class that holds together the components needed to test a cloud implementation."""

    def __init__(
        self, path_class: type, client_class: type, drive: str = "drive", test_dir: str = ""
    ):
        """
        Args:
            path_class (type): CloudPath subclass
            client_class (type): Client subclass
        """
        self.path_class = path_class
        self.client_class = client_class
        self.drive = drive
        self.test_dir = test_dir

    @property
    def cloud_prefix(self):
        return self.path_class.cloud_prefix

    def create_cloud_path(self, path: str):
        """CloudPath constructor that appends cloud prefix. Use this to instantiate
        cloud path instances with generic paths. Includes drive and root test_dir already."""
        return self.path_class(
            cloud_path=f"{self.path_class.cloud_prefix}{self.drive}/{self.test_dir}/{path}"
        )


def create_test_dir_name(request) -> str:
    """Generates unique test directory name using test module and test function names."""
    module_name = request.module.__name__.rpartition(".")[-1]
    function_name = request.function.__name__
    test_dir = f"{SESSION_UUID}-{module_name}-{function_name}"
    print("Test directory name is:", test_dir)
    return test_dir


@fixture()
def azure_rig(request, monkeypatch, assets_dir):
    drive = os.getenv("LIVE_AZURE_CONTAINER", "container")
    test_dir = create_test_dir_name(request)

    if os.getenv("USE_LIVE_CLOUD") == "1":
        # Set up test assets
        blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        )
        test_files = [
            f for f in assets_dir.glob("**/*") if f.is_file() and f.name not in UPLOAD_IGNORE_LIST
        ]
        for test_file in test_files:
            blob_client = blob_service_client.get_blob_client(
                container=drive,
                blob=str(f"{test_dir}/{PurePosixPath(test_file.relative_to(assets_dir))}"),
            )
            blob_client.upload_blob(test_file.read_bytes(), overwrite=True)
    else:
        # Mock cloud SDK
        monkeypatch.setattr(
            cloudpathlib.azure.azblobclient,
            "BlobServiceClient",
            mocked_client_class_factory(test_dir),
        )

    rig = CloudProviderTestRig(
        path_class=AzureBlobPath,
        client_class=AzureBlobClient,
        drive=drive,
        test_dir=test_dir,
    )

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client

    if os.getenv("USE_LIVE_CLOUD") == "1":
        # Clean up test dir
        container_client = blob_service_client.get_container_client(drive)
        to_delete = container_client.list_blobs(name_starts_with=test_dir)
        container_client.delete_blobs(*to_delete)


@fixture()
def gs_rig(request, monkeypatch, assets_dir):
    drive = os.getenv("LIVE_GS_BUCKET", "bucket")
    test_dir = create_test_dir_name(request)

    if os.getenv("USE_LIVE_CLOUD") == "1":
        # Set up test assets
        bucket = google_storage.Client().bucket(drive)
        test_files = [
            f for f in assets_dir.glob("**/*") if f.is_file() and f.name not in UPLOAD_IGNORE_LIST
        ]
        for test_file in test_files:
            blob = google_storage.Blob(
                str(f"{test_dir}/{PurePosixPath(test_file.relative_to(assets_dir))}"),
                bucket,
            )
            blob.upload_from_filename(str(test_file))
    else:
        # Mock cloud SDK
        monkeypatch.setattr(
            cloudpathlib.gs.gsclient,
            "StorageClient",
            mocked_gsclient_class_factory(test_dir),
        )

    rig = CloudProviderTestRig(
        path_class=GSPath, client_class=GSClient, drive=drive, test_dir=test_dir
    )

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client

    if os.getenv("USE_LIVE_CLOUD") == "1":
        # Clean up test dir
        for blob in bucket.list_blobs(prefix=test_dir):
            blob.delete()


@fixture()
def s3_rig(request, monkeypatch, assets_dir):
    drive = os.getenv("LIVE_S3_BUCKET", "bucket")
    test_dir = create_test_dir_name(request)

    if os.getenv("USE_LIVE_CLOUD") == "1":
        # Set up test assets
        bucket = boto3.resource("s3").Bucket(drive)
        test_files = [
            f for f in assets_dir.glob("**/*") if f.is_file() and f.name not in UPLOAD_IGNORE_LIST
        ]
        for test_file in test_files:
            bucket.upload_file(
                str(test_file),
                str(f"{test_dir}/{PurePosixPath(test_file.relative_to(assets_dir))}"),
            )
    else:
        # Mock cloud SDK
        monkeypatch.setattr(
            cloudpathlib.s3.s3client,
            "Session",
            mocked_session_class_factory(test_dir),
        )

    rig = CloudProviderTestRig(
        path_class=S3Path, client_class=S3Client, drive=drive, test_dir=test_dir
    )

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client

    if os.getenv("USE_LIVE_CLOUD") == "1":
        # Clean up test dir
        bucket.objects.filter(Prefix=test_dir).delete()


rig = fixture_union("rig", [azure_rig, gs_rig, s3_rig])
