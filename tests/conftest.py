import os
from pathlib import Path, PurePosixPath
import shutil
from tempfile import TemporaryDirectory
from typing import Dict, Optional

from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
import boto3
import botocore
from dotenv import find_dotenv, load_dotenv
from google.cloud import storage as google_storage
from pytest_cases import fixture, fixture_union
from shortuuid import uuid
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from cloudpathlib import AzureBlobClient, AzureBlobPath, GSClient, GSPath, S3Client, S3Path
from cloudpathlib.cloudpath import implementation_registry
from cloudpathlib.local import (
    local_azure_blob_implementation,
    LocalAzureBlobClient,
    LocalAzureBlobPath,
    local_gs_implementation,
    LocalGSClient,
    LocalGSPath,
    local_s3_implementation,
    LocalS3Client,
    LocalS3Path,
)
import cloudpathlib.azure.azblobclient
from cloudpathlib.azure.azblobclient import _hns_rmtree
import cloudpathlib.s3.s3client
from .mock_clients.mock_azureblob import MockBlobServiceClient, DEFAULT_CONTAINER_NAME
from .mock_clients.mock_adls_gen2 import MockedDataLakeServiceClient
from .mock_clients.mock_gs import (
    mocked_client_class_factory as mocked_gsclient_class_factory,
    DEFAULT_GS_BUCKET_NAME,
    MockTransferManager,
)
from .mock_clients.mock_s3 import mocked_session_class_factory, DEFAULT_S3_BUCKET_NAME


if os.getenv("USE_LIVE_CLOUD") == "1":
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
        self,
        path_class: type,
        client_class: type,
        drive: str = "drive",
        test_dir: str = "",
        live_server: bool = False,
        required_client_kwargs: Optional[Dict] = None,
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
        self.live_server = live_server  # if the server is a live server
        self.required_client_kwargs = (
            required_client_kwargs if required_client_kwargs is not None else {}
        )

    @property
    def cloud_prefix(self):
        return self.path_class.cloud_prefix

    def create_cloud_path(self, path: str, client=None):
        """CloudPath constructor that appends cloud prefix. Use this to instantiate
        cloud path instances with generic paths. Includes drive and root test_dir already.

        If `client`, use that client to create the path.
        """
        if client:
            return client.CloudPath(
                cloud_path=f"{self.path_class.cloud_prefix}{self.drive}/{self.test_dir}/{path}"
            )
        else:
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


def _azure_fixture(conn_str_env_var, adls_gen2, request, monkeypatch, assets_dir):
    drive = os.getenv("LIVE_AZURE_CONTAINER", DEFAULT_CONTAINER_NAME)
    test_dir = create_test_dir_name(request)

    live_server = os.getenv("USE_LIVE_CLOUD") == "1"

    connection_kwargs = dict()
    tmpdir = TemporaryDirectory()

    if live_server:
        # Set up test assets
        blob_service_client = BlobServiceClient.from_connection_string(os.getenv(conn_str_env_var))
        data_lake_service_client = DataLakeServiceClient.from_connection_string(
            os.getenv(conn_str_env_var)
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

        connection_kwargs["connection_string"] = os.getenv(conn_str_env_var)
    else:
        # pass key mocked params to clients via connection string
        monkeypatch.setenv(
            "AZURE_STORAGE_CONNECTION_STRING", f"{Path(tmpdir.name) / test_dir};{adls_gen2}"
        )
        monkeypatch.setenv("AZURE_STORAGE_GEN2_CONNECTION_STRING", "")

        monkeypatch.setattr(
            cloudpathlib.azure.azblobclient,
            "BlobServiceClient",
            MockBlobServiceClient,
        )

        monkeypatch.setattr(
            cloudpathlib.azure.azblobclient,
            "DataLakeServiceClient",
            MockedDataLakeServiceClient,
        )

    rig = CloudProviderTestRig(
        path_class=AzureBlobPath,
        client_class=AzureBlobClient,
        drive=drive,
        test_dir=test_dir,
        live_server=live_server,
        required_client_kwargs=connection_kwargs,
    )

    rig.client_class(**connection_kwargs).set_as_default_client()  # set default client

    # add flag for adls gen2 rig to skip some tests
    rig.is_adls_gen2 = adls_gen2
    rig.connection_string = os.getenv(conn_str_env_var)  # used for client instantiation tests

    yield rig

    rig.client_class._default_client = None  # reset default client

    if live_server:
        if blob_service_client.get_account_information().get("is_hns_enabled", False):
            _hns_rmtree(data_lake_service_client, drive, test_dir)

        else:
            # Clean up test dir
            container_client = blob_service_client.get_container_client(drive)
            to_delete = container_client.list_blobs(name_starts_with=test_dir)
            to_delete = sorted(to_delete, key=lambda b: len(b.name.split("/")), reverse=True)

            container_client.delete_blobs(*to_delete)

    else:
        tmpdir.cleanup()


@fixture()
def azure_rig(request, monkeypatch, assets_dir):
    yield from _azure_fixture(
        "AZURE_STORAGE_CONNECTION_STRING", False, request, monkeypatch, assets_dir
    )


@fixture()
def azure_gen2_rig(request, monkeypatch, assets_dir):
    yield from _azure_fixture(
        "AZURE_STORAGE_GEN2_CONNECTION_STRING", True, request, monkeypatch, assets_dir
    )


@fixture()
def gs_rig(request, monkeypatch, assets_dir):
    drive = os.getenv("LIVE_GS_BUCKET", DEFAULT_GS_BUCKET_NAME)
    test_dir = create_test_dir_name(request)

    live_server = os.getenv("USE_LIVE_CLOUD") == "1"

    if live_server:
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
        monkeypatch.setattr(
            cloudpathlib.gs.gsclient,
            "transfer_manager",
            MockTransferManager,
        )

    rig = CloudProviderTestRig(
        path_class=GSPath,
        client_class=GSClient,
        drive=drive,
        test_dir=test_dir,
        live_server=live_server,
    )

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client

    if live_server:
        # Clean up test dir
        for blob in bucket.list_blobs(prefix=test_dir):
            blob.delete()


@fixture()
def s3_rig(request, monkeypatch, assets_dir):
    drive = os.getenv("LIVE_S3_BUCKET", DEFAULT_S3_BUCKET_NAME)
    test_dir = create_test_dir_name(request)

    live_server = os.getenv("USE_LIVE_CLOUD") == "1"

    if live_server:
        # Set up test assets
        session = boto3.Session()  # Fresh session to ensure isolation
        bucket = session.resource("s3").Bucket(drive)
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
        path_class=S3Path,
        client_class=S3Client,
        drive=drive,
        test_dir=test_dir,
        live_server=live_server,
    )

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client

    if live_server:
        # Clean up test dir
        bucket.objects.filter(Prefix=test_dir).delete()


@fixture()
def custom_s3_rig(request, monkeypatch, assets_dir):
    """
    Custom S3 rig used to test the integrations with non-AWS S3-compatible object storages like
        - MinIO (https://min.io/)
        - CEPH  (https://ceph.io/ceph-storage/object-storage/)
        - others
    """
    drive = os.getenv("CUSTOM_S3_BUCKET", DEFAULT_S3_BUCKET_NAME)
    test_dir = create_test_dir_name(request)
    custom_endpoint_url = os.getenv("CUSTOM_S3_ENDPOINT", "https://s3.us-west-1.drivendatabws.com")

    live_server = os.getenv("USE_LIVE_CLOUD") == "1"

    if live_server:
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.getenv("CUSTOM_S3_KEY_ID"))
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.getenv("CUSTOM_S3_SECRET_KEY"))

        # Upload test assets
        session = boto3.Session()  # Fresh session to ensure isolation from AWS S3 auth
        s3 = session.resource("s3", endpoint_url=custom_endpoint_url)

        # idempotent and our test server on heroku only has ephemeral storage
        # so we need to try to create each time
        try:
            #  try a few times to spin up the bucket since the heroku worker needs some time to wake up
            @retry(
                stop=stop_after_attempt(5),
                wait=wait_fixed(2),
                retry=retry_if_exception_type(botocore.exceptions.ClientError),
                reraise=True,
            )
            def _spin_up_bucket():
                s3.meta.client.head_bucket(Bucket=drive)

            _spin_up_bucket()
        except botocore.exceptions.ClientError:
            try:
                s3.create_bucket(Bucket=drive)
            except botocore.exceptions.ClientError as e:
                # ok if bucket already exists
                if e.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                    raise

        bucket = s3.Bucket(drive)

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
        path_class=S3Path,
        client_class=S3Client,
        drive=drive,
        test_dir=test_dir,
        live_server=live_server,
        required_client_kwargs=dict(endpoint_url=custom_endpoint_url),
    )

    rig.client_class(
        endpoint_url=custom_endpoint_url
    ).set_as_default_client()  # set default client

    # add flag for custom_s3 rig to skip some tests
    rig.is_custom_s3 = True

    yield rig

    rig.client_class._default_client = None  # reset default client

    if live_server:
        bucket.objects.filter(Prefix=test_dir).delete()


@fixture()
def local_azure_rig(request, monkeypatch, assets_dir):
    drive = os.getenv("LIVE_AZURE_CONTAINER", DEFAULT_CONTAINER_NAME)
    test_dir = create_test_dir_name(request)

    # copy test assets
    shutil.copytree(assets_dir, LocalAzureBlobClient.get_default_storage_dir() / drive / test_dir)

    monkeypatch.setitem(implementation_registry, "azure", local_azure_blob_implementation)

    rig = CloudProviderTestRig(
        path_class=LocalAzureBlobPath,
        client_class=LocalAzureBlobClient,
        drive=drive,
        test_dir=test_dir,
    )

    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "")
    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client
    rig.client_class.reset_default_storage_dir()  # reset local storage directory


@fixture()
def local_gs_rig(request, monkeypatch, assets_dir):
    drive = os.getenv("LIVE_GS_BUCKET", DEFAULT_GS_BUCKET_NAME)
    test_dir = create_test_dir_name(request)

    # copy test assets
    shutil.copytree(assets_dir, LocalGSClient.get_default_storage_dir() / drive / test_dir)

    monkeypatch.setitem(implementation_registry, "gs", local_gs_implementation)

    rig = CloudProviderTestRig(
        path_class=LocalGSPath,
        client_class=LocalGSClient,
        drive=drive,
        test_dir=test_dir,
    )

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client
    rig.client_class.reset_default_storage_dir()  # reset local storage directory


@fixture()
def local_s3_rig(request, monkeypatch, assets_dir):
    drive = os.getenv("LIVE_S3_BUCKET", DEFAULT_S3_BUCKET_NAME)
    test_dir = create_test_dir_name(request)

    # copy test assets
    shutil.copytree(assets_dir, LocalS3Client.get_default_storage_dir() / drive / test_dir)

    monkeypatch.setitem(implementation_registry, "s3", local_s3_implementation)

    rig = CloudProviderTestRig(
        path_class=LocalS3Path,
        client_class=LocalS3Client,
        drive=drive,
        test_dir=test_dir,
    )

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client
    rig.client_class.reset_default_storage_dir()  # reset local storage directory


# create azure fixtures for both blob and gen2 storage
azure_rigs = fixture_union(
    "azure_rigs",
    [
        azure_rig,  # azure_rig0
        azure_gen2_rig,  # azure_rig1
    ],
)

rig = fixture_union(
    "rig",
    [
        azure_rig,  # azure_rig0
        azure_gen2_rig,  # azure_rig1
        gs_rig,
        s3_rig,
        custom_s3_rig,
        local_azure_rig,
        local_s3_rig,
        local_gs_rig,
    ],
)

# run some s3-specific tests on custom s3 (ceph, minio, etc.) and aws s3
s3_like_rig = fixture_union(
    "s3_like_rig",
    [
        s3_rig,
        custom_s3_rig,
    ],
)
