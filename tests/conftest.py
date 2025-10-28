from functools import wraps
import os
from pathlib import Path, PurePosixPath
import shutil
import ssl
import time
from tempfile import TemporaryDirectory
from typing import Dict, Optional
from urllib.parse import urlparse
from urllib.request import HTTPSHandler

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

from cloudpathlib.azure import AzureBlobClient, AzureBlobPath, _AzureBlobStorageRaw
from cloudpathlib.gs import GSClient, GSPath, _GSStorageRaw
from cloudpathlib.s3 import S3Client, S3Path, _S3StorageRaw
from cloudpathlib.cloudpath import implementation_registry, CloudImplementation
from cloudpathlib.http.httpclient import HttpClient, HttpsClient
from cloudpathlib.http.httppath import HttpPath, HttpsPath
from cloudpathlib.http.http_io import _HttpStorageRaw
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
from .http_fixtures import http_server, https_server, utilities_dir  # noqa: F401
from .mock_clients.mock_azureblob import MockBlobServiceClient, DEFAULT_CONTAINER_NAME
from .mock_clients.mock_adls_gen2 import MockedDataLakeServiceClient
from .mock_clients.mock_gs import (
    mocked_client_class_factory as mocked_gsclient_class_factory,
    DEFAULT_GS_BUCKET_NAME,
    MockTransferManager,
    mock_default_auth,
)
from .mock_clients.mock_s3 import mocked_session_class_factory, DEFAULT_S3_BUCKET_NAME
from .utils import _sync_filesystem


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


@fixture()
def live_server() -> bool:
    """Whether to use a live server."""
    return os.getenv("USE_LIVE_CLOUD") == "1"


class CloudProviderTestRig:
    """Class that holds together the components needed to test a cloud implementation."""

    def __init__(
        self,
        cloud_implementation: CloudImplementation,
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
        self.cloud_implementation = cloud_implementation
        self.drive = drive
        self.test_dir = test_dir
        self.live_server = live_server  # if the server is a live server
        self.required_client_kwargs = (
            required_client_kwargs if required_client_kwargs is not None else {}
        )

    @property
    def path_class(self):
        return self.cloud_implementation.path_class

    @property
    def client_class(self):
        return self.cloud_implementation.client_class

    @property
    def raw_io_class(self):
        return self.cloud_implementation.raw_io_class

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


@fixture
def wait_for_mkdir(monkeypatch):
    """Fixture that patches os.mkdir to wait for directory creation for tests that sometimes are flaky."""
    original_mkdir = os.mkdir

    @wraps(original_mkdir)
    def wrapped_mkdir(path, *args, **kwargs):
        result = original_mkdir(path, *args, **kwargs)
        _sync_filesystem()

        start = time.time()

        while not os.path.exists(path) and time.time() - start < 5:
            time.sleep(0.01)
            _sync_filesystem()

        assert os.path.exists(path), f"Directory {path} was not created"
        return result

    monkeypatch.setattr(os, "mkdir", wrapped_mkdir)


def _azure_fixture(conn_str_env_var, adls_gen2, request, monkeypatch, assets_dir, live_server):
    drive = (
        os.getenv("LIVE_AZURE_CONTAINER", DEFAULT_CONTAINER_NAME)
        if live_server
        else DEFAULT_CONTAINER_NAME
    )

    test_dir = create_test_dir_name(request)

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

    azure_blob_implementation = CloudImplementation()
    azure_blob_implementation._client_class = AzureBlobClient
    azure_blob_implementation._path_class = AzureBlobPath
    azure_blob_implementation._raw_io_class = _AzureBlobStorageRaw

    rig = CloudProviderTestRig(
        cloud_implementation=azure_blob_implementation,
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
def azure_rig(request, monkeypatch, assets_dir, live_server):
    yield from _azure_fixture(
        "AZURE_STORAGE_CONNECTION_STRING", False, request, monkeypatch, assets_dir, live_server
    )


@fixture()
def azure_gen2_rig(request, monkeypatch, assets_dir, live_server):
    yield from _azure_fixture(
        "AZURE_STORAGE_GEN2_CONNECTION_STRING", True, request, monkeypatch, assets_dir, live_server
    )


@fixture()
def gs_rig(request, monkeypatch, assets_dir, live_server):
    drive = (
        os.getenv("LIVE_GS_BUCKET", DEFAULT_GS_BUCKET_NAME)
        if live_server
        else DEFAULT_GS_BUCKET_NAME
    )
    test_dir = create_test_dir_name(request)

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
        monkeypatch.setattr(cloudpathlib.gs.gsclient, "google_default_auth", mock_default_auth)

    gs_implementation = CloudImplementation()
    gs_implementation._client_class = GSClient
    gs_implementation._path_class = GSPath
    gs_implementation._raw_io_class = _GSStorageRaw

    rig = CloudProviderTestRig(
        cloud_implementation=gs_implementation,
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
def s3_rig(request, monkeypatch, assets_dir, live_server):
    drive = (
        os.getenv("LIVE_S3_BUCKET", DEFAULT_S3_BUCKET_NAME)
        if live_server
        else DEFAULT_S3_BUCKET_NAME
    )

    test_dir = create_test_dir_name(request)

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

    s3_implementation = CloudImplementation()
    s3_implementation._client_class = S3Client
    s3_implementation._path_class = S3Path
    s3_implementation._raw_io_class = _S3StorageRaw

    rig = CloudProviderTestRig(
        cloud_implementation=s3_implementation,
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
def custom_s3_rig(request, monkeypatch, assets_dir, live_server):
    """
    Custom S3 rig used to test the integrations with non-AWS S3-compatible object storages like
        - MinIO (https://min.io/)
        - CEPH  (https://ceph.io/ceph-storage/object-storage/)
        - others
    """
    drive = (
        os.getenv("CUSTOM_S3_BUCKET", DEFAULT_S3_BUCKET_NAME)
        if live_server
        else DEFAULT_S3_BUCKET_NAME
    )

    test_dir = create_test_dir_name(request)
    custom_endpoint_url = os.getenv("CUSTOM_S3_ENDPOINT", "https://s3.us-west-1.drivendatabws.com")

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

    custom_s3_implementation = CloudImplementation()
    custom_s3_implementation._client_class = S3Client
    custom_s3_implementation._path_class = S3Path
    custom_s3_implementation._raw_io_class = _S3StorageRaw

    rig = CloudProviderTestRig(
        cloud_implementation=custom_s3_implementation,
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
def local_azure_rig(request, monkeypatch, assets_dir, live_server):
    drive = (
        os.getenv("LIVE_AZURE_CONTAINER", DEFAULT_CONTAINER_NAME)
        if live_server
        else DEFAULT_CONTAINER_NAME
    )

    test_dir = create_test_dir_name(request)

    # copy test assets
    shutil.copytree(assets_dir, LocalAzureBlobClient.get_default_storage_dir() / drive / test_dir)

    monkeypatch.setitem(implementation_registry, "azure", local_azure_blob_implementation)

    local_azure_blob_cloud_implementation = CloudImplementation()
    local_azure_blob_cloud_implementation._client_class = LocalAzureBlobClient
    local_azure_blob_cloud_implementation._path_class = LocalAzureBlobPath
    local_azure_blob_cloud_implementation._raw_io_class = _AzureBlobStorageRaw

    rig = CloudProviderTestRig(
        cloud_implementation=local_azure_blob_cloud_implementation,
        drive=drive,
        test_dir=test_dir,
    )

    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "")
    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client
    rig.client_class.reset_default_storage_dir()  # reset local storage directory


@fixture()
def local_gs_rig(request, monkeypatch, assets_dir, live_server):
    drive = (
        os.getenv("LIVE_GS_BUCKET", DEFAULT_GS_BUCKET_NAME)
        if live_server
        else DEFAULT_GS_BUCKET_NAME
    )

    test_dir = create_test_dir_name(request)

    # copy test assets
    shutil.copytree(assets_dir, LocalGSClient.get_default_storage_dir() / drive / test_dir)

    monkeypatch.setitem(implementation_registry, "gs", local_gs_implementation)

    local_gs_cloud_implementation = CloudImplementation()
    local_gs_cloud_implementation._client_class = LocalGSClient
    local_gs_cloud_implementation._path_class = LocalGSPath
    local_gs_cloud_implementation._raw_io_class = _GSStorageRaw

    rig = CloudProviderTestRig(
        cloud_implementation=local_gs_cloud_implementation,
        drive=drive,
        test_dir=test_dir,
    )

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client
    rig.client_class.reset_default_storage_dir()  # reset local storage directory


@fixture()
def local_s3_rig(request, monkeypatch, assets_dir, live_server):
    drive = (
        os.getenv("LIVE_S3_BUCKET", DEFAULT_S3_BUCKET_NAME)
        if live_server
        else DEFAULT_S3_BUCKET_NAME
    )

    test_dir = create_test_dir_name(request)

    # copy test assets
    shutil.copytree(assets_dir, LocalS3Client.get_default_storage_dir() / drive / test_dir)

    monkeypatch.setitem(implementation_registry, "s3", local_s3_implementation)

    local_s3_cloud_implementation = CloudImplementation()
    local_s3_cloud_implementation._client_class = LocalS3Client
    local_s3_cloud_implementation._path_class = LocalS3Path
    local_s3_cloud_implementation._raw_io_class = _S3StorageRaw

    rig = CloudProviderTestRig(
        cloud_implementation=local_s3_implementation,
        drive=drive,
        test_dir=test_dir,
    )

    rig.client_class().set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client
    rig.client_class.reset_default_storage_dir()  # reset local storage directory


class HttpProviderTestRig(CloudProviderTestRig):
    def create_cloud_path(self, path: str, client=None):
        """Http version needs to include netloc as well"""
        if client:
            return client.CloudPath(
                cloud_path=f"{self.path_class.cloud_prefix}{self.drive}/{self.test_dir}/{path}"
            )
        else:
            return self.path_class(
                cloud_path=f"{self.path_class.cloud_prefix}{self.drive}/{self.test_dir}/{path}"
            )


@fixture()
def http_rig(request, assets_dir, http_server):  # noqa: F811
    test_dir = create_test_dir_name(request)

    host, server_dir = http_server
    drive = urlparse(host).netloc

    # copy test assets
    shutil.copytree(assets_dir, server_dir / test_dir)
    _sync_filesystem()

    http_implementation = CloudImplementation()
    http_implementation._client_class = HttpClient
    http_implementation._path_class = HttpPath
    http_implementation._raw_io_class = _HttpStorageRaw

    rig = CloudProviderTestRig(
        cloud_implementation=http_implementation,
        drive=drive,
        test_dir=test_dir,
    )

    rig.http_server_dir = server_dir
    rig.client_class(**rig.required_client_kwargs).set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client
    shutil.rmtree(server_dir)
    _sync_filesystem()


@fixture()
def https_rig(request, assets_dir, https_server):  # noqa: F811
    test_dir = create_test_dir_name(request)

    host, server_dir = https_server
    drive = urlparse(host).netloc

    # copy test assets
    shutil.copytree(assets_dir, server_dir / test_dir)
    _sync_filesystem()

    skip_verify_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    skip_verify_ctx.check_hostname = False
    skip_verify_ctx.load_verify_locations(utilities_dir / "insecure-test.pem")

    https_implementation = CloudImplementation()
    https_implementation._client_class = HttpsClient
    https_implementation._path_class = HttpsPath
    https_implementation._raw_io_class = _HttpStorageRaw

    rig = CloudProviderTestRig(
        cloud_implementation=https_implementation,
        drive=drive,
        test_dir=test_dir,
        required_client_kwargs=dict(
            auth=HTTPSHandler(context=skip_verify_ctx, check_hostname=False)
        ),
    )

    rig.http_server_dir = server_dir
    rig.client_class(**rig.required_client_kwargs).set_as_default_client()  # set default client

    yield rig

    rig.client_class._default_client = None  # reset default client
    shutil.rmtree(server_dir)
    _sync_filesystem()


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
        http_rig,
        https_rig,
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

# run some http-specific tests on http and https
http_like_rig = fixture_union(
    "http_like_rig",
    [
        http_rig,
        https_rig,
    ],
)
