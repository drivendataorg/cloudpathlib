from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache, wraps
import hashlib
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
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)
import boto3
import botocore
from dotenv import find_dotenv, load_dotenv
from google.cloud import storage as google_storage
import pytest
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
from .rigs import ALL_RIGS, custom_s3_endpoint, NETWORK_RIGS
from .utils import _sync_filesystem, getenv

USE_LIVE_CLOUD = getenv("USE_LIVE_CLOUD") == "1"

if USE_LIVE_CLOUD:
    load_dotenv(find_dotenv())


SESSION_UUID = uuid()

# ignore these files when uploading test assets
UPLOAD_IGNORE_LIST = [
    ".DS_Store",  # macOS cruft
]

# threads used by fixtures for independent requests (asset uploads, cleanup)
FIXTURE_IO_THREADS = 8


def _parse_selected_rigs():
    """Rig names to run, from the `CLOUDPATHLIB_TEST_RIGS` environment variable.

    `CLOUDPATHLIB_TEST_RIGS` is a comma-separated list of rig names (e.g., `s3,gs`). When it
    is not set, live runs use the network-backed rigs only and mocked runs use every rig.
    """
    selection = getenv("CLOUDPATHLIB_TEST_RIGS", "").strip()

    if not selection:
        return list(NETWORK_RIGS if USE_LIVE_CLOUD else ALL_RIGS)

    requested = [name.strip() for name in selection.split(",") if name.strip()]
    unknown = [name for name in requested if name not in ALL_RIGS]

    if unknown:
        raise pytest.UsageError(
            f"CLOUDPATHLIB_TEST_RIGS contains unknown rig name(s): {', '.join(unknown)}. "
            f"Valid rig names are: {', '.join(ALL_RIGS)}."
        )

    # keep the canonical ordering so test ids don't depend on how the variable was written
    return [name for name in ALL_RIGS if name in requested]


SELECTED_RIGS = _parse_selected_rigs()

# Credentials for the session-scoped provider fixtures are read once, at import time, so that
# the per-test AWS environment variable patching done by `custom_s3_rig` cannot change which
# account the AWS S3 fixtures point at.
AWS_S3_SESSION_KWARGS = {
    key: value
    for key, value in (
        ("aws_access_key_id", getenv("AWS_ACCESS_KEY_ID")),
        ("aws_secret_access_key", getenv("AWS_SECRET_ACCESS_KEY")),
        ("aws_session_token", getenv("AWS_SESSION_TOKEN")),
        ("profile_name", getenv("AWS_PROFILE")),
    )
    if value
}

CUSTOM_S3_SESSION_KWARGS = {
    key: value
    for key, value in (
        ("aws_access_key_id", getenv("CUSTOM_S3_KEY_ID")),
        ("aws_secret_access_key", getenv("CUSTOM_S3_SECRET_KEY")),
    )
    if value
}


def _require_rig(rig_name: str) -> None:
    """Skip a test that asks for a rig directly if that rig was not selected."""
    if rig_name not in SELECTED_RIGS:
        pytest.skip(f"Rig '{rig_name}' not selected by CLOUDPATHLIB_TEST_RIGS.")


def _seed_assets_for(request) -> bool:
    """Whether this test needs the five baseline files copied into its isolated directory."""
    return request.node.get_closest_marker("no_seed_assets") is None


def _chunked(items, size):
    """Yield lists of up to `size` items."""
    items = list(items)
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _map_in_parallel(fn, items):
    """Call `fn` on each item in a thread pool, raising the first exception it raises."""
    items = list(items)

    if not items:
        return

    with ThreadPoolExecutor(max_workers=min(FIXTURE_IO_THREADS, len(items))) as executor:
        for _ in executor.map(fn, items):
            pass


def _upload_test_assets(assets_dir, test_dir, upload_file) -> None:
    """Upload the test assets into `test_dir` on a live backend.

    `upload_file` is called with the local file `Path` and the destination key. Each call
    writes a distinct key, so they are made in parallel.
    """
    test_files = [
        f for f in assets_dir.glob("**/*") if f.is_file() and f.name not in UPLOAD_IGNORE_LIST
    ]

    def _upload(test_file):
        upload_file(test_file, f"{test_dir}/{PurePosixPath(test_file.relative_to(assets_dir))}")

    _map_in_parallel(_upload, test_files)


@fixture()
def assets_dir() -> Path:
    """Path to test assets directory."""
    return Path(__file__).parent / "assets"


@fixture()
def live_server() -> bool:
    """Whether to use a live server."""
    return USE_LIVE_CLOUD


@pytest.fixture(scope="session")
def azure_service_clients():
    """Getter for the live Azure service clients, built at most once per session.

    (Session-scoped fixtures are per-worker when running under pytest-xdist.) The returned
    getter takes a connection string and also reports whether that storage account has a
    hierarchical namespace, which rig teardown needs; probing it costs a request, so the
    result is cached rather than looked up for every test.
    """

    @lru_cache(maxsize=None)
    def _get(connection_string):
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        data_lake_service_client = DataLakeServiceClient.from_connection_string(connection_string)
        is_hns_enabled = blob_service_client.get_account_information().get("is_hns_enabled", False)
        return blob_service_client, data_lake_service_client, is_hns_enabled

    return _get


@pytest.fixture(scope="session")
def gs_bucket():
    """Getter for a live Google Cloud Storage bucket handle, built at most once per session."""

    @lru_cache(maxsize=None)
    def _get(drive):
        return google_storage.Client().bucket(drive)

    return _get


@pytest.fixture(scope="session")
def s3_bucket():
    """Getter for a live AWS S3 bucket handle, built at most once per session."""

    @lru_cache(maxsize=None)
    def _get(drive):
        # explicit credentials (see AWS_S3_SESSION_KWARGS) rather than ambient environment
        session = boto3.Session(**AWS_S3_SESSION_KWARGS)
        return session.resource("s3").Bucket(drive)

    return _get


@pytest.fixture(scope="session")
def custom_s3_bucket():
    """Getter for a live custom S3 bucket handle, built at most once per session.

    Our custom S3 test server only has ephemeral storage and may need to wake up, so this
    also does the retrying head_bucket/create_bucket dance—once per session instead of once
    per test.
    """

    @lru_cache(maxsize=None)
    def _get(drive, endpoint_url):
        # explicit credentials (see CUSTOM_S3_SESSION_KWARGS) rather than ambient environment
        session = boto3.Session(**CUSTOM_S3_SESSION_KWARGS)
        s3 = session.resource("s3", endpoint_url=endpoint_url)

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

        return s3.Bucket(drive)

    return _get


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

    # parametrized tests share a function name, so add a short digest of the full node name
    # to keep each test's directory unique (they may run concurrently under pytest-xdist)
    node_name = request.node.name
    suffix = (
        ""
        if node_name == function_name
        else "-" + hashlib.sha1(node_name.encode("utf-8")).hexdigest()[:6]
    )

    test_dir = f"{SESSION_UUID}-{module_name}-{function_name}{suffix}"
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


def _azure_fixture(
    conn_str_env_var,
    adls_gen2,
    request,
    monkeypatch,
    assets_dir,
    live_server,
    azure_service_clients,
):
    drive = (
        getenv("LIVE_AZURE_CONTAINER", DEFAULT_CONTAINER_NAME)
        if live_server
        else DEFAULT_CONTAINER_NAME
    )

    test_dir = create_test_dir_name(request)

    connection_kwargs = dict()
    tmpdir = TemporaryDirectory()

    if live_server:
        connection_string = getenv(conn_str_env_var)

        blob_service_client, data_lake_service_client, is_hns_enabled = azure_service_clients(
            connection_string
        )

        # Set up test assets
        def _upload(test_file, key):
            blob_service_client.get_blob_client(container=drive, blob=key).upload_blob(
                test_file.read_bytes(), overwrite=True
            )

        if _seed_assets_for(request):
            _upload_test_assets(assets_dir, test_dir, _upload)

        connection_kwargs["connection_string"] = connection_string
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
    rig.connection_string = getenv(conn_str_env_var)  # used for client instantiation tests

    yield rig

    rig.client_class._default_client = None  # reset default client

    if live_server:
        if is_hns_enabled:
            try:
                _hns_rmtree(data_lake_service_client, drive, test_dir)
            except ResourceNotFoundError:
                # A test without seed assets may leave no directory to remove.
                pass

        else:
            # Clean up test dir
            container_client = blob_service_client.get_container_client(drive)
            to_delete = container_client.list_blobs(name_starts_with=test_dir)
            to_delete = sorted(to_delete, key=lambda b: len(b.name.split("/")), reverse=True)

            # delete_blobs is a batch request, and the service caps a batch at 256 blobs
            for chunk in _chunked(to_delete, 256):
                container_client.delete_blobs(*chunk)

    else:
        tmpdir.cleanup()


@fixture()
def azure_rig(request, monkeypatch, assets_dir, live_server, azure_service_clients):
    _require_rig("azure")
    yield from _azure_fixture(
        "AZURE_STORAGE_CONNECTION_STRING",
        False,
        request,
        monkeypatch,
        assets_dir,
        live_server,
        azure_service_clients,
    )


@fixture()
def azure_gen2_rig(request, monkeypatch, assets_dir, live_server, azure_service_clients):
    _require_rig("azure_gen2")
    yield from _azure_fixture(
        "AZURE_STORAGE_GEN2_CONNECTION_STRING",
        True,
        request,
        monkeypatch,
        assets_dir,
        live_server,
        azure_service_clients,
    )


@fixture()
def gs_rig(request, monkeypatch, assets_dir, live_server, gs_bucket):
    _require_rig("gs")

    drive = (
        getenv("LIVE_GS_BUCKET", DEFAULT_GS_BUCKET_NAME) if live_server else DEFAULT_GS_BUCKET_NAME
    )
    test_dir = create_test_dir_name(request)

    if live_server:
        bucket = gs_bucket(drive)

        # Set up test assets
        def _upload(test_file, key):
            google_storage.Blob(key, bucket).upload_from_filename(str(test_file))

        if _seed_assets_for(request):
            _upload_test_assets(assets_dir, test_dir, _upload)
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
        # Clean up test dir; batch the deletes (the API allows 100 subrequests per batch)
        # instead of issuing a request per blob
        for chunk in _chunked(bucket.list_blobs(prefix=test_dir), 100):
            with bucket.client.batch():
                for blob in chunk:
                    blob.delete()


@fixture()
def s3_rig(request, monkeypatch, assets_dir, live_server, s3_bucket):
    _require_rig("s3")

    drive = (
        getenv("LIVE_S3_BUCKET", DEFAULT_S3_BUCKET_NAME) if live_server else DEFAULT_S3_BUCKET_NAME
    )

    test_dir = create_test_dir_name(request)

    if live_server:
        bucket = s3_bucket(drive)

        # Set up test assets; upload via the (thread-safe) client rather than the resource
        def _upload(test_file, key):
            bucket.meta.client.upload_file(str(test_file), drive, key)

        if _seed_assets_for(request):
            _upload_test_assets(assets_dir, test_dir, _upload)
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
def custom_s3_rig(request, monkeypatch, assets_dir, live_server, custom_s3_bucket):
    """
    Custom S3 rig used to test the integrations with non-AWS S3-compatible object storages like
        - MinIO (https://min.io/)
        - CEPH  (https://ceph.io/ceph-storage/object-storage/)
        - others
    """
    _require_rig("custom_s3")

    drive = (
        getenv("CUSTOM_S3_BUCKET", DEFAULT_S3_BUCKET_NAME)
        if live_server
        else DEFAULT_S3_BUCKET_NAME
    )

    test_dir = create_test_dir_name(request)
    custom_endpoint_url = custom_s3_endpoint()

    if live_server:
        # the client under test picks these up from the environment
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", getenv("CUSTOM_S3_KEY_ID"))
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", getenv("CUSTOM_S3_SECRET_KEY"))

        bucket = custom_s3_bucket(drive, custom_endpoint_url)

        # Upload test assets; upload via the (thread-safe) client rather than the resource
        def _upload(test_file, key):
            bucket.meta.client.upload_file(str(test_file), drive, key)

        if _seed_assets_for(request):
            _upload_test_assets(assets_dir, test_dir, _upload)
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
    _require_rig("local_azure")

    drive = (
        getenv("LIVE_AZURE_CONTAINER", DEFAULT_CONTAINER_NAME)
        if live_server
        else DEFAULT_CONTAINER_NAME
    )

    test_dir = create_test_dir_name(request)

    # copy test assets
    if _seed_assets_for(request):
        shutil.copytree(
            assets_dir, LocalAzureBlobClient.get_default_storage_dir() / drive / test_dir
        )

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
    _require_rig("local_gs")

    drive = (
        getenv("LIVE_GS_BUCKET", DEFAULT_GS_BUCKET_NAME) if live_server else DEFAULT_GS_BUCKET_NAME
    )

    test_dir = create_test_dir_name(request)

    # copy test assets
    if _seed_assets_for(request):
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
    _require_rig("local_s3")

    drive = (
        getenv("LIVE_S3_BUCKET", DEFAULT_S3_BUCKET_NAME) if live_server else DEFAULT_S3_BUCKET_NAME
    )

    test_dir = create_test_dir_name(request)

    # copy test assets
    if _seed_assets_for(request):
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
    _require_rig("http")

    test_dir = create_test_dir_name(request)

    host, server_dir = http_server
    drive = urlparse(host).netloc

    # copy test assets
    if _seed_assets_for(request):
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
    shutil.rmtree(server_dir / test_dir, ignore_errors=True)
    _sync_filesystem()


@fixture()
def https_rig(request, assets_dir, https_server):  # noqa: F811
    _require_rig("https")

    test_dir = create_test_dir_name(request)

    host, server_dir = https_server
    drive = urlparse(host).netloc

    # copy test assets
    if _seed_assets_for(request):
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
    shutil.rmtree(server_dir / test_dir, ignore_errors=True)
    _sync_filesystem()


RIG_FIXTURES = {
    "azure": azure_rig,
    "azure_gen2": azure_gen2_rig,
    "gs": gs_rig,
    "s3": s3_rig,
    "custom_s3": custom_s3_rig,
    "local_azure": local_azure_rig,
    "local_s3": local_s3_rig,
    "local_gs": local_gs_rig,
    "http": http_rig,
    "https": https_rig,
}


def _rig_union(union_name, rig_names):
    """Fixture union over the rigs in `rig_names` that were selected for this run.

    pytest_cases cannot build an empty union, so if the selection leaves no rigs for this
    union, register a fixture that skips instead. Assign the result to a module-level name
    matching `union_name`, as with `fixture_union`.
    """
    selected = [RIG_FIXTURES[name] for name in rig_names if name in SELECTED_RIGS]

    if not selected:

        @pytest.fixture(name=union_name)
        def _no_selected_rigs():
            pytest.skip(f"No rigs for '{union_name}' selected by CLOUDPATHLIB_TEST_RIGS.")

        return _no_selected_rigs

    return fixture_union(union_name, selected)


# create azure fixtures for both blob and gen2 storage
azure_rigs = _rig_union("azure_rigs", ["azure", "azure_gen2"])


rig = _rig_union("rig", ALL_RIGS)

# run some s3-specific tests on custom s3 (ceph, minio, etc.) and aws s3
s3_like_rig = _rig_union("s3_like_rig", ["s3", "custom_s3"])

# run some http-specific tests on http and https
http_like_rig = _rig_union("http_like_rig", ["http", "https"])
