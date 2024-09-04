import mimetypes
import os
import random
import string
from pathlib import Path

import pytest

from cloudpathlib import CloudPath
from cloudpathlib.client import register_client_class
from cloudpathlib.cloudpath import implementation_registry, register_path_class
from cloudpathlib.s3.s3client import S3Client
from cloudpathlib.s3.s3path import S3Path


def test_default_client_instantiation(rig):
    if not getattr(rig, "is_custom_s3", False) and not (getattr(rig, "is_adls_gen2", False)):
        # Skip resetting the default client for custom S3 endpoint, but keep the other tests,
        # since they're still useful.
        rig.client_class._default_client = None

    # CloudPath dispatch
    p = CloudPath(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt")
    # Explicit class
    p2 = rig.create_cloud_path("dir_0/file0_0.txt")
    # Default client CloudPath constructor
    p3 = rig.client_class.get_default_client().CloudPath(
        f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt"
    )
    # Default client path-class-name constructor
    p4 = getattr(rig.client_class.get_default_client(), rig.path_class.__name__)(
        f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt"
    )

    # Check that paths are the same
    assert p == p2 == p3 == p4

    # Check that client is the same instance
    assert p.client is p2.client is p3.client is p4.client

    # Check the file content is the same
    assert p.read_bytes() == p2.read_bytes() == p3.read_bytes() == p4.read_bytes()

    # should be using same instance of client, so cache should be the same
    assert p._local == p2._local == p3._local == p4._local


def test_different_clients(rig):
    p = rig.create_cloud_path("dir_0/file0_0.txt")

    new_client = rig.client_class(**rig.required_client_kwargs)
    p2 = new_client.CloudPath(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt")

    assert p.client is not p2.client
    assert p._local is not p2._local


def test_content_type_setting(rig, tmpdir):
    random.seed(1337)  # reproducible file names

    mimes = [
        (".css", "text/css"),
        (".html", "text/html"),
        (
            ".js",
            ["application/javascript", "text/javascript"],
        ),  # JS type can be different on different platforms
        (".mp3", "audio/mpeg"),
        (".mp4", "video/mp4"),
        (".jpeg", "image/jpeg"),
        (".png", "image/png"),
    ]

    def _test_write_content_type(suffix, expected, rig_ref, check=True):
        filename = "".join(random.choices(string.ascii_letters, k=8)) + suffix
        filepath = Path(tmpdir / filename)
        filepath.write_text("testing")

        cp = rig_ref.create_cloud_path(filename)
        cp.upload_from(filepath)

        meta = cp.client._get_metadata(cp)

        if check:
            if isinstance(expected, list):
                assert meta["content_type"] in expected
            else:
                assert meta["content_type"] == expected

    # should guess by default
    for suffix, content_type in mimes:
        _test_write_content_type(suffix, content_type, rig)

    # None does whatever library default is; not checked, just ensure
    # we don't throw an error
    for suffix, content_type in mimes:
        _test_write_content_type(suffix, content_type, rig, check=False)

    # custom mime type method
    def my_content_type(path):
        # do lookup for content types I define; fallback to
        # guess_type for anything else
        return {
            ".potato": ("application/potato", None),
        }.get(Path(path).suffix, mimetypes.guess_type(path))

    mimes.append((".potato", "application/potato"))

    # see if testing custom s3 endpoint, make sure to pass the url to the constructor
    kwargs = rig.required_client_kwargs.copy()
    custom_endpoint = os.getenv("CUSTOM_S3_ENDPOINT", "https://s3.us-west-1.drivendatabws.com")
    if (
        rig.client_class is S3Client
        and rig.live_server
        and custom_endpoint in rig.create_cloud_path("").client.client._endpoint.host
    ):
        kwargs["endpoint_url"] = custom_endpoint

    # set up default client to use content_type_method
    rig.client_class(content_type_method=my_content_type, **kwargs).set_as_default_client()

    for suffix, content_type in mimes:
        _test_write_content_type(suffix, content_type, rig)


@pytest.fixture
def custom_s3_path():
    # A fixture isolates these classes as they modify the global registry of
    # implementations.
    @register_path_class("mys3")
    class MyS3Path(S3Path):
        cloud_prefix: str = "mys3://"

    @register_client_class("mys3")
    class MyS3Client(S3Client):
        pass

    yield (MyS3Path, MyS3Client)

    # cleanup after use
    implementation_registry.pop("mys3")


def test_custom_mys3path_instantiation(custom_s3_path):
    CustomPath, _ = custom_s3_path

    path = CustomPath("mys3://bucket/dir/file.txt")
    assert isinstance(path, CustomPath)
    assert path.cloud_prefix == "mys3://"
    assert path.bucket == "bucket"
    assert path.key == "dir/file.txt"


def test_custom_mys3client_instantiation(custom_s3_path):
    _, CustomClient = custom_s3_path

    client = CustomClient()
    assert isinstance(client, CustomClient)
    assert client.CloudPath("mys3://bucket/dir/file.txt").cloud_prefix == "mys3://"


def test_custom_mys3client_default_client(custom_s3_path):
    _, CustomClient = custom_s3_path

    CustomClient().set_as_default_client()

    path = CloudPath("mys3://bucket/dir/file.txt")
    assert isinstance(path.client, CustomClient)
    assert path.cloud_prefix == "mys3://"
