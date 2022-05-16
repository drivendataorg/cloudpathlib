import mimetypes
from pathlib import Path
import random
import string

from cloudpathlib import CloudPath


def test_default_client_instantiation(rig):
    if not getattr(rig, "is_custom_s3", False):
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

    new_client = rig.client_class()
    p2 = new_client.CloudPath(f"{rig.cloud_prefix}{rig.drive}/{rig.test_dir}/dir_0/file0_0.txt")

    assert p.client is not p2.client
    assert p._local is not p2._local


def test_content_type_setting(rig, tmpdir):
    random.seed(1337)  # reproducible file names

    mimes = [
        (".css", "text/css"),
        (".html", "text/html"),
        (".js", "application/javascript"),
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
            assert meta["content_type"] == expected

    # should guess by default
    for suffix, content_type in mimes:
        _test_write_content_type(suffix, content_type, rig)

    # None does whatever library default is; not checked, just ensure
    # we don't throw an error
    for suffix, content_type in mimes:
        _test_write_content_type(suffix, content_type, rig, check=False)

    # custom mimetype method
    def my_content_type(path):
        # do lookup for content types I define; fallback to
        # guess_type for anything else
        return {
            ".potato": ("application/potato", None),
        }.get(Path(path).suffix, mimetypes.guess_type(path))

    mimes.append((".potato", "application/potato"))

    # update rig to use custom client
    rig.client_class(content_type_method=my_content_type).set_as_default_client()

    for suffix, content_type in mimes:
        _test_write_content_type(suffix, content_type, rig)
