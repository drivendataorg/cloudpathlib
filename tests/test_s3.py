from datetime import datetime
from time import sleep
from unittest import mock

import pytest

from cloudpathlib import S3Client, S3Path
from cloudpathlib import DirectoryNotEmpty, InvalidPrefix

from .mock_clients.mock_s3 import MockBoto3Session


def test_initialize_s3():
    with pytest.raises(TypeError):
        p = S3Path()

    with pytest.raises(InvalidPrefix):
        p = S3Path("NOT_S3_PATH")

    # case insensitive
    cases = ["s3://b/k", "S3://b/k", "s3://b/k.file", "S3://b/k", "s3://b"]

    for expected in cases:
        p = S3Path(expected)
        assert repr(p) == f"S3Path('{expected}')"
        assert str(p) == expected

        assert p._no_prefix == expected.split("://", 1)[-1]

        assert p._url.scheme == expected.split("://", 1)[0].lower()
        assert p._url.netloc == expected.split("://", 1)[-1].split("/")[0]

        assert str(p._path) == expected.split(":/", 1)[-1]


def test_joins():
    assert S3Path("S3://a/b/c/d").name == "d"
    assert S3Path("S3://a/b/c/d.file").name == "d.file"
    assert S3Path("S3://a/b/c/d.file").stem == "d"
    assert S3Path("S3://a/b/c/d.file").suffix == ".file"
    assert S3Path("S3://a/b/c/d.tar.gz").suffixes == [".tar", ".gz"]
    assert str(S3Path("s3://a/b/c/d.file").with_suffix(".png")) == "s3://a/b/c/d.png"

    assert S3Path("s3://a") / "b" == S3Path("s3://a/b")
    assert S3Path("s3://a/b/c/d") / "../../b" == S3Path("s3://a/b/b")

    assert S3Path("s3://a/b/c/d").match("**/c/*")
    assert not S3Path("s3://a/b/c/d").match("**/c")
    assert S3Path("s3://a/b/c/d").match("s3://a/*/c/d")

    assert S3Path("s3://a/b/c/d").anchor == "s3://"
    assert S3Path("s3://a/b/c/d").parent == S3Path("s3://a/b/c")

    assert S3Path("s3://a/b/c/d").parents == [
        S3Path("s3://a/b/c"),
        S3Path("s3://a/b"),
        S3Path("s3://a"),
    ]

    assert S3Path("s3://a").joinpath("b", "c") == S3Path("s3://a/b/c")

    assert S3Path("s3://a/b/c").samefile(S3Path("s3://a/b/c"))

    assert S3Path("s3://a/b/c").as_uri() == "s3://a/b/c"

    assert S3Path("s3://a/b/c/d").parts == ("s3://", "a", "b", "c", "d")


@mock.patch("cloudpathlib.clients.s3.s3client.Session", return_value=MockBoto3Session())
def test_with_mock_s3(mock_boto3, tmp_path):
    # Reset default client
    S3Client().set_as_default_client()

    p = S3Path("s3://bucket/dir_0/file0_0.txt")
    assert p == S3Client.get_default_client().CloudPath("s3://bucket/dir_0/file0_0.txt")
    assert p == S3Client.get_default_client().S3Path("s3://bucket/dir_0/file0_0.txt")

    assert p.exists()

    p2 = S3Path("s3://bucket/dir_0/not_a_file")
    assert not p2.exists()
    p2.touch()
    assert p2.exists()
    p2.unlink()

    p3 = S3Path("s3://bucket/dir_0/")
    assert p3.exists()
    assert len(list(p3.iterdir())) == 3
    assert len(list(p3.glob("**/*"))) == 3

    with pytest.raises(ValueError):
        p3.unlink()

    with pytest.raises(DirectoryNotEmpty):
        p3.rmdir()
    p3.rmtree()
    assert not p3.exists()

    p4 = S3Path("S3://bucket")
    assert p4.exists()
    assert p4.key == ""
    p4 = S3Path("S3://bucket/")

    assert p4.exists()
    assert p4.key == ""

    assert len(list(p4.iterdir())) == 1  # only s3://bucket/dir_1/ should still exist
    assert len(list(p4.glob("**/*"))) == 4
    assert len(list(p4.glob("s3://bucket/**/*"))) == 4

    assert list(p4.glob("**/*")) == list(p4.rglob("*"))

    p.write_text("lalala")
    assert p.read_text() == "lalala"
    p2.write_text("lalala")
    p.write_bytes(p2.read_bytes())
    assert p.read_text() == p2.read_text()

    before_touch = datetime.now()
    sleep(0.1)
    p.touch()
    assert datetime.fromtimestamp(p.stat().st_mtime) > before_touch

    # no-op
    p.mkdir()

    assert p.etag is not None

    dest = S3Path("s3://bucket/dir2/new_file0_0.txt")
    assert not dest.exists()
    p.rename(dest)
    assert dest.exists()

    assert not p.exists()
    p.touch()
    dest.replace(p)
    assert p.exists()

    dl_file = tmp_path / "file"
    p.download_to(dl_file)
    assert dl_file.exists()
    assert p.read_text() == dl_file.read_text()

    dl_dir = tmp_path / "directory"
    dl_dir.mkdir(parents=True, exist_ok=True)
    p4.download_to(dl_dir)
    cloud_rel_paths = sorted([p._no_prefix_no_drive for p in p4.glob("**/*")])
    dled_rel_paths = sorted([str(p)[len(str(dl_dir)) :] for p in dl_dir.glob("**/*")])
    assert cloud_rel_paths == dled_rel_paths


@mock.patch("cloudpathlib.clients.s3.s3client.Session", return_value=MockBoto3Session())
def test_client_instantiation(mock_boto3, tmp_path):
    # Reset default client
    S3Client().set_as_default_client()

    p = S3Path("s3://bucket/dir_0/file0_0.txt")
    p2 = S3Path("s3://bucket/dir_0/file0_0.txt")

    # Check that client is the same instance
    assert p.client is p2.client

    # Check the file content is the same
    assert p.read_bytes() == p2.read_bytes()

    # should be using same instance of client, so cache should be the same
    assert p._local == p2._local
