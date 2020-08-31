from datetime import datetime
from time import sleep
from unittest import mock

import pytest

from cloudpathlib import AzureBlobPath, AzureBlobBackend, S3Backend
from cloudpathlib.cloudpath import BackendMismatch, DirectoryNotEmpty, InvalidPrefix

from .mock_backends.mock_azureblob import MockBlobServiceClient


@pytest.fixture
def fake_connection_string(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "AccountName=fake;AccountKey=fake2;")


def test_initialize_az(fake_connection_string):
    with pytest.raises(TypeError):
        p = AzureBlobPath()

    with pytest.raises(InvalidPrefix):
        p = AzureBlobPath("NOT_S3_PATH")

    with pytest.raises(BackendMismatch):
        p = AzureBlobPath("az://test/t", backend=S3Backend())

    # case insensitive
    cases = ["az://b/k", "AZ://b/k", "Az://b/k.file", "aZ://b/k", "az://b"]

    for expected in cases:
        p = AzureBlobPath(expected)
        assert repr(p) == f"AzureBlobPath('{expected}')"
        assert str(p) == expected

        assert p._no_prefix == expected.split("://", 1)[-1]

        assert p._url.scheme == expected.split("://", 1)[0].lower()
        assert p._url.netloc == expected.split("://", 1)[-1].split("/")[0]

        assert str(p._path) == expected.split(":/", 1)[-1]


def test_az_joins(fake_connection_string):
    assert AzureBlobPath("az://a/b/c/d").name == "d"
    assert AzureBlobPath("az://a/b/c/d.file").name == "d.file"
    assert AzureBlobPath("az://a/b/c/d.file").stem == "d"
    assert AzureBlobPath("az://a/b/c/d.file").suffix == ".file"
    assert str(AzureBlobPath("az://a/b/c/d.file").with_suffix(".png")) == "az://a/b/c/d.png"

    assert AzureBlobPath("az://a") / "b" == AzureBlobPath("az://a/b")
    assert AzureBlobPath("az://a/b/c/d") / "../../b" == AzureBlobPath("az://a/b/b")


@mock.patch(
    "cloudpathlib.backends.azure.azblobbackend.BlobServiceClient.from_connection_string",
    return_value=MockBlobServiceClient(),
)
def test_with_mock_az(mock_azure, tmp_path):
    # Reset default backend
    AzureBlobBackend().set_as_default_backend()

    p = AzureBlobPath("az://bucket/dir_0/file0_0.txt")
    assert p == AzureBlobBackend.get_default_backend().CloudPath("az://bucket/dir_0/file0_0.txt")
    assert p == AzureBlobBackend.get_default_backend().AzureBlobPath(
        "az://bucket/dir_0/file0_0.txt"
    )

    assert p.exists()

    p2 = AzureBlobPath("az://bucket/dir_0/not_a_file")
    assert not p2.exists()
    p2.touch()
    assert p2.exists()
    p2.unlink()
    assert not p2.exists()

    p3 = AzureBlobPath("az://bucket/dir_0/")
    assert p3.exists()
    assert len(list(p3.iterdir())) == 3
    assert len(list(p3.glob("**/*"))) == 3

    with pytest.raises(ValueError):
        p3.unlink()

    with pytest.raises(DirectoryNotEmpty):
        p3.rmdir()
    p3.rmtree()
    assert not p3.exists()

    p4 = AzureBlobPath("AZ://bucket")
    assert p4.exists()
    assert p4.blob == ""
    p4 = AzureBlobPath("AZ://bucket/")

    assert p4.exists()
    assert p4.blob == ""

    assert len(list(p4.iterdir())) == 1  # only s3://bucket/dir_1/ should still exist
    assert len(list(p4.glob("**/*"))) == 4
    assert len(list(p4.glob("az://bucket/**/*"))) == 4

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

    dest = AzureBlobPath("az://bucket/dir2/new_file0_0.txt")
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


@mock.patch(
    "cloudpathlib.backends.azure.azblobbackend.BlobServiceClient.from_connection_string",
    return_value=MockBlobServiceClient(),
)
def test_backend_instantiation(mock_azure, tmp_path):
    # Reset default backend
    AzureBlobBackend().set_as_default_backend()

    p = AzureBlobPath("az://bucket/dir_0/file0_0.txt")
    p2 = AzureBlobPath("az://bucket/dir_0/file0_0.txt")

    # Check that backend is the same instance
    assert p.backend is p2.backend

    # Check the file content is the same
    assert p.read_bytes() == p2.read_bytes()

    # should be using same instance of backend, so cache should be the same
    assert p._local == p2._local
