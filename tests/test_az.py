import pytest

from cloudpathlib import AzureBlobPath, S3Backend
from cloudpathlib.cloudpath import InvalidPrefix, BackendMismatch


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
