import pytest

from cloudpathlib import S3Path
from cloudpathlib.cloudpath import InvalidPrefix


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
    assert S3Path("S3://a/b/c/d.file").with_suffix(".png") == "S3://a/b/c/d.file"

    assert S3Path("s3://a") / "b" == S3Path("s3://a/b")
