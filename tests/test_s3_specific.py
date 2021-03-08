import pytest

from cloudpathlib import S3Path
from cloudpathlib.local import LocalS3Path


@pytest.mark.parametrize("path_class", [S3Path, LocalS3Path])
def test_s3path_properties(path_class):
    p = path_class("s3://bucket")
    assert p.key == ""
    assert p.bucket == "bucket"

    p2 = path_class("s3://bucket/")
    assert p2.key == ""
    assert p2.bucket == "bucket"
