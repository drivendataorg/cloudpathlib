from cloudpathlib import S3Path


def test_s3path_properties(s3_rig):
    p = S3Path("s3://bucket")
    assert p.key == ""
    assert p.bucket == "bucket"

    p2 = S3Path("s3://bucket/")
    assert p2.key == ""
    assert p2.bucket == "bucket"
