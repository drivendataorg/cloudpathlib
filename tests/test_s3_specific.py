import pytest

from cloudpathlib import S3Path
from cloudpathlib.local import LocalS3Path
from boto3.s3.transfer import TransferConfig


@pytest.mark.parametrize("path_class", [S3Path, LocalS3Path])
def test_s3path_properties(path_class):
    p = path_class("s3://bucket")
    assert p.key == ""
    assert p.bucket == "bucket"

    p2 = path_class("s3://bucket/")
    assert p2.key == ""
    assert p2.bucket == "bucket"


def test_transfer_config(s3_rig, assets_dir, tmp_path):

    transfer_config = TransferConfig(multipart_threshold=50)
    client = s3_rig.client_class(boto3_transfer_config=transfer_config)
    assert client.boto3_transfer_config.multipart_threshold == 50
    # check defaults are inherited as well
    assert client.boto3_transfer_config.use_threads

    # download
    client.set_as_default_client()
    p = s3_rig.create_cloud_path("dir_0/file0_0.txt")
    dl_dir = tmp_path
    assert not (dl_dir / p.name).exists()
    p.download_to(dl_dir)
    assert client.s3.download_config == transfer_config

    # upload
    p2 = s3_rig.create_cloud_path("dir_0/file0_0_uploaded.txt")
    assert not p2.exists()
    p2.upload_from(dl_dir / p.name)
    assert client.s3.upload_config == transfer_config
    p2.unlink()
