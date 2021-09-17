from concurrent.futures import ProcessPoolExecutor
from time import sleep

import pytest

from boto3.s3.transfer import TransferConfig
import botocore
from cloudpathlib import S3Path
from cloudpathlib.local import LocalS3Path
import psutil


@pytest.mark.parametrize("path_class", [S3Path, LocalS3Path])
def test_s3path_properties(path_class):
    p = path_class("s3://bucket")
    assert p.key == ""
    assert p.bucket == "bucket"

    p2 = path_class("s3://bucket/")
    assert p2.key == ""
    assert p2.bucket == "bucket"


def test_transfer_config(s3_rig, tmp_path):
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

    # we can only check the configs are actually passed on the mock
    if not s3_rig.live_server:
        assert client.s3.download_config == transfer_config

    # upload
    p2 = s3_rig.create_cloud_path("dir_0/file0_0_uploaded.txt")
    assert not p2.exists()
    p2.upload_from(dl_dir / p.name)

    # we can only check the configs are actually passed on the mock
    if not s3_rig.live_server:
        assert client.s3.upload_config == transfer_config

    p2.unlink()


def _download_with_threads(s3_rig, tmp_path, use_threads):
    """Job used by tests to ensure Transfer config changes are
    actually passed through to boto3 and respected.
    """
    sleep(1)  # give test monitoring process time to start watching

    transfer_config = TransferConfig(
        max_concurrency=100,
        use_threads=use_threads,
        multipart_chunksize=1 * 1024,
        multipart_threshold=10 * 1024,
    )
    client = s3_rig.client_class(boto3_transfer_config=transfer_config)
    p = client.CloudPath(f"s3://{s3_rig.drive}/{s3_rig.test_dir}/dir_0/file0_to_download.txt")

    assert not p.exists()

    # file should be about 60KB
    text = "lalala" * 10_000
    p.write_text(text)

    assert p.exists()

    # assert not (dl_dir / p.name).exists()
    p.download_to(tmp_path)

    p.unlink()

    assert not p.exists()


def test_transfer_config_live(s3_rig, tmp_path):
    """Tests that boto3 receives and respects the transfer config
    when working with a live backend. Does this by observing
    if the `use_threads` parameter changes to number of threads
    used by a child process that does a download.
    """
    if s3_rig.live_server:

        def _execute_on_subprocess_and_observe(use_threads):
            main_test_process = psutil.Process().pid

            with ProcessPoolExecutor(max_workers=1) as executor:
                job = executor.submit(
                    _download_with_threads,
                    s3_rig=s3_rig,
                    tmp_path=tmp_path,
                    use_threads=use_threads,
                )

                max_threads = 0

                # timeout after 100 seconds
                for _ in range(1000):
                    worker_process_id = (
                        psutil.Process(main_test_process).children()[-1].pid
                    )  # most recently started child
                    n_thread = psutil.Process(worker_process_id).num_threads()

                    # observe number of threads used
                    max_threads = max(max_threads, n_thread)

                    sleep(0.1)

                    if job.done():
                        _ = job.result()  # raises if job raised
                        break

                return max_threads

        # usually ~3 threads are spun up whe use_threads is False
        assert _execute_on_subprocess_and_observe(use_threads=False) < 5

        # usually ~15 threads are spun up whe use_threads is True
        assert _execute_on_subprocess_and_observe(use_threads=True) > 10


def test_no_sign_request(s3_rig, tmp_path):
    """Tests that we can pass no_sign_request to the S3Client and we will
    be able to access public resources but not private ones.
    """
    if s3_rig.live_server:
        client = s3_rig.client_class(no_sign_request=True)

        # unsigned can access public path (part of AWS open data)
        p = client.CloudPath(
            "s3://ladi/Images/FEMA_CAP/2020/70349/DSC_0001_5a63d42e-27c6-448a-84f1-bfc632125b8e.jpg"
        )
        assert p.exists()

        p.download_to(tmp_path)
        assert (tmp_path / p.name).read_bytes() == p.read_bytes()

        # unsigned raises for private S3 file that exists
        p = client.CloudPath(f"s3://{s3_rig.drive}/dir_0/file0_to_download.txt")
        with pytest.raises(botocore.exceptions.ClientError):
            p.exists()
