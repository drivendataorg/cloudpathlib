from concurrent.futures import ProcessPoolExecutor
from itertools import islice
from time import sleep
import time

from urllib.parse import urlparse, parse_qs
import pytest

from boto3.s3.transfer import TransferConfig
import botocore
from cloudpathlib import S3Client, S3Path
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
    if not s3_rig.live_server:
        pytest.skip("This test only runs against live servers.")

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


def test_fake_directories(s3_like_rig):
    """S3 can have "fake" directories created
    either in the AWS S3 Console or by uploading
    a 0 size object ending in a `/`. If these objects
    exist, we want to treat them as directories.

    Note: Our normal tests do _not_ create folders in this
    way, so this test is the only one to exercise these "fake" dirs.

    Ref: https://github.com/boto/boto3/issues/377
    """
    if not s3_like_rig.live_server:
        pytest.skip("This test only runs against live servers.")

    boto3_s3_client = s3_like_rig.client_class._default_client.client

    response = boto3_s3_client.put_object(
        Bucket=f"{s3_like_rig.drive}",
        Body="",
        Key=f"{s3_like_rig.test_dir}/fake_directory/",
    )

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # test either way of referring to the directory (with and w/o terminal slash)
    fake_dir_slash = s3_like_rig.create_cloud_path("fake_directory/")
    fake_dir_no_slash = s3_like_rig.create_cloud_path("fake_directory")

    for test_case in [fake_dir_no_slash, fake_dir_slash]:
        assert test_case.exists()
        assert test_case.is_dir()
        assert not test_case.is_file()


def test_directories(s3_like_rig):
    """Fixing bug that marks as directories prefixes of existing paths

    Ref: https://github.com/drivendataorg/cloudpathlib/issues/208
    """
    super_path = s3_like_rig.create_cloud_path("dir_0/file0_0.txt")
    assert super_path.exists()
    assert not super_path.is_dir()

    super_path = s3_like_rig.create_cloud_path("dir_0")
    assert super_path.exists()
    assert super_path.is_dir()

    super_path = s3_like_rig.create_cloud_path("dir_0/file0")
    assert not super_path.exists()
    assert not super_path.is_dir()


def test_no_sign_request(s3_rig, tmp_path):
    """Tests that we can pass no_sign_request to the S3Client and we will
    be able to access public resources but not private ones.
    """
    if not s3_rig.live_server:
        pytest.skip("This test only runs against live servers.")

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


def test_extra_args_via_requester_pays(s3_rig, tmp_path):
    """Tests that we can pass Extra to the S3Client and we will
    be able to use those by leveraging the RequestPayer extra.

    NOTE: Requires AWS S3 credentials where you will pay for
    the requests here (which are a minimal amount).
    """
    if not s3_rig.live_server:
        pytest.skip("This test only runs against live servers.")

    # without the extras, this should raise
    with pytest.raises(botocore.exceptions.ClientError):
        list(s3_rig.path_class("s3://arxiv/pdf/").iterdir())

    client = s3_rig.client_class(extra_args={"RequestPayer": "requester"})

    # unsigned can access public path (axiv bucket is requester pays)
    paths = list(islice(client.CloudPath("s3://arxiv/pdf/").iterdir(), 0, 5))
    assert len(paths) > 0

    # download of a file to the local bucket
    local = client.CloudPath("s3://arxiv/pdf/arXiv_pdf_manifest.xml").download_to(tmp_path)

    assert local.stat().st_size > 0


def test_aws_endpoint_url_env(monkeypatch):
    """Allows setting endpoint_url from env variable
    until upstream boto3 PR is merged.
    https://github.com/boto/boto3/pull/2746
    """
    s3_url = "https://s3.amazonaws.com"
    localstack_url = "http://localhost:4566"

    s3_client = S3Client()
    assert s3_client.client.meta.endpoint_url == s3_url

    monkeypatch.setenv("AWS_ENDPOINT_URL", localstack_url)
    s3_client_custom_endpoint = S3Client()
    assert s3_client_custom_endpoint.client.meta.endpoint_url == localstack_url


def test_as_url_local(monkeypatch):
    path = S3Path("s3://arxiv/pdf")
    public_url = path.as_url()
    assert public_url == "https://arxiv.s3.amazonaws.com/pdf"

    localstack_url = "http://localhost:4566"
    monkeypatch.setenv("AWS_ENDPOINT_URL", localstack_url)
    s3_client_custom_endpoint = S3Client()

    path = S3Path("s3://arxiv/pdf", client=s3_client_custom_endpoint)
    public_url = path.as_url()
    assert public_url == f"{localstack_url}/arxiv/pdf"


def test_as_url_presign(s3_rig):
    p: S3Path = s3_rig.create_cloud_path("dir_0/file0_0.txt")
    expire_seconds = 3600
    expire_timestamp = int(time.time()) + expire_seconds
    presigned_url = p.as_url(presign=True, expire_seconds=expire_seconds)
    parts = urlparse(presigned_url)
    query_params = parse_qs(parts.query)

    assert parts.path.endswith("file0_0.txt")

    # v4 presigned URL (version depends on region and config)
    # https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
    if "X-Amz-Expires" in query_params:
        assert query_params["X-Amz-Expires"] == [str(expire_seconds)]
        assert "X-Amz-Algorithm" in query_params
        assert "X-Amz-Credential" in query_params
        assert "X-Amz-Date" in query_params
        assert "X-Amz-SignedHeaders" in query_params
        assert "X-Amz-Signature" in query_params
    elif "Expires" in query_params:
        assert query_params["Expires"] == [str(expire_timestamp)]
        assert "AWSAccessKeyId" in query_params
        assert "Signature" in query_params
    else:
        assert False, "Unknown presigned URL format"
