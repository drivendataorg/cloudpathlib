from urllib.parse import urlparse, parse_qs

from google.api_core import retry
from google.api_core import exceptions
import pytest

from cloudpathlib import GSPath
from cloudpathlib.local import LocalGSPath


@pytest.mark.parametrize("path_class", [GSPath, LocalGSPath])
def test_gspath_properties(path_class, monkeypatch):
    if path_class == GSPath:
        monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "fake-project")

    p = path_class("gs://mybucket")
    assert p.blob == ""
    assert p.bucket == "mybucket"

    p2 = path_class("gs://mybucket/")
    assert p2.blob == ""
    assert p2.bucket == "mybucket"


@pytest.mark.parametrize("worker_type", ["process", "thread"])
def test_concurrent_download(gs_rig, tmp_path, worker_type):
    client = gs_rig.client_class(download_chunks_concurrently_kwargs={"worker_type": worker_type})
    p = gs_rig.create_cloud_path("dir_0/file0_0.txt", client=client)
    dl_dir = tmp_path
    assert not (dl_dir / p.name).exists()
    p.download_to(dl_dir)
    assert (dl_dir / p.name).is_file()


def test_as_url(gs_rig):
    p: GSPath = gs_rig.create_cloud_path("dir_0/file0_0.txt")
    public_url = p.as_url()
    public_url_parts = urlparse(public_url)
    assert public_url_parts.hostname and public_url_parts.hostname.startswith(
        "storage.googleapis.com"
    )
    assert public_url_parts.path.endswith("file0_0.txt")

    expire_seconds = 3600
    presigned_url = p.as_url(presign=True, expire_seconds=expire_seconds)
    parts = urlparse(presigned_url)
    query_params = parse_qs(parts.query)
    assert parts.path.endswith("file0_0.txt")
    assert query_params["X-Goog-Expires"] == [str(expire_seconds)]
    assert "X-Goog-Algorithm" in query_params
    assert "X-Goog-Credential" in query_params
    assert "X-Goog-Date" in query_params
    assert "X-Goog-SignedHeaders" in query_params
    assert "X-Goog-Signature" in query_params


@pytest.mark.parametrize(
    "contents",
    [
        "hello world",
        "another test case",
    ],
)
def test_md5_property(contents, gs_rig, monkeypatch):
    def _calculate_b64_wrapped_md5_hash(contents: str) -> str:
        # https://cloud.google.com/storage/docs/json_api/v1/objects
        from base64 import b64encode
        from hashlib import md5

        contents_md5_bytes = md5(contents.encode()).digest()
        b64string = b64encode(contents_md5_bytes).decode()
        return b64string

    # if USE_LIVE_CLOUD this doesn't have any effect
    expected_hash = _calculate_b64_wrapped_md5_hash(contents)
    monkeypatch.setenv("MOCK_EXPECTED_MD5_HASH", expected_hash)

    p: GSPath = gs_rig.create_cloud_path("dir_0/file0_0.txt")
    p.write_text(contents)
    assert p.md5 == expected_hash


def test_timeout_and_retry(gs_rig):
    custom_retry = retry.Retry(
        timeout=0.50,
        predicate=retry.if_exception_type(exceptions.ServerError),
    )

    fast_timeout_client = gs_rig.client_class(timeout=0.00001, retry=custom_retry)

    with pytest.raises(Exception) as exc_info:
        p = gs_rig.create_cloud_path("dir_0/file0_0.txt", client=fast_timeout_client)
        p.write_text("hello world " * 10000)

        assert "timed out" in str(exc_info.value)

    # can't force retries to happen in live cloud tests, so skip
    if not gs_rig.live_server:
        custom_retry = retry.Retry(
            initial=1.0,
            multiplier=1.0,
            timeout=15.0,
            predicate=retry.if_exception_type(exceptions.ServerError),
        )

        p = gs_rig.create_cloud_path(
            "dir_0/file0_0.txt", client=gs_rig.client_class(retry=custom_retry)
        )
        p.write_text("hello world")

        assert custom_retry.mocked_retries == 1
