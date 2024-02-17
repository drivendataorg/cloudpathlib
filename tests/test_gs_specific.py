import pytest

from urllib.parse import urlparse, parse_qs
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
