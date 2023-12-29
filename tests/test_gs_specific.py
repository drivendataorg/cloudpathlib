import pytest

from cloudpathlib import GSPath
from cloudpathlib.local import LocalGSPath


@pytest.mark.parametrize("path_class", [GSPath, LocalGSPath])
def test_gspath_properties(path_class):
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
