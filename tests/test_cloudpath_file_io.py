from datetime import datetime
import os
from pathlib import PurePosixPath
from time import sleep

import pytest

from cloudpathlib import DirectoryNotEmpty


def test_file_discovery(rig):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    assert p.exists()

    p2 = rig.create_cloud_path("dir_0/not_a_file")
    assert not p2.exists()
    p2.touch()
    assert p2.exists()
    p2.unlink()

    p3 = rig.create_cloud_path("dir_0/")
    assert p3.exists()
    assert len(list(p3.iterdir())) == 3
    assert len(list(p3.glob("**/*"))) == 3

    with pytest.raises(ValueError):
        p3.unlink()

    with pytest.raises(DirectoryNotEmpty):
        p3.rmdir()
    p3.rmtree()
    assert not p3.exists()

    p4 = rig.create_cloud_path("")
    assert p4.exists()

    assert len(list(p4.iterdir())) == 1  # only bucket/dir_1/ should still exist
    assert len(list(p4.glob("**/*"))) == 4

    assert list(p4.glob("**/*")) == list(p4.rglob("*"))


def test_file_read_writes(rig, tmp_path):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    p2 = rig.create_cloud_path("dir_0/not_a_file")
    p3 = rig.create_cloud_path("")

    text = "lalala" * 10_000

    p.write_text(text)
    assert p.read_text() == text
    p2.write_text(text)

    # sleep between writes to p to ensure different
    # modified times
    sleep(1)

    p.write_bytes(p2.read_bytes())
    assert p.read_text() == p2.read_text()

    before_touch = datetime.now()
    sleep(1)
    p.touch()
    assert datetime.fromtimestamp(p.stat().st_mtime) > before_touch

    # no-op
    p.mkdir()

    assert p.etag is not None

    dest = rig.create_cloud_path("dir2/new_file0_0.txt")
    assert not dest.exists()
    p.rename(dest)
    assert dest.exists()

    assert not p.exists()
    p.touch()
    dest.replace(p)
    assert p.exists()

    dl_file = tmp_path / "file"
    p.download_to(dl_file)
    assert dl_file.exists()
    assert p.read_text() == dl_file.read_text()

    dl_dir = tmp_path / "directory"
    dl_dir.mkdir(parents=True, exist_ok=True)
    p3.download_to(dl_dir)
    cloud_rel_paths = sorted(
        # CloudPath("prefix://drive/dir/file.txt")._no_prefix_no_drive = "/dir/file.txt"
        [p._no_prefix_no_drive[len(rig.test_dir) + 2 :] for p in p3.glob("**/*")]
    )
    dled_rel_paths = sorted(
        [str(PurePosixPath(p.relative_to(dl_dir))) for p in dl_dir.glob("**/*")]
    )
    assert cloud_rel_paths == dled_rel_paths


def test_fspath(rig):
    p = rig.create_cloud_path("dir_0")
    assert os.fspath(p) == p.fspath


def test_os_open(rig):
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    with open(p, "r") as f:
        assert f.readable()
