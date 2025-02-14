from pathlib import Path
from shutil import ignore_patterns
from time import sleep

import pytest

from cloudpathlib.http.httppath import HttpPath, HttpsPath
from cloudpathlib.local import LocalGSPath, LocalS3Path, LocalS3Client
from cloudpathlib.exceptions import (
    CloudPathFileExistsError,
    CloudPathNotADirectoryError,
    OverwriteNewerCloudError,
)


@pytest.fixture
def upload_assets_dir(tmpdir):
    tmp_assets = tmpdir.mkdir("test_upload_from_dir")
    p = Path(tmp_assets)
    (p / "upload_1.txt").write_text("Hello from 1")
    (p / "upload_2.txt").write_text("Hello from 2")

    sub = p / "subdir"
    sub.mkdir(parents=True)
    (sub / "sub_upload_1.txt").write_text("Hello from sub 1")
    (sub / "sub_upload_2.txt").write_text("Hello from sub 2")

    yield p


def assert_mirrored(cloud_path, local_path, check_no_extra=True):
    # file exists and is file
    if local_path.is_file():
        assert cloud_path.exists()
        assert cloud_path.is_file()
    else:
        # all local files exist on cloud
        for lp in local_path.iterdir():
            assert_mirrored(cloud_path / lp.name, lp)

        # no extra files on cloud
        if check_no_extra:
            assert len(list(local_path.glob("**/*"))) == len(list(cloud_path.glob("**/*")))

    return True


def test_upload_from_file(rig, upload_assets_dir):
    to_upload = upload_assets_dir / "upload_1.txt"

    # to file, file does not exists
    p = rig.create_cloud_path("upload_test.txt")
    assert not p.exists()

    p.upload_from(to_upload)
    assert p.exists()
    assert p.read_text() == "Hello from 1"

    # to file, file exists
    to_upload_2 = upload_assets_dir / "upload_2.txt"
    sleep(1.1)
    to_upload_2.touch()  # make sure local is newer
    p.upload_from(to_upload_2)
    assert p.exists()
    assert p.read_text() == "Hello from 2"

    # to file, file exists and is newer
    p.write_text("newer")
    with pytest.raises(OverwriteNewerCloudError):
        p.upload_from(upload_assets_dir / "upload_1.txt")

    # to file, file exists and is newer; overwrite
    p.write_text("even newer")
    sleep(1.1)
    p.upload_from(upload_assets_dir / "upload_1.txt", force_overwrite_to_cloud=True)
    assert p.exists()
    assert p.read_text() == "Hello from 1"

    # to dir, dir exists
    p = rig.create_cloud_path("dir_0/")  # created by fixtures
    assert p.exists()
    p.upload_from(upload_assets_dir / "upload_1.txt")
    assert (p / "upload_1.txt").exists()
    assert (p / "upload_1.txt").read_text() == "Hello from 1"


def test_upload_from_dir(rig, upload_assets_dir):
    # to dir, dir does not exists
    p = rig.create_cloud_path("upload_test_dir")
    assert not p.exists()

    p.upload_from(upload_assets_dir)
    assert assert_mirrored(p, upload_assets_dir)

    # to dir, dir exists
    p2 = rig.create_cloud_path("dir_0/")  # created by fixtures
    assert p2.exists()

    p2.upload_from(upload_assets_dir)
    assert assert_mirrored(p2, upload_assets_dir, check_no_extra=False)

    # a newer file exists on cloud
    sleep(1)
    (p / "upload_1.txt").write_text("newer")
    with pytest.raises(OverwriteNewerCloudError):
        p.upload_from(upload_assets_dir)

    # force overwrite
    (p / "upload_1.txt").write_text("even newer")
    (p / "upload_2.txt").unlink()
    p.upload_from(upload_assets_dir, force_overwrite_to_cloud=True)
    assert assert_mirrored(p, upload_assets_dir)


def test_copy(rig, upload_assets_dir, tmpdir):
    to_upload = upload_assets_dir / "upload_1.txt"
    p = rig.create_cloud_path("upload_test.txt")
    assert not p.exists()
    p.upload_from(to_upload)
    assert p.exists()

    # cloud to local dir
    dst = Path(tmpdir.mkdir("test_copy_to_local"))
    out_file = p.copy(dst)
    assert out_file.exists()
    assert out_file.read_text() == "Hello from 1"
    out_file.unlink()

    p.copy(str(out_file))
    assert out_file.exists()
    assert out_file.read_text() == "Hello from 1"

    # cloud to local file
    p.copy(dst / "file.txt")

    # cloud to cloud -> make sure no local cache
    p_new = p.copy(p.parent / "new_upload_1.txt")
    assert p_new.exists()

    if rig.path_class not in [HttpPath, HttpsPath]:
        assert not p_new._local.exists()  # cache should never have been downloaded
        assert not p._local.exists()  # cache should never have been downloaded
        assert p_new.read_text() == "Hello from 1"

    # cloud to cloud path as string
    cloud_dest = str(p.parent / "new_upload_0.txt")
    p_new = p.copy(cloud_dest)
    assert p_new.exists()
    assert p_new.read_text() == "Hello from 1"

    # cloud to cloud directory
    cloud_dest = rig.create_cloud_path("dir_1/")  # created by fixtures
    p_new = p.copy(cloud_dest)
    assert str(p_new) == str(p_new.parent / p.name)  # file created
    assert p_new.exists()
    assert p_new.read_text() == "Hello from 1"

    # cloud to cloud overwrite
    p_new.write_text("p_new")
    with pytest.raises(OverwriteNewerCloudError):
        p_new = p.copy(p_new)

    p_new = p.copy(p_new, force_overwrite_to_cloud=True)
    assert p_new.exists()

    # cloud to other cloud
    p2 = rig.create_cloud_path("dir_0/file0_0.txt")
    other = (
        LocalS3Path("s3://fake-bucket/new_other.txt")
        if not isinstance(p2.client, LocalS3Client)
        else LocalGSPath("gs://fake-bucket/new_other.txt")
    )
    assert not other.exists()

    assert not p2._local.exists()  # not in cache
    p2.copy(other)  # forces download + reupload
    assert p2._local.exists()  # in cache
    assert other.exists()
    assert other.read_text() == p2.read_text()

    other.unlink()

    # cloud to other cloud dir
    other_dir = (
        LocalS3Path("s3://fake-bucket/new_other")
        if not isinstance(p2.client, LocalS3Client)
        else LocalGSPath("gs://fake-bucket/new_other")
    )
    (other_dir / "file.txt").write_text("i am a file")  # ensure other_dir exists
    assert other_dir.exists()
    assert not (other_dir / p2.name).exists()

    p2.copy(other_dir)
    assert (other_dir / p2.name).exists()
    assert (other_dir / p2.name).read_text() == p2.read_text()
    (other_dir / p2.name).unlink()

    # cloud dir raises
    cloud_dir = rig.create_cloud_path("dir_1/")  # created by fixtures
    with pytest.raises(ValueError) as e:
        p_new = cloud_dir.copy(Path(tmpdir.mkdir("test_copy_dir_fails")))
        assert "use the method copytree" in str(e)


def test_copytree(rig, tmpdir):
    # cloud file raises
    with pytest.raises(CloudPathNotADirectoryError):
        p = rig.create_cloud_path("dir_0/file0_0.txt")
        local_out = Path(tmpdir.mkdir("copytree_fail_on_file"))
        p.copytree(local_out)

    with pytest.raises(CloudPathFileExistsError):
        p = rig.create_cloud_path("dir_0/")
        p_out = rig.create_cloud_path("dir_0/file0_0.txt")
        p.copytree(p_out)

    # cloud dir to local dir that exists
    p = rig.create_cloud_path("dir_1/")
    local_out = Path(tmpdir.mkdir("copytree_from_cloud"))
    p.copytree(local_out)
    assert assert_mirrored(p, local_out)

    # str version of path
    local_out = Path(tmpdir.mkdir("copytree_to_str_path"))
    p.copytree(str(local_out))
    assert assert_mirrored(p, local_out)

    # cloud dir to local dir that does not exist
    local_out = local_out / "new_folder"
    p.copytree(local_out)
    assert assert_mirrored(p, local_out)

    # cloud dir to cloud dir that does not exist
    p2 = rig.create_cloud_path("new_dir/")
    p.copytree(p2)
    assert assert_mirrored(p2, p)

    # cloud dir to cloud dir that exists
    p2 = rig.create_cloud_path("new_dir2/")
    (p2 / "existing_file.txt").write_text("asdf")  # ensures p2 exists
    p.copytree(p2)
    assert assert_mirrored(p2, p, check_no_extra=False)

    (p / "new_file.txt").write_text("hello!")  # add file so we can assert mirror
    with pytest.raises(OverwriteNewerCloudError):
        p.copytree(p2)

    p.copytree(p2, force_overwrite_to_cloud=True)
    assert assert_mirrored(p2, p, check_no_extra=False)

    # additional files that will be ignored using the ignore argument
    (p / "ignored.py").write_text("print('ignore')")
    (p / "dir1" / "file1.txt").write_text("ignore")
    (p / "dir2" / "file2.txt").write_text("ignore")

    # cloud dir to local dir but ignoring files (shutil.ignore_patterns)
    p3 = rig.create_cloud_path("new_dir3/")
    p.copytree(p3, ignore=ignore_patterns("*.py", "dir*"))
    assert assert_mirrored(p, p3, check_no_extra=False)
    assert not (p3 / "ignored.py").exists()
    assert not (p3 / "dir1").exists()
    assert not (p3 / "dir2").exists()

    # cloud dir to local dir but ignoring files (custom function)
    p4 = rig.create_cloud_path("new_dir4/")

    def _custom_ignore(path, names):
        ignore = []
        for name in names:
            if name.endswith(".py"):
                ignore.append(name)
            elif name.startswith("dir"):
                ignore.append(name)
        return ignore

    p.copytree(p4, ignore=_custom_ignore)
    assert assert_mirrored(p, p4, check_no_extra=False)
    assert not (p4 / "ignored.py").exists()
    assert not (p4 / "dir1").exists()
    assert not (p4 / "dir2").exists()
