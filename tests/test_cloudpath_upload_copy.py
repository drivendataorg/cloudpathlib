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
from tests.utils import _sync_filesystem


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
    sleep(1.1)
    p.write_text("newer")
    with pytest.raises(OverwriteNewerCloudError):
        p.upload_from(upload_assets_dir / "upload_1.txt")

    # to file, file exists and is newer; overwrite
    sleep(1.1)
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

    _sync_filesystem()

    # force overwrite
    sleep(1)
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
    sleep(1.1)
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

    # Test copying directories
    cloud_dir = rig.create_cloud_path("dir_1/")  # created by fixtures

    # Copy cloud directory to local directory
    local_dst = Path(tmpdir.mkdir("test_copy_dir_to_local"))
    result = cloud_dir.copy(local_dst)
    assert isinstance(result, Path)
    assert result.exists()
    assert result.is_dir()
    # Check that contents were copied
    assert (result / "file_1_0.txt").exists()
    assert (result / "dir_1_0").exists()

    # Copy cloud directory to cloud directory
    cloud_dst = rig.create_cloud_path("copied_dir/")
    result = cloud_dir.copy(cloud_dst)
    assert result.exists()
    # For HTTP/HTTPS providers, is_dir() may not work as expected due to dir_matcher logic
    if rig.path_class not in [HttpPath, HttpsPath]:
        assert result.is_dir()
    # Check that contents were copied
    assert (result / "file_1_0.txt").exists()
    assert (result / "dir_1_0").exists()

    # Copy cloud directory to string path
    local_dst2 = Path(tmpdir.mkdir("test_copy_dir_to_str"))
    result = cloud_dir.copy(str(local_dst2))
    assert result.exists()
    assert result.is_dir()
    # Check that contents were copied
    assert (result / "file_1_0.txt").exists()
    assert (result / "dir_1_0").exists()


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


def test_info(rig):
    """Test the info() method returns a CloudPathInfo object."""
    p = rig.create_cloud_path("dir_0/file0_0.txt")
    info = p.info()

    # Check that info() returns a CloudPathInfo object
    from cloudpathlib.cloudpath_info import CloudPathInfo

    assert isinstance(info, CloudPathInfo)

    # Check that the info object has the expected methods
    assert hasattr(info, "exists")
    assert hasattr(info, "is_dir")
    assert hasattr(info, "is_file")
    assert hasattr(info, "is_symlink")

    # Test that the info object works correctly
    assert info.exists() == p.exists()
    assert info.is_file() == p.is_file()
    assert info.is_dir() == p.is_dir()
    assert info.is_symlink() is False  # Cloud paths are never symlinks


def test_copy_into(rig, tmpdir):
    """Test the copy_into() method."""
    # Create a test file
    p = rig.create_cloud_path("test_file.txt")
    p.write_text("Hello from copy_into")

    # Test copying into a local directory
    local_dir = Path(tmpdir.mkdir("copy_into_local"))
    result = p.copy_into(local_dir)

    assert isinstance(result, Path)
    assert result.exists()
    assert result.name == "test_file.txt"
    assert result.read_text() == "Hello from copy_into"

    # Test copying into a cloud directory
    cloud_dir = rig.create_cloud_path("copy_into_cloud/")
    cloud_dir.mkdir()
    result = p.copy_into(cloud_dir)

    assert result.exists()
    assert str(result) == str(cloud_dir / "test_file.txt")
    assert result.read_text() == "Hello from copy_into"

    # Test copying into a string path
    local_dir2 = Path(tmpdir.mkdir("copy_into_str"))
    result = p.copy_into(str(local_dir2))

    assert result.exists()
    assert result.name == "test_file.txt"
    assert result.read_text() == "Hello from copy_into"

    # Test copying directories with copy_into
    cloud_dir = rig.create_cloud_path("dir_1/")  # created by fixtures

    # Copy cloud directory into local directory
    local_dst = Path(tmpdir.mkdir("copy_into_dir_local"))
    result = cloud_dir.copy_into(local_dst)
    assert isinstance(result, Path)
    assert result.exists()
    assert result.is_dir()
    assert result.name == "dir_1"  # Should preserve directory name
    # Check that contents were copied
    assert (result / "file_1_0.txt").exists()
    assert (result / "dir_1_0").exists()

    # Copy cloud directory into cloud directory
    cloud_dst = rig.create_cloud_path("copy_into_cloud_dst/")
    cloud_dst.mkdir()
    result = cloud_dir.copy_into(cloud_dst)
    assert result.exists()
    # For HTTP/HTTPS providers, is_dir() may not work as expected due to dir_matcher logic
    # Instead, check that the directory contents were copied
    if rig.path_class not in [HttpPath, HttpsPath]:
        assert result.is_dir()
    assert str(result) == str(cloud_dst / "dir_1")
    # Check that contents were copied
    assert (result / "file_1_0.txt").exists()
    assert (result / "dir_1_0").exists()

    # Copy cloud directory into string path
    local_dst2 = Path(tmpdir.mkdir("copy_into_dir_str"))
    result = cloud_dir.copy_into(str(local_dst2))
    assert result.exists()
    assert result.is_dir()
    assert result.name == "dir_1"  # Should preserve directory name
    # Check that contents were copied
    assert (result / "file_1_0.txt").exists()
    assert (result / "dir_1_0").exists()


def test_move(rig, tmpdir):
    """Test the move() method."""
    # Create a test file
    p = rig.create_cloud_path("test_move_file.txt")
    p.write_text("Hello from move")

    # Test moving to a local file
    local_file = Path(tmpdir) / "moved_file.txt"
    result = p.move(local_file)

    assert isinstance(result, Path)
    assert result.exists()
    assert result.read_text() == "Hello from move"
    # Note: When moving cloud->local, the source may still exist due to download_to behavior

    # Test moving to a cloud location (same client)
    p2 = rig.create_cloud_path("test_move_file2.txt")
    p2.write_text("Hello from move 2")

    cloud_dest = rig.create_cloud_path("moved_cloud_file.txt")
    result = p2.move(cloud_dest)

    assert result.exists()
    assert result.read_text() == "Hello from move 2"
    assert not p2.exists()  # Original should be gone for cloud->cloud moves

    # Test moving to a string path
    p3 = rig.create_cloud_path("test_move_file3.txt")
    p3.write_text("Hello from move 3")

    local_file2 = Path(tmpdir) / "moved_file3.txt"
    result = p3.move(str(local_file2))

    assert result.exists()
    assert result.read_text() == "Hello from move 3"
    # Note: When moving cloud->local, the source may still exist due to download_to behavior


def test_move_into(rig, tmpdir):
    """Test the move_into() method."""
    # Create a test file
    p = rig.create_cloud_path("test_move_into_file.txt")
    p.write_text("Hello from move_into")

    # Test moving into a local directory
    local_dir = Path(tmpdir.mkdir("move_into_local"))
    result = p.move_into(local_dir)

    assert isinstance(result, Path)
    assert result.exists()
    assert result.name == "test_move_into_file.txt"
    assert result.read_text() == "Hello from move_into"
    # Note: When moving cloud->local, the source may still exist due to download_to behavior

    # Test moving into a cloud directory
    p2 = rig.create_cloud_path("test_move_into_file2.txt")
    p2.write_text("Hello from move_into 2")

    cloud_dir = rig.create_cloud_path("move_into_cloud/")
    cloud_dir.mkdir()
    result = p2.move_into(cloud_dir)

    assert result.exists()
    assert str(result) == str(cloud_dir / "test_move_into_file2.txt")
    assert result.read_text() == "Hello from move_into 2"
    assert not p2.exists()  # Original should be gone for cloud->cloud moves

    # Test moving into a string path
    p3 = rig.create_cloud_path("test_move_into_file3.txt")
    p3.write_text("Hello from move_into 3")

    local_dir2 = Path(tmpdir.mkdir("move_into_str"))
    result = p3.move_into(str(local_dir2))

    assert result.exists()
    assert result.name == "test_move_into_file3.txt"
    assert result.read_text() == "Hello from move_into 3"
    # Note: When moving cloud->local, the source may still exist due to download_to behavior


def test_copy_nonexistent_file_error(rig):
    """Test that copying a non-existent file raises ValueError."""
    # Create a path that doesn't exist
    p = rig.create_cloud_path("nonexistent_file.txt")
    assert not p.exists()

    # Try to copy it - should raise ValueError (line 1148)
    with pytest.raises(ValueError, match=r"Path .* must exist to copy\."):
        p.copy(rig.create_cloud_path("destination.txt"))


def test_copy_with_cloudpath_objects(rig, tmpdir):
    """Test copy operations using CloudPath objects directly (not strings)."""
    # Create a test file
    p = rig.create_cloud_path("test_copy_objects.txt")
    p.write_text("Hello from copy objects")

    # Test copying directory with CloudPath object target (line 1155: target_path = target)
    # First create a directory with actual content
    cloud_dir = rig.create_cloud_path("test_dir/")
    (cloud_dir / "file1.txt").write_text("content1")
    (cloud_dir / "subdir/file2.txt").write_text("content2")

    # Copy to cloud directory using CloudPath object (not string)
    target_dir = rig.create_cloud_path("copied_dir/")
    result = cloud_dir.copy(target_dir)  # This should hit line 1155: target_path = target
    assert result.exists()
    # For HTTP/HTTPS providers, is_dir() may not work as expected due to dir_matcher logic
    if rig.path_class not in [HttpPath, HttpsPath]:
        assert result.is_dir()
    # Verify contents were copied
    assert (result / "file1.txt").exists()
    assert (result / "subdir/file2.txt").exists()

    # Test copying file with CloudPath object target (line 1166: destination = target)
    target_path = rig.create_cloud_path("copied_file.txt")
    result = p.copy(target_path)  # Using CloudPath object directly, not string - hits line 1166
    assert result.exists()
    assert result.read_text() == "Hello from copy objects"


def test_copy_into_with_cloudpath_objects(rig, tmpdir):
    """Test copy_into with CloudPath objects to cover line 1292."""
    # Create a test file
    p = rig.create_cloud_path("test_copy_into_objects.txt")
    p.write_text("Hello from copy_into objects")

    # Test copy_into with CloudPath object target_dir (line 1292: target_path = target_dir / self.name)
    cloud_dir = rig.create_cloud_path("copy_into_target/")
    cloud_dir.mkdir()

    result = p.copy_into(cloud_dir)  # Using CloudPath object directly, not string
    assert result.exists()
    assert str(result) == str(cloud_dir / "test_copy_into_objects.txt")
    assert result.read_text() == "Hello from copy_into objects"


def test_move_into_with_cloudpath_objects(rig, tmpdir):
    """Test move_into with CloudPath objects to cover line 1450."""
    # Create a test file
    p = rig.create_cloud_path("test_move_into_objects.txt")
    p.write_text("Hello from move_into objects")

    # Test move_into with CloudPath object target_dir (line 1450: target_path = target_dir / self.name)
    cloud_dir = rig.create_cloud_path("move_into_target/")
    cloud_dir.mkdir()

    result = p.move_into(cloud_dir)  # Using CloudPath object directly, not string
    assert result.exists()
    assert str(result) == str(cloud_dir / "test_move_into_objects.txt")
    assert result.read_text() == "Hello from move_into objects"
    assert not p.exists()  # Original should be gone for cloud->cloud moves
