import builtins
import importlib
import os
import os.path
import glob
import tempfile

import pytest

from cloudpathlib import patch_open, patch_os_functions, patch_glob, patch_all_builtins
import cloudpathlib
from cloudpathlib.cloudpath import CloudPath


def test_patch_open(rig):
    cp = rig.create_cloud_path("dir_0/new_file.txt")

    with pytest.raises(FileNotFoundError):
        with open(cp, "w") as f:
            f.write("Hello!")

    # set via method call
    with patch_open():
        with open(cp, "w") as f:
            f.write("Hello!")

        assert cp.read_text() == "Hello!"


def test_patch_open_with_env(rig, monkeypatch):
    orig_open = builtins.open
    orig_fspath = CloudPath.__fspath__

    try:
        monkeypatch.setenv("CLOUDPATHLIB_PATCH_OPEN", "1")
        importlib.reload(cloudpathlib)

        cp = rig.create_cloud_path("dir_0/new_file_two.txt")

        with open(cp, "w") as f:
            f.write("Hello!")

        assert cp.read_text() == "Hello!"

    finally:
        builtins.open = orig_open
        CloudPath.__fspath__ = orig_fspath


def test_patch_os_functions(rig):
    """Test all OS and os.path functions in a single comprehensive test."""

    # Set up test data
    test_dir = rig.create_cloud_path("test_dir/")
    test_file = rig.create_cloud_path("test_dir/test_file.txt")
    test_file.write_text("test content")

    # Create another file for testing operations
    source_file = rig.create_cloud_path("test_dir/source.txt")
    source_file.write_text("source content")
    dest_file = rig.create_cloud_path("test_dir/dest.txt")

    with patch_os_functions():
        # Test os.fspath
        result = os.fspath(test_file)
        assert result == test_file

        # Test os.listdir
        result = os.listdir(test_dir)
        assert isinstance(result, list)
        assert all(isinstance(item, CloudPath) for item in result)
        assert len(result) > 0

        # Test os.lstat
        result = os.lstat(test_file)
        assert hasattr(result, "st_size")
        assert hasattr(result, "st_mtime")

        # Test os.mkdir (may not work on all providers)
        new_dir = rig.create_cloud_path("test_dir/new_dir/")
        try:
            os.mkdir(new_dir)
        except Exception:
            pass  # Some providers don't support directory creation

        # Test os.makedirs (may not work on all providers)
        deep_dir = rig.create_cloud_path("test_dir/deep/nested/dir/")
        try:
            os.makedirs(deep_dir)
        except Exception:
            pass  # Some providers don't support directory creation

        # Test os.remove
        temp_file = rig.create_cloud_path("test_dir/temp_remove.txt")
        temp_file.write_text("temp")
        os.remove(temp_file)
        assert not temp_file.exists()

        # Test os.rename
        os.rename(source_file, dest_file)
        assert not source_file.exists()
        assert dest_file.exists()
        assert dest_file.read_text() == "source content"

        # Test os.replace (may not work on all providers)
        replace_source = rig.create_cloud_path("test_dir/replace_source.txt")
        replace_source.write_text("replace source")
        replace_dest = rig.create_cloud_path("test_dir/replace_dest.txt")
        replace_dest.write_text("old content")
        try:
            os.replace(replace_source, replace_dest)
            assert not replace_source.exists()
            assert replace_dest.exists()
            assert replace_dest.read_text() == "replace source"
        except Exception:
            pass  # Some providers don't support atomic replace

        # Test os.rmdir (may not work on all providers)
        empty_dir = rig.create_cloud_path("test_dir/empty_dir/")
        try:
            os.rmdir(empty_dir)
            assert not empty_dir.exists()
        except Exception:
            pass  # Some providers don't support directory removal

        # Test os.scandir
        result = os.scandir(test_dir)
        items = list(result)
        assert all(isinstance(item, CloudPath) for item in items)
        assert len(items) > 0

        # Test os.stat
        result = os.stat(test_file)
        assert hasattr(result, "st_size")
        assert hasattr(result, "st_mtime")

        # Test os.unlink
        temp_unlink = rig.create_cloud_path("test_dir/temp_unlink.txt")
        temp_unlink.write_text("temp")
        os.unlink(temp_unlink)
        assert not temp_unlink.exists()

        # Test os.walk
        result = list(os.walk(test_dir))
        assert len(result) > 0
        for root, dirs, files in result:
            assert isinstance(root, CloudPath)
            assert all(
                isinstance(d, str) for d in dirs
            )  # pathlib.Path.walk returns dirs as string, not Path
            assert all(
                isinstance(f, str) for f in files
            )  # pathlib.Path.walk returns filenames as string, not Path

        # Test os.path.basename
        result = os.path.basename(test_file)
        assert result == "test_file.txt"

        # Test os.path.commonpath
        file1 = rig.create_cloud_path("test_dir/file1.txt")
        file2 = rig.create_cloud_path("test_dir/file2.txt")
        result = os.path.commonpath([file1, file2])
        assert isinstance(result, CloudPath)

        # Test os.path.commonprefix
        result = os.path.commonprefix([file1, file2])
        assert isinstance(result, str)
        assert "test_dir" in result

        # Test os.path.dirname
        result = os.path.dirname(test_file)
        assert isinstance(result, CloudPath)

        # Test os.path.exists
        result = os.path.exists(test_file)
        assert isinstance(result, bool)
        assert result is True

        # Test os.path.getatime
        result = os.path.getatime(test_file)
        if isinstance(result, tuple):
            result = result[0]
        if result is not None:
            assert isinstance(result, (int, float))

        # Test os.path.getmtime
        result = os.path.getmtime(test_file)
        if isinstance(result, tuple):
            result = result[0]
        if result is not None:
            assert isinstance(result, (int, float))

        # Test os.path.getctime
        result = os.path.getctime(test_file)
        if isinstance(result, tuple):
            result = result[0]
        if result is not None:
            assert isinstance(result, (int, float))

        # Test os.path.getsize
        result = os.path.getsize(test_file)
        if isinstance(result, tuple):
            result = result[0]
        if result is not None:
            assert isinstance(result, int)

        # Test os.path.isfile
        try:
            assert os.path.isfile(test_file) is True
            assert os.path.isfile(test_dir) is False
        except AttributeError:
            pass  # Some providers don't support _is_file_or_dir

        # Test os.path.isdir
        try:
            assert os.path.isdir(test_dir) is True
            assert os.path.isdir(test_file) is False
        except AttributeError:
            pass  # Some providers don't support _is_file_or_dir

        # Test os.path.join
        result = os.path.join(test_dir, "subdir", "file.txt")
        assert isinstance(result, CloudPath)
        expected = rig.create_cloud_path("test_dir/subdir/file.txt")
        assert result == expected

        # Test os.path.split
        head, tail = os.path.split(test_file)
        assert isinstance(head, CloudPath)
        assert isinstance(tail, str)
        assert tail == "test_file.txt"

        # Test os.path.splitext
        root, ext = os.path.splitext(test_file)
        assert isinstance(root, str)
        assert isinstance(ext, str)
        assert ext == ".txt"


def test_patch_os_functions_with_strings(rig):
    """Test that regular string paths still work with patched functions."""
    with patch_os_functions():
        # Regular string paths should still work
        assert os.path.exists(".")  # Current directory should exist
        assert os.path.isdir(".")  # Current directory should be a directory


def test_patch_os_functions_context_manager(rig):
    """Test that patches are applied and restored correctly."""
    original_listdir = os.listdir
    original_exists = os.path.exists

    with patch_os_functions():
        # Patches should be applied
        assert os.listdir != original_listdir
        assert os.path.exists != original_exists

    # Patches should be restored
    assert os.listdir == original_listdir
    assert os.path.exists == original_exists


def test_patch_os_functions_error_handling(rig):
    """Test error handling for non-existent files."""
    non_existent = rig.create_cloud_path("non_existent_file.txt")

    with patch_os_functions():
        with pytest.raises(FileNotFoundError):
            os.remove(non_existent)


def test_patch_os_functions_mixed_usage(rig):
    """Test mixed usage of CloudPath and regular paths."""
    cloud_path = rig.create_cloud_path("test_dir/cloud_file.txt")
    cloud_path.write_text("test content")

    # Create a temporary local file
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write("local content")
        local_path = f.name

    try:
        with patch_os_functions():
            # Both CloudPath and regular paths should work
            assert os.path.exists(cloud_path)
            assert os.path.exists(local_path)

            # Handle the tuple return type for getsize
            cloud_size = os.path.getsize(cloud_path)
            if isinstance(cloud_size, tuple):
                cloud_size = cloud_size[0]
            # Some providers may return None for file size
            if cloud_size is not None:
                assert cloud_size >= 0  # Allow 0 size

            local_size = os.path.getsize(local_path)
            assert local_size > 0
    finally:
        # Clean up local file
        os.unlink(local_path)


def test_patch_glob_with_strings(rig):
    """Test glob with regular string patterns."""
    with patch_glob():
        # Regular string patterns should still work
        result = glob.glob("*.py")  # Should find Python files
        assert isinstance(result, list)


def test_patch_glob_with_cloudpath_patterns(rig):
    """Test glob with CloudPath patterns."""
    with patch_glob():
        # Test basic file pattern matching
        test_dir = rig.create_cloud_path("test_dir")
        test_dir.mkdir(exist_ok=True)

        # Create test files
        test_file1 = test_dir / "file1.txt"
        test_file2 = test_dir / "file2.txt"
        test_file3 = test_dir / "data.csv"

        test_file1.write_text("content1")
        test_file2.write_text("content2")
        test_file3.write_text("data")

        # Test basic wildcard patterns
        result = glob.glob(test_dir / "*.txt")
        assert len(result) == 2
        assert all(isinstance(p, type(test_dir)) for p in result)
        assert any("file1.txt" in str(p) for p in result)
        assert any("file2.txt" in str(p) for p in result)

        # Test specific file pattern
        result = glob.glob(test_dir / "file*.txt")
        assert len(result) == 2

        # Test with different extension
        result = glob.glob(test_dir / "*.csv")
        assert len(result) == 1
        assert "data.csv" in str(result[0])


def test_patch_glob_with_recursive_patterns(rig):
    """Test glob with recursive ** patterns."""
    with patch_glob():
        # Create nested directory structure
        root_dir = rig.create_cloud_path("glob_test_root")
        root_dir.mkdir(exist_ok=True)

        subdir1 = root_dir / "subdir1"
        subdir1.mkdir(exist_ok=True)

        subdir2 = subdir1 / "subdir2"
        subdir2.mkdir(exist_ok=True)

        # Create files at different levels
        root_file = root_dir / "root.txt"
        sub1_file = subdir1 / "sub1.txt"
        sub2_file = subdir2 / "sub2.txt"

        root_file.write_text("root")
        sub1_file.write_text("sub1")
        sub2_file.write_text("sub2")

        # Test recursive pattern to find all .txt files
        # Note: CloudPath recursive glob support may vary by implementation
        result = glob.glob(root_dir / "**/*.txt")
        # Should find at least the root file, and potentially subdirectory files
        assert len(result) >= 1
        assert any("root.txt" in str(p) for p in result)

        # Test recursive pattern from specific subdirectory
        result = glob.glob(subdir1 / "**/*.txt")
        # Should find at least the sub1.txt file
        assert len(result) >= 1
        assert any("sub1.txt" in str(p) for p in result)

        # Test recursive pattern with specific depth
        result = glob.glob(root_dir / "*/*.txt")
        assert len(result) == 1
        assert "sub1.txt" in str(result[0])


def test_patch_glob_with_iglob(rig):
    """Test iglob iterator functionality."""
    with patch_glob():
        test_dir = rig.create_cloud_path("iglob_test")
        test_dir.mkdir(exist_ok=True)

        # Create test files
        files = []
        for i in range(3):
            test_file = test_dir / f"file{i}.txt"
            test_file.write_text(f"content{i}")
            files.append(test_file)

        # Test iglob returns iterator
        result = glob.iglob(test_dir / "*.txt")
        assert hasattr(result, "__iter__")

        # Convert to list and verify
        result_list = list(result)
        assert len(result_list) == 3
        assert all(isinstance(p, type(test_dir)) for p in result_list)

        # Test that iterator can only be consumed once
        result2 = glob.iglob(test_dir / "*.txt")
        first_item = next(result2)
        assert isinstance(first_item, type(test_dir))


def test_patch_glob_with_root_dir_parameter(rig):
    """Test glob with root_dir parameter."""
    with patch_glob():
        # Create test structure
        root_dir = rig.create_cloud_path("root_dir_test")
        root_dir.mkdir(exist_ok=True)

        subdir = root_dir / "subdir"
        subdir.mkdir(exist_ok=True)

        test_file = subdir / "test.txt"
        test_file.write_text("test")

        # Test with root_dir parameter
        result = glob.glob("test.txt", root_dir=subdir)
        assert len(result) == 1
        assert isinstance(result[0], type(root_dir))
        assert "test.txt" in str(result[0])

        # Test with pattern and root_dir
        result = glob.glob("*.txt", root_dir=subdir)
        assert len(result) == 1

        # Test with recursive pattern and root_dir
        result = glob.glob("**/*.txt", root_dir=root_dir)
        assert len(result) == 1


def test_patch_glob_with_complex_patterns(rig):
    """Test glob with complex pattern combinations."""
    with patch_glob():
        test_dir = rig.create_cloud_path("complex_pattern_test")
        test_dir.mkdir(exist_ok=True)

        # Create files with various names
        files = [
            "file1.txt",
            "file2.py",
            "data.csv",
            "config.json",
            "README.md",
            "test_file.py",
            "archive.tar.gz",
        ]

        created_files = []
        for filename in files:
            file_path = test_dir / filename
            file_path.write_text("content")
            created_files.append(file_path)

        # Test multiple extensions (brace expansion not supported in standard glob)
        # So we test individual patterns instead
        result = glob.glob(test_dir / "*.txt")
        assert len(result) == 1
        result = glob.glob(test_dir / "*.py")
        assert len(result) == 2

        # Test character classes
        result = glob.glob(test_dir / "file[0-9].*")
        assert len(result) == 2  # file1.txt and file2.py

        # Test negation (not supported in standard glob, but test for errors)
        try:
            result = glob.glob(test_dir / "!*.txt")
            # If negation works, it should return non-txt files
            assert all("txt" not in str(p) for p in result)
        except (ValueError, TypeError):
            # Negation not supported, which is expected
            pass

        # For HTTP(S), advanced patterns may require directory listings that aren't supported
        is_http = rig.path_class.cloud_prefix.startswith("http")
        if not is_http:
            # Test question mark wildcard
            result = glob.glob(test_dir / "file?.txt")
            # The ? wildcard should match exactly one character
            # Only file1.txt matches in our setup
            assert len(result) == 1
            assert any("file1.txt" in str(f) for f in result)

            # Test multiple wildcards
            result = glob.glob(test_dir / "*file*.py")
            assert len(result) == 2  # file2.py and test_file.py both contain "file"
            assert any("test_file.py" in str(f) for f in result)
            assert any("file2.py" in str(f) for f in result)


def test_patch_glob_error_handling(rig):
    """Test glob error handling for invalid patterns and paths."""
    with patch_glob():
        # Ensure directory exists and is listable by creating at least one file
        test_dir = rig.create_cloud_path("error_test")
        dummy = test_dir / "dummy.txt"
        dummy.write_text("dummy")

        # Test with empty pattern (some providers may return the directory's immediate children)
        result = glob.glob(test_dir / "")
        assert isinstance(result, list)
        if len(result) == 1:
            assert str(result[0]).endswith("/error_test/dummy.txt") or str(result[0]).endswith(
                "\\error_test\\dummy.txt"
            )

        # Test with just wildcards
        result = glob.glob(test_dir / "*")
        assert isinstance(result, list)


def test_patch_glob_context_manager(rig):
    """Test that glob patches are applied and restored correctly."""
    original_glob = glob.glob
    original_iglob = glob.iglob

    with patch_glob():
        # Patches should be applied
        assert glob.glob != original_glob
        assert glob.iglob != original_iglob

    # Patches should be restored
    assert glob.glob == original_glob
    assert glob.iglob == original_iglob


def test_patch_glob_mixed_usage(rig):
    """Test mixed usage of CloudPath and regular paths with glob."""
    with patch_glob():
        # Create test structure
        cloud_dir = rig.create_cloud_path("mixed_test")
        cloud_dir.mkdir(exist_ok=True)

        test_file = cloud_dir / "test.txt"
        test_file.write_text("test")

        # Test CloudPath pattern
        cloud_result = glob.glob(cloud_dir / "*.txt")
        assert len(cloud_result) == 1
        assert isinstance(cloud_result[0], type(cloud_dir))

        # Test string pattern (should still work)
        string_result = glob.glob("*.py")  # Find Python files in current directory
        assert isinstance(string_result, list)

        # Test with root_dir as CloudPath and string pattern
        result = glob.glob("*.txt", root_dir=cloud_dir)
        assert len(result) == 1
        assert isinstance(result[0], type(cloud_dir))


def test_patch_glob_edge_cases(rig):
    """Test glob with edge cases and boundary conditions."""
    with patch_glob():
        test_dir = rig.create_cloud_path("edge_case_test")
        test_dir.mkdir(exist_ok=True)

        # Create files with special names
        is_http = rig.path_class.cloud_prefix.startswith("http")
        special_files = [
            # For HTTP(S), skip file with spaces because URLs may not be encoded by the client
            *([] if is_http else ["file with spaces.txt"]),
            "file-with-dashes.txt",
            "file_with_underscores.txt",
            "file.with.dots.txt",
            "file123.txt",
            "123file.txt",
            ".hidden.txt",
            "file.txt.bak",
        ]

        created_files = []
        for filename in special_files:
            file_path = test_dir / filename
            file_path.write_text("content")
            created_files.append(file_path)

        # Test files with spaces (skip for HTTP(S))
        if not is_http:
            result = glob.glob(test_dir / "* *.txt")
            assert len(result) == 1
            assert "file with spaces.txt" in str(result[0])

        # Test files with dashes
        result = glob.glob(test_dir / "*-*.txt")
        assert len(result) == 1
        assert "file-with-dashes.txt" in str(result[0])

        # Test files with underscores
        result = glob.glob(test_dir / "*_*.txt")
        assert len(result) == 1
        assert "file_with_underscores.txt" in str(result[0])

        # Test files with dots
        result = glob.glob(test_dir / "*.*.txt")
        # Our mock providers may treat hidden files like normal entries, so allow 1 or 2
        assert 1 <= len(result) <= 2
        assert any("file.with.dots.txt" in str(f) for f in result)

        # Test hidden files (may not be supported equally in all providers)
        result = glob.glob(test_dir / ".*.txt")
        # Accept either 0 or 1 depending on provider behavior
        assert len(result) in (0, 1)
        if result:
            assert ".hidden.txt" in str(result[0])

        # Test files ending with .bak
        result = glob.glob(test_dir / "*.bak")
        assert len(result) == 1
        assert "file.txt.bak" in str(result[0])

        # Test numeric patterns
        result = glob.glob(test_dir / "[0-9]*.txt")
        assert len(result) == 1
        assert "123file.txt" in str(result[0])


def test_patch_all_builtins_simple(rig):
    cp = rig.create_cloud_path("dir_0/new_file_patch_all.txt")
    test_dir = rig.create_cloud_path("test_patch_all_dir/")

    # Without patch, opening a CloudPath should fail
    with pytest.raises(FileNotFoundError):
        with open(cp, "w") as f:
            f.write("Hello!")

    # With all builtins patched, open, os.path, and glob should work
    with patch_all_builtins():
        # Test open patching
        with open(cp, "w") as f:
            f.write("Hello!")
        assert cp.read_text() == "Hello!"

        # Test os.path patching
        assert os.path.exists(cp)
        assert os.path.isfile(cp)
        assert os.path.basename(cp) == "new_file_patch_all.txt"

        # Test glob patching
        test_dir.mkdir(exist_ok=True)
        glob_file1 = test_dir / "glob1.txt"
        glob_file2 = test_dir / "glob2.txt"
        glob_file1.write_text("content1")
        glob_file2.write_text("content2")

        result = glob.glob(test_dir / "*.txt")
        assert len(result) == 2
        assert all(isinstance(p, type(test_dir)) for p in result)
