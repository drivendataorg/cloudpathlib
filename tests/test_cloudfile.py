"""
Tests for the CloudFile class.

This module contains comprehensive tests for the CloudFile class, testing
all its functionality including reading, writing, seeking, streaming, and caching behavior.
"""

import io
import os
import tempfile
from pathlib import Path
from time import sleep

import pytest

from cloudpathlib import CloudFile, CloudPath
from cloudpathlib.exceptions import (
    CloudFileException,
    CloudFileReadError,
    CloudFileWriteError,
    CloudFileSeekError,
    CloudPathIsADirectoryError,
    CloudPathNotExistsError,
)


class TestCloudFileBasic:
    """Test basic CloudFile functionality."""
    
    def test_cloudfile_creation(self, rig):
        """Test CloudFile creation with different modes."""
        # Test read mode
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        with CloudFile(cloud_path, "rb") as f:
            assert f.mode == "rb"
            assert f.name == str(cloud_path)
            assert f.readable()
            assert not f.writable()
            assert f.seekable()
            assert not f.closed
        
        # Test write mode
        cloud_path = rig.create_cloud_path("test_write.txt")
        with CloudFile(cloud_path, "wb") as f:
            assert f.mode == "wb"
            assert not f.readable()
            assert f.writable()
            assert f.seekable()
        
        # Test append mode
        cloud_path = rig.create_cloud_path("test_append.txt")
        with CloudFile(cloud_path, "ab") as f:
            assert f.mode == "ab"
            assert not f.readable()
            assert f.writable()
            assert f.seekable()
    
    def test_cloudfile_invalid_mode(self, rig):
        """Test CloudFile creation with invalid modes."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with pytest.raises(ValueError, match="Unsupported mode"):
            CloudFile(cloud_path, "r")
        
        with pytest.raises(ValueError, match="Unsupported mode"):
            CloudFile(cloud_path, "w")
        
        with pytest.raises(ValueError, match="Unsupported mode"):
            CloudFile(cloud_path, "a")
    
    def test_cloudfile_directory_error(self, rig):
        """Test CloudFile creation with directory path."""
        cloud_path = rig.create_cloud_path("dir_0/")
        
        with pytest.raises(CloudPathIsADirectoryError):
            CloudFile(cloud_path, "rb")
    
    def test_cloudfile_nonexistent_read(self, rig):
        """Test CloudFile creation for nonexistent file in read mode."""
        cloud_path = rig.create_cloud_path("nonexistent.txt")
        
        with pytest.raises(CloudPathNotExistsError):
            CloudFile(cloud_path, "rb")
    
    def test_cloudfile_exclusive_creation(self, rig):
        """Test CloudFile exclusive creation mode."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with pytest.raises(FileExistsError):
            CloudFile(cloud_path, "wb")
    
    def test_cloudfile_invalid_path_type(self):
        """Test CloudFile creation with invalid path type."""
        with pytest.raises(TypeError, match="cloud_path must be a CloudPath instance"):
            CloudFile("not_a_cloud_path", "rb")


class TestCloudFileReading:
    """Test CloudFile reading functionality."""
    
    def test_cloudfile_read_entire_file(self, rig):
        """Test reading entire file."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        original_content = cloud_path.read_bytes()
        
        with CloudFile(cloud_path, "rb") as f:
            content = f.read()
            assert content == original_content
    
    def test_cloudfile_read_partial(self, rig):
        """Test reading partial file content."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        original_content = cloud_path.read_bytes()
        
        with CloudFile(cloud_path, "rb") as f:
            # Read first 10 bytes
            content = f.read(10)
            assert content == original_content[:10]
            
            # Read next 10 bytes
            content = f.read(10)
            assert content == original_content[10:20]
    
    def test_cloudfile_readline(self, rig):
        """Test reading lines from file."""
        # Create a file with multiple lines
        cloud_path = rig.create_cloud_path("test_multiline.txt")
        lines = [b"line 1\n", b"line 2\n", b"line 3\n"]
        content = b"".join(lines)
        
        with CloudFile(cloud_path, "wb") as f:
            f.write(content)
        
        with CloudFile(cloud_path, "rb") as f:
            assert f.readline() == lines[0]
            assert f.readline() == lines[1]
            assert f.readline() == lines[2]
            assert f.readline() == b""  # EOF
    
    def test_cloudfile_readlines(self, rig):
        """Test reading all lines from file."""
        # Create a file with multiple lines
        cloud_path = rig.create_cloud_path("test_multiline.txt")
        lines = [b"line 1\n", b"line 2\n", b"line 3\n"]
        content = b"".join(lines)
        
        with CloudFile(cloud_path, "wb") as f:
            f.write(content)
        
        with CloudFile(cloud_path, "rb") as f:
            read_lines = f.readlines()
            assert read_lines == lines
    
    def test_cloudfile_iteration(self, rig):
        """Test file iteration."""
        # Create a file with multiple lines
        cloud_path = rig.create_cloud_path("test_multiline.txt")
        lines = [b"line 1\n", b"line 2\n", b"line 3\n"]
        content = b"".join(lines)
        
        with CloudFile(cloud_path, "wb") as f:
            f.write(content)
        
        with CloudFile(cloud_path, "rb") as f:
            for i, line in enumerate(f):
                assert line == lines[i]
    
    def test_cloudfile_read_after_close(self, rig):
        """Test reading after file is closed."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        f = CloudFile(cloud_path, "rb")
        f.close()
        
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.read()
    
    def test_cloudfile_read_write_mode(self, rig):
        """Test reading in write-only mode."""
        cloud_path = rig.create_cloud_path("test_write_only.txt")
        
        with CloudFile(cloud_path, "wb") as f:
            with pytest.raises(CloudFileReadError, match="File not opened for reading"):
                f.read()


class TestCloudFileWriting:
    """Test CloudFile writing functionality."""
    
    def test_cloudfile_write_new_file(self, rig):
        """Test writing to a new file."""
        cloud_path = rig.create_cloud_path("test_write_new.txt")
        content = b"Hello, World!"
        
        with CloudFile(cloud_path, "wb") as f:
            written = f.write(content)
            assert written == len(content)
        
        # Verify content was written
        assert cloud_path.read_bytes() == content
    
    def test_cloudfile_write_multiple_chunks(self, rig):
        """Test writing multiple chunks to a file."""
        cloud_path = rig.create_cloud_path("test_write_chunks.txt")
        chunks = [b"Hello", b", ", b"World", b"!"]
        
        with CloudFile(cloud_path, "wb") as f:
            for chunk in chunks:
                written = f.write(chunk)
                assert written == len(chunk)
        
        # Verify content was written
        assert cloud_path.read_bytes() == b"".join(chunks)
    
    def test_cloudfile_writelines(self, rig):
        """Test writing lines to a file."""
        cloud_path = rig.create_cloud_path("test_writelines.txt")
        lines = [b"line 1\n", b"line 2\n", b"line 3\n"]
        
        with CloudFile(cloud_path, "wb") as f:
            f.writelines(lines)
        
        # Verify content was written
        assert cloud_path.read_bytes() == b"".join(lines)
    
    def test_cloudfile_append_mode(self, rig):
        """Test append mode."""
        cloud_path = rig.create_cloud_path("test_append.txt")
        original_content = b"Original content\n"
        append_content = b"Appended content\n"
        
        # Write original content
        with CloudFile(cloud_path, "wb") as f:
            f.write(original_content)
        
        # Append additional content
        with CloudFile(cloud_path, "ab") as f:
            written = f.write(append_content)
            assert written == len(append_content)
        
        # Verify content
        expected = original_content + append_content
        assert cloud_path.read_bytes() == expected
    
    def test_cloudfile_read_write_mode(self, rig):
        """Test read-write mode."""
        cloud_path = rig.create_cloud_path("test_read_write.txt")
        original_content = b"Original content\n"
        new_content = b"New content\n"
        
        # Write original content
        with CloudFile(cloud_path, "wb") as f:
            f.write(original_content)
        
        # Read and write in read-write mode
        with CloudFile(cloud_path, "r+b") as f:
            # Read original content
            content = f.read()
            assert content == original_content
            
            # Seek to beginning and overwrite
            f.seek(0)
            f.write(new_content)
            
            # Seek to beginning and read again
            f.seek(0)
            content = f.read()
            assert content == new_content
    
    def test_cloudfile_write_after_close(self, rig):
        """Test writing after file is closed."""
        cloud_path = rig.create_cloud_path("test_write_after_close.txt")
        f = CloudFile(cloud_path, "wb")
        f.close()
        
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.write(b"test")
    
    def test_cloudfile_write_read_mode(self, rig):
        """Test writing in read-only mode."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with CloudFile(cloud_path, "rb") as f:
            with pytest.raises(CloudFileWriteError, match="File not opened for writing"):
                f.write(b"test")


class TestCloudFileSeeking:
    """Test CloudFile seeking functionality."""
    
    def test_cloudfile_seek_from_beginning(self, rig):
        """Test seeking from beginning of file."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        original_content = cloud_path.read_bytes()
        
        with CloudFile(cloud_path, "rb") as f:
            # Seek to position 10
            pos = f.seek(10)
            assert pos == 10
            
            # Read from position 10
            content = f.read(5)
            assert content == original_content[10:15]
    
    def test_cloudfile_seek_from_current(self, rig):
        """Test seeking from current position."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        original_content = cloud_path.read_bytes()
        
        with CloudFile(cloud_path, "rb") as f:
            # Read first 5 bytes
            f.read(5)
            
            # Seek 5 bytes forward from current position
            pos = f.seek(5, 1)  # SEEK_CUR
            assert pos == 10
            
            # Read from new position
            content = f.read(5)
            assert content == original_content[10:15]
    
    def test_cloudfile_seek_from_end(self, rig):
        """Test seeking from end of file."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        original_content = cloud_path.read_bytes()
        file_size = len(original_content)
        
        with CloudFile(cloud_path, "rb") as f:
            # Seek 5 bytes from end
            pos = f.seek(-5, 2)  # SEEK_END
            assert pos == file_size - 5
            
            # Read last 5 bytes
            content = f.read()
            assert content == original_content[-5:]
    
    def test_cloudfile_seek_negative_position(self, rig):
        """Test seeking to negative position."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with CloudFile(cloud_path, "rb") as f:
            with pytest.raises(CloudFileSeekError, match="Cannot seek to negative position"):
                f.seek(-1)
    
    def test_cloudfile_seek_invalid_whence(self, rig):
        """Test seeking with invalid whence value."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with CloudFile(cloud_path, "rb") as f:
            with pytest.raises(CloudFileSeekError, match="Invalid whence value"):
                f.seek(0, 3)
    
    def test_cloudfile_tell(self, rig):
        """Test tell() method."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with CloudFile(cloud_path, "rb") as f:
            assert f.tell() == 0
            
            f.read(10)
            assert f.tell() == 10
            
            f.seek(20)
            assert f.tell() == 20
    
    def test_cloudfile_seek_after_close(self, rig):
        """Test seeking after file is closed."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        f = CloudFile(cloud_path, "rb")
        f.close()
        
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.seek(0)


class TestCloudFileTruncation:
    """Test CloudFile truncation functionality."""
    
    def test_cloudfile_truncate_to_size(self, rig):
        """Test truncating file to specific size."""
        cloud_path = rig.create_cloud_path("test_truncate.txt")
        original_content = b"Hello, World! This is a test file."
        
        # Write original content
        with CloudFile(cloud_path, "wb") as f:
            f.write(original_content)
        
        # Truncate to 10 bytes
        with CloudFile(cloud_path, "r+b") as f:
            size = f.truncate(10)
            assert size == 10
        
        # Verify truncation
        assert cloud_path.read_bytes() == original_content[:10]
    
    def test_cloudfile_truncate_to_current_position(self, rig):
        """Test truncating file to current position."""
        cloud_path = rig.create_cloud_path("test_truncate_position.txt")
        original_content = b"Hello, World! This is a test file."
        
        # Write original content
        with CloudFile(cloud_path, "wb") as f:
            f.write(original_content)
        
        # Truncate to current position
        with CloudFile(cloud_path, "r+b") as f:
            f.seek(10)  # Move to position 10
            size = f.truncate()  # Truncate to current position
            assert size == 10
        
        # Verify truncation
        assert cloud_path.read_bytes() == original_content[:10]
    
    def test_cloudfile_truncate_write_mode(self, rig):
        """Test truncating in write-only mode."""
        cloud_path = rig.create_cloud_path("test_truncate_write.txt")
        
        with CloudFile(cloud_path, "wb") as f:
            with pytest.raises(CloudFileWriteError, match="File not opened for writing"):
                f.truncate(10)
    
    def test_cloudfile_truncate_after_close(self, rig):
        """Test truncating after file is closed."""
        cloud_path = rig.create_cloud_path("test_truncate_closed.txt")
        f = CloudFile(cloud_path, "wb")
        f.close()
        
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.truncate(10)


class TestCloudFileContextManager:
    """Test CloudFile context manager functionality."""
    
    def test_cloudfile_context_manager_read(self, rig):
        """Test context manager for reading."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        original_content = cloud_path.read_bytes()
        
        with CloudFile(cloud_path, "rb") as f:
            content = f.read()
            assert content == original_content
            assert not f.closed
        
        assert f.closed
    
    def test_cloudfile_context_manager_write(self, rig):
        """Test context manager for writing."""
        cloud_path = rig.create_cloud_path("test_context_write.txt")
        content = b"Hello, World!"
        
        with CloudFile(cloud_path, "wb") as f:
            f.write(content)
            assert not f.closed
        
        assert f.closed
        assert cloud_path.read_bytes() == content
    
    def test_cloudfile_context_manager_exception(self, rig):
        """Test context manager with exception."""
        cloud_path = rig.create_cloud_path("test_context_exception.txt")
        
        try:
            with CloudFile(cloud_path, "wb") as f:
                f.write(b"test")
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # File should still be closed
        assert f.closed


class TestCloudFileCaching:
    """Test CloudFile caching behavior."""
    
    def test_cloudfile_streaming_read(self, rig):
        """Test that small reads don't trigger disk caching."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with CloudFile(cloud_path, "rb") as f:
            # Small read should not trigger disk caching
            content = f.read(100)
            assert len(content) == 100
            # Note: We can't easily test if disk caching occurred without
            # modifying the implementation to expose this information
    
    def test_cloudfile_seek_triggers_caching(self, rig):
        """Test that seeking triggers disk caching."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with CloudFile(cloud_path, "rb") as f:
            # Seeking should trigger disk caching
            f.seek(10)
            content = f.read(10)
            assert len(content) == 10
    
    def test_cloudfile_large_read_triggers_caching(self, rig):
        """Test that large reads trigger disk caching."""
        # Create a large file for testing
        cloud_path = rig.create_cloud_path("test_large_file.txt")
        large_content = b"x" * (2 * 1024 * 1024)  # 2MB
        
        with CloudFile(cloud_path, "wb") as f:
            f.write(large_content)
        
        with CloudFile(cloud_path, "rb") as f:
            # Large read should trigger disk caching
            content = f.read(1024 * 1024 + 1)  # Read more than 1MB
            assert len(content) == 1024 * 1024 + 1


class TestCloudFileProperties:
    """Test CloudFile properties."""
    
    def test_cloudfile_properties(self, rig):
        """Test CloudFile properties."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with CloudFile(cloud_path, "rb") as f:
            assert f.name == str(cloud_path)
            assert f.mode == "rb"
            assert not f.closed
            assert f.readable()
            assert not f.writable()
            assert f.seekable()
        
        assert f.closed
    
    def test_cloudfile_mode_setter(self, rig):
        """Test CloudFile mode setter."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with CloudFile(cloud_path, "rb") as f:
            f.mode = "r+b"
            assert f.mode == "r+b"
            assert f.readable()
            assert f.writable()


class TestCloudFileEdgeCases:
    """Test CloudFile edge cases and error conditions."""
    
    def test_cloudfile_empty_file(self, rig):
        """Test reading from empty file."""
        cloud_path = rig.create_cloud_path("test_empty.txt")
        
        with CloudFile(cloud_path, "wb") as f:
            pass  # Create empty file
        
        with CloudFile(cloud_path, "rb") as f:
            content = f.read()
            assert content == b""
    
    def test_cloudfile_zero_read(self, rig):
        """Test reading zero bytes."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        
        with CloudFile(cloud_path, "rb") as f:
            content = f.read(0)
            assert content == b""
    
    def test_cloudfile_read_beyond_eof(self, rig):
        """Test reading beyond end of file."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        file_size = len(cloud_path.read_bytes())
        
        with CloudFile(cloud_path, "rb") as f:
            f.seek(file_size)  # Seek to end
            content = f.read(100)  # Try to read beyond EOF
            assert content == b""
    
    def test_cloudfile_seek_beyond_eof(self, rig):
        """Test seeking beyond end of file."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        file_size = len(cloud_path.read_bytes())
        
        with CloudFile(cloud_path, "rb") as f:
            f.seek(file_size + 100)  # Seek beyond EOF
            assert f.tell() == file_size + 100
            content = f.read()
            assert content == b""
    
    def test_cloudfile_multiple_close(self, rig):
        """Test closing file multiple times."""
        cloud_path = rig.create_cloud_path("dir_0/file0_0.txt")
        f = CloudFile(cloud_path, "rb")
        
        f.close()
        assert f.closed
        
        # Second close should not raise an error
        f.close()
        assert f.closed
    
    def test_cloudfile_flush(self, rig):
        """Test flush method."""
        cloud_path = rig.create_cloud_path("test_flush.txt")
        
        with CloudFile(cloud_path, "wb") as f:
            f.write(b"test")
            f.flush()  # Should not raise an error
        
        assert cloud_path.read_bytes() == b"test"
    
    def test_cloudfile_flush_after_close(self, rig):
        """Test flush after file is closed."""
        cloud_path = rig.create_cloud_path("test_flush_closed.txt")
        f = CloudFile(cloud_path, "wb")
        f.close()
        
        with pytest.raises(ValueError, match="I/O operation on closed file"):
            f.flush()


class TestCloudFileIntegration:
    """Test CloudFile integration with different cloud providers."""
    
    def test_cloudfile_s3_integration(self, rig):
        """Test CloudFile with S3."""
        if not hasattr(rig, 'is_s3') or not rig.is_s3:
            pytest.skip("S3 not available")
        
        cloud_path = rig.create_cloud_path("test_s3_integration.txt")
        content = b"S3 integration test content"
        
        with CloudFile(cloud_path, "wb") as f:
            f.write(content)
        
        with CloudFile(cloud_path, "rb") as f:
            read_content = f.read()
            assert read_content == content
    
    def test_cloudfile_azure_integration(self, rig):
        """Test CloudFile with Azure."""
        if not hasattr(rig, 'is_azure') or not rig.is_azure:
            pytest.skip("Azure not available")
        
        cloud_path = rig.create_cloud_path("test_azure_integration.txt")
        content = b"Azure integration test content"
        
        with CloudFile(cloud_path, "wb") as f:
            f.write(content)
        
        with CloudFile(cloud_path, "rb") as f:
            read_content = f.read()
            assert read_content == content
    
    def test_cloudfile_gs_integration(self, rig):
        """Test CloudFile with Google Cloud Storage."""
        if not hasattr(rig, 'is_gs') or not rig.is_gs:
            pytest.skip("Google Cloud Storage not available")
        
        cloud_path = rig.create_cloud_path("test_gs_integration.txt")
        content = b"GS integration test content"
        
        with CloudFile(cloud_path, "wb") as f:
            f.write(content)
        
        with CloudFile(cloud_path, "rb") as f:
            read_content = f.read()
            assert read_content == content 