"""
Tests for cloud storage streaming I/O.

Tests CloudBufferedIO, CloudTextIO, and streaming mode for direct
streaming without local caching.
"""

import io
import threading
import time
import zipfile
import pytest

from cloudpathlib import S3Path, AzureBlobPath, GSPath
from cloudpathlib import CloudBufferedIO, CloudTextIO
from cloudpathlib.cloud_io import _CloudStorageRaw
from cloudpathlib.enums import FileCacheMode
from cloudpathlib.exceptions import (
    CloudPathFileNotFoundError,
    CloudPathNotImplementedError,
    OverwriteNewerCloudError,
)

# Sample test data
BINARY_DATA = b"Hello, World! This is binary data.\n" * 100
TEXT_DATA = "Hello, World! This is text data.\n" * 100
MULTILINE_TEXT = """Line 1
Line 2
Line 3
Line 4 with special chars: éñ中文
"""


@pytest.fixture
def temp_cloud_file(rig):
    """Create a temporary cloud file for testing."""
    # Skip if streaming IO is not implemented for this provider
    # HTTP/HTTPS support streaming reads and the test server supports writes
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://", "http://", "https://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_streaming_io.txt")
    path.write_text(TEXT_DATA)
    # Set client to streaming mode
    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming
    yield path
    # Restore original mode
    path.client.file_cache_mode = original_mode
    try:
        path.unlink()
    except Exception:
        pass


@pytest.fixture
def temp_cloud_binary_file(rig):
    """Create a temporary cloud binary file for testing."""
    # Skip if streaming IO is not implemented for this provider
    # HTTP/HTTPS support streaming reads and the test server supports writes
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://", "http://", "https://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_streaming_io.bin")
    path.write_bytes(BINARY_DATA)
    # Set client to streaming mode
    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming
    yield path
    # Restore original mode
    path.client.file_cache_mode = original_mode
    try:
        path.unlink()
    except Exception:
        pass


@pytest.fixture
def temp_cloud_multiline_file(rig):
    """Create a temporary cloud file with multiple lines for testing."""
    # Skip if streaming IO is not implemented for this provider
    # HTTP/HTTPS support streaming reads and the test server supports writes
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://", "http://", "https://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_streaming_multiline.txt")
    path.write_text(MULTILINE_TEXT, encoding="utf-8")
    # Set client to streaming mode
    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming
    yield path
    # Restore original mode
    path.client.file_cache_mode = original_mode
    try:
        path.unlink()
    except Exception:
        pass


# ============================================================================
# CloudBufferedIO tests (binary streaming)
# ============================================================================


def test_read_binary_stream(temp_cloud_binary_file):
    """Test reading binary data via streaming."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        # Verify it's the right type
        assert isinstance(f, CloudBufferedIO)
        assert isinstance(f, io.BufferedIOBase)

        # Read all data
        data = f.read()
        assert data == BINARY_DATA


def test_read_chunks(temp_cloud_binary_file):
    """Test reading data in chunks."""
    chunk_size = 100
    with temp_cloud_binary_file.open(mode="rb") as f:
        chunks = []
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
            assert len(chunk) <= chunk_size

        # Verify we got all data
        assert b"".join(chunks) == BINARY_DATA


def test_read1(temp_cloud_binary_file):
    """Test read1 method."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        chunk = f.read1(50)
        assert len(chunk) <= 50
        assert len(chunk) > 0


def test_readinto(temp_cloud_binary_file):
    """Test readinto method."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        buf = bytearray(100)
        n = f.readinto(buf)
        assert n > 0
        assert n <= 100
        assert buf[:n] == BINARY_DATA[:n]


def test_seek_tell(temp_cloud_binary_file):
    """Test seek and tell operations."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        # Initial position
        assert f.tell() == 0

        # Read some data
        f.read(50)
        assert f.tell() == 50

        # Seek to beginning
        pos = f.seek(0)
        assert pos == 0
        assert f.tell() == 0

        # Seek relative
        pos = f.seek(10, io.SEEK_CUR)
        assert pos == 10

        # Seek from end
        pos = f.seek(-10, io.SEEK_END)
        assert pos == len(BINARY_DATA) - 10


def test_seekable_readable_writable(temp_cloud_binary_file):
    """Test capability flags."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        assert f.readable()
        assert not f.writable()
        assert f.seekable()


def test_buffered_io_properties(temp_cloud_binary_file):
    """Test file properties."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        assert f.name == str(temp_cloud_binary_file)
        assert f.mode == "rb"
        assert not f.closed

    assert f.closed


def test_buffered_io_context_manager(temp_cloud_binary_file):
    """Test context manager protocol."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        assert not f.closed
        data = f.read(10)
        assert len(data) == 10

    assert f.closed


def test_write_binary_stream(rig):
    """Test writing binary data via streaming."""
    # Skip if streaming IO is not implemented for this provider
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://", "http://", "https://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_write_binary.bin")

    try:
        # Set client to streaming mode
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        # Write data
        with path.open(mode="wb") as f:
            assert isinstance(f, CloudBufferedIO)
            assert f.writable()
            assert not f.readable()

            n = f.write(BINARY_DATA)
            assert n == len(BINARY_DATA)

        # Restore original mode
        path.client.file_cache_mode = original_mode

        # Verify data was written
        assert path.exists()
        assert path.read_bytes() == BINARY_DATA
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_write_chunks(rig):
    """Test writing data in chunks."""
    # Skip if streaming IO is not implemented for this provider
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://", "http://", "https://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_write_chunks.bin")

    try:
        # Set client to streaming mode
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        chunk_size = 100
        with path.open(mode="wb", buffer_size=chunk_size) as f:
            for i in range(0, len(BINARY_DATA), chunk_size):
                chunk = BINARY_DATA[i : i + chunk_size]
                f.write(chunk)

        # Restore original mode
        path.client.file_cache_mode = original_mode

        # Verify
        assert path.read_bytes() == BINARY_DATA
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_flush(rig):
    """Test explicit flush."""
    # Skip if streaming IO is not implemented for this provider
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_flush.bin")

    try:
        # Set client to streaming mode
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        with path.open(mode="wb") as f:
            f.write(b"First chunk")
            f.flush()
            f.write(b" Second chunk")

        # Restore original mode
        path.client.file_cache_mode = original_mode

        assert path.read_bytes() == b"First chunk Second chunk"
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_buffered_io_isinstance_checks(temp_cloud_binary_file):
    """Test that instances pass isinstance checks."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        assert isinstance(f, io.IOBase)
        assert isinstance(f, io.BufferedIOBase)
        assert not isinstance(f, io.TextIOBase)


def test_not_found_error(rig):
    """Test error when file doesn't exist."""
    path = rig.create_cloud_path("nonexistent.bin")

    with pytest.raises(FileNotFoundError):
        with path.open(mode="rb") as f:
            f.read()


# ============================================================================
# CloudTextIO tests (text streaming)
# ============================================================================


def test_read_text_stream(temp_cloud_file):
    """Test reading text data via streaming."""
    with temp_cloud_file.open(mode="rt") as f:
        # Verify it's the right type
        assert isinstance(f, CloudTextIO)
        assert isinstance(f, io.TextIOBase)

        # Read all data
        data = f.read()
        assert data == TEXT_DATA


def test_read_text_mode_without_t(temp_cloud_file):
    """Test reading text with mode 'r' (without explicit 't')."""
    with temp_cloud_file.open(mode="r") as f:
        assert isinstance(f, CloudTextIO)
        data = f.read()
        assert data == TEXT_DATA


def test_readline(temp_cloud_multiline_file):
    """Test readline method."""
    with temp_cloud_multiline_file.open(mode="rt", encoding="utf-8") as f:
        line1 = f.readline()
        assert line1 == "Line 1\n"

        line2 = f.readline()
        assert line2 == "Line 2\n"


def test_readlines(temp_cloud_multiline_file):
    """Test readlines method."""
    with temp_cloud_multiline_file.open(mode="rt", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) == 4
        assert lines[0] == "Line 1\n"
        assert "special chars" in lines[3]


def test_iteration(temp_cloud_multiline_file):
    """Test iterating over lines."""
    with temp_cloud_multiline_file.open(mode="rt", encoding="utf-8") as f:
        lines = list(f)
        assert len(lines) == 4
        assert lines[0] == "Line 1\n"


def test_encoding(rig):
    """Test different encodings."""
    path = rig.create_cloud_path("test_encoding.txt")
    utf8_text = "Hello 世界 🌍"

    try:
        # Write with UTF-8
        path.write_text(utf8_text, encoding="utf-8")

        # Read with UTF-8
        with path.open(mode="rt", encoding="utf-8") as f:
            assert f.encoding == "utf-8"
            data = f.read()
            assert data == utf8_text
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_text_properties(temp_cloud_file):
    """Test text mode properties."""
    with temp_cloud_file.open(mode="rt", encoding="utf-8", errors="strict") as f:
        assert f.encoding == "utf-8"
        assert f.errors == "strict"
        assert f.name == str(temp_cloud_file)
        assert "r" in f.mode


def test_write_text_stream(rig):
    """Test writing text data via streaming."""
    # Skip if streaming IO is not implemented for this provider
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://", "http://", "https://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_write_text.txt")

    try:
        # Set client to streaming mode
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        with path.open(mode="wt") as f:
            assert isinstance(f, CloudTextIO)
            n = f.write(TEXT_DATA)
            assert n == len(TEXT_DATA)

        # Restore original mode
        path.client.file_cache_mode = original_mode

        # Verify
        assert path.read_text() == TEXT_DATA
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_writelines(rig):
    """Test writelines method."""
    # Skip if streaming IO is not implemented for this provider
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_writelines.txt")
    lines = ["Line 1\n", "Line 2\n", "Line 3\n"]

    try:
        # Set client to streaming mode
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        with path.open(mode="wt") as f:
            f.writelines(lines)

        # Restore original mode
        path.client.file_cache_mode = original_mode

        assert path.read_text() == "".join(lines)
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_text_io_isinstance_checks(temp_cloud_file):
    """Test that instances pass isinstance checks."""
    with temp_cloud_file.open(mode="rt") as f:
        assert isinstance(f, io.IOBase)
        assert isinstance(f, io.TextIOBase)
        assert not isinstance(f, io.BufferedIOBase)


def test_buffer_property(temp_cloud_file):
    """Test access to underlying binary buffer."""
    with temp_cloud_file.open(mode="rt") as f:
        assert hasattr(f, "buffer")
        assert isinstance(f.buffer, CloudBufferedIO)


# ============================================================================
# CloudPath.open streaming integration tests
# ============================================================================


def test_cloudpath_stream_read(temp_cloud_file):
    """Test CloudPath.open with streaming mode for reading."""
    # The temp_cloud_file fixture already sets streaming mode
    with temp_cloud_file.open(mode="r") as f:
        assert isinstance(f, CloudTextIO)
        data = f.read()
        assert data == TEXT_DATA


def test_cloudpath_stream_write(rig):
    """Test CloudPath.open with streaming mode for writing."""
    # Skip if streaming IO is not implemented for this provider
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_stream_write.txt")

    try:
        # Set client to streaming mode
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        with path.open(mode="w") as f:
            assert isinstance(f, CloudTextIO)
            f.write(TEXT_DATA)

        # Restore original mode
        path.client.file_cache_mode = original_mode

        assert path.read_text() == TEXT_DATA
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_cloudpath_stream_binary(temp_cloud_binary_file):
    """Test CloudPath.open with streaming mode for binary."""
    # The temp_cloud_binary_file fixture already sets streaming mode
    with temp_cloud_binary_file.open(mode="rb") as f:
        assert isinstance(f, CloudBufferedIO)
        data = f.read()
        assert data == BINARY_DATA


def test_cloudpath_stream_false_uses_cache(rig):
    """Test that non-streaming mode uses traditional caching."""
    # Skip if streaming IO is not implemented for this provider
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_caching.txt")
    path.write_text(TEXT_DATA)

    try:
        # Default mode (not streaming) should use caching
        assert path.client.file_cache_mode != FileCacheMode.streaming

        with path.open(mode="r") as f:
            # Should not be a CloudTextIO instance
            assert not isinstance(f, CloudTextIO)
            # Should still read correctly
            data = f.read()
            assert data == TEXT_DATA
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_cloudpath_default_no_streaming(rig):
    """Test that default behavior uses caching, not streaming."""
    # Skip if streaming IO is not implemented for this provider
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_default.txt")
    path.write_text(TEXT_DATA)

    try:
        # Default client mode should not be streaming
        assert path.client.file_cache_mode != FileCacheMode.streaming

        with path.open(mode="r") as f:
            # Default should not use streaming
            assert not isinstance(f, CloudTextIO)
            data = f.read()
            assert data == TEXT_DATA
    finally:
        try:
            path.unlink()
        except Exception:
            pass


# ============================================================================
# CloudPath.open factory tests
# ============================================================================


def test_auto_client_s3(temp_cloud_file):
    """Test auto-detection of S3 client."""
    if not isinstance(temp_cloud_file, S3Path):
        pytest.skip("Not testing S3")

    with temp_cloud_file.open(mode="rt") as f:
        data = f.read()
        assert len(data) > 0


def test_auto_client_azure(temp_cloud_file):
    """Test auto-detection of Azure client."""
    if not isinstance(temp_cloud_file, AzureBlobPath):
        pytest.skip("Not testing Azure")

    with temp_cloud_file.open(mode="rt") as f:
        data = f.read()
        assert len(data) > 0


def test_auto_client_gs(temp_cloud_file):
    """Test auto-detection of GCS client."""
    if not isinstance(temp_cloud_file, GSPath):
        pytest.skip("Not testing GCS")

    with temp_cloud_file.open(mode="rt") as f:
        data = f.read()
        assert len(data) > 0


def test_explicit_client(temp_cloud_file):
    """Test passing explicit client."""
    with temp_cloud_file.open(mode="rt") as f:
        data = f.read()
        assert len(data) > 0


def test_buffer_size_parameter(temp_cloud_binary_file):
    """Test custom buffer size."""
    buffer_size = 1024
    with temp_cloud_binary_file.open(mode="rb", buffer_size=buffer_size) as f:
        assert f._buffer_size == buffer_size


def test_text_parameters(rig):
    """Test text-specific parameters."""
    path = rig.create_cloud_path("test_params.txt")
    text = "Test data"

    try:
        path.write_text(text)

        with path.open(mode="rt", encoding="utf-8", errors="strict", newline=None) as f:
            assert f.encoding == "utf-8"
            assert f.errors == "strict"
            data = f.read()
            assert data == text
    finally:
        try:
            path.unlink()
        except Exception:
            pass


# ============================================================================
# Edge cases and error conditions
# ============================================================================


def test_empty_file_read(rig):
    """Test reading an empty file."""
    path = rig.create_cloud_path("test_empty.txt")

    try:
        path.write_text("")

        with path.open(mode="rt") as f:
            data = f.read()
            assert data == ""
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_empty_file_write(rig):
    """Test writing an empty file."""
    # Skip if streaming IO is not implemented for this provider
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://", "http://", "https://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_empty_write.txt")

    try:
        # Set client to streaming mode
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        with path.open(mode="wt"):
            pass  # Write nothing

        # Restore original mode
        path.client.file_cache_mode = original_mode

        assert path.exists()
        assert path.read_text() == ""
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_large_file_streaming(rig):
    """Test streaming a larger file."""
    path = rig.create_cloud_path("test_large.bin")
    # 1 MB of data
    large_data = b"X" * (1024 * 1024)

    try:
        path.write_bytes(large_data)

        # Read in chunks
        with path.open(mode="rb", buffer_size=8192) as f:
            chunks = []
            while True:
                chunk = f.read(8192)
                if not chunk:
                    break
                chunks.append(chunk)

            result = b"".join(chunks)
            assert len(result) == len(large_data)
            assert result == large_data
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_seek_beyond_eof(temp_cloud_binary_file):
    """Test seeking beyond end of file."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        # Seek beyond EOF
        size = len(BINARY_DATA)
        pos = f.seek(size + 1000)
        assert pos == size + 1000

        # Reading should return empty
        data = f.read(10)
        assert data == b""


def test_closed_file_operations(temp_cloud_file):
    """Test operations on closed file raise errors."""
    with temp_cloud_file.open(mode="rt") as f:
        pass  # Just open and close

    # Now f is closed
    with pytest.raises(ValueError):
        f.read()

    with pytest.raises(ValueError):
        f.readline()


def test_binary_mode_required_for_buffered(temp_cloud_file):
    """Test that CloudBufferedIO requires binary mode."""
    # Get the raw IO class
    raw_io_class = temp_cloud_file._cloud_meta.raw_io_class
    if raw_io_class is None:
        pytest.skip("No raw IO class registered")

    # This should raise an error
    with pytest.raises(ValueError, match="binary mode"):
        CloudBufferedIO(
            raw_io_class=raw_io_class,
            client=temp_cloud_file.client,
            cloud_path=temp_cloud_file,
            mode="r",
        )


def test_text_mode_required_for_text(temp_cloud_file):
    """Test that CloudTextIO requires text mode."""
    # Get the raw IO class
    raw_io_class = temp_cloud_file._cloud_meta.raw_io_class
    if raw_io_class is None:
        pytest.skip("No raw IO class registered")

    with pytest.raises(ValueError, match="text mode"):
        CloudTextIO(
            raw_io_class=raw_io_class,
            client=temp_cloud_file.client,
            cloud_path=temp_cloud_file,
            mode="rb",
        )


def test_unsupported_operations(temp_cloud_file):
    """Test unsupported operations raise appropriate errors."""
    with temp_cloud_file.open(mode="rt") as f:
        # fileno() should raise
        with pytest.raises(OSError):
            f.fileno()

        # isatty() should return False
        assert not f.isatty()


def test_read_write_mode_not_implemented(temp_cloud_file):
    """Test that read/write modes work as expected."""
    # For now, r+ and w+ may have limitations
    # Test basic write mode
    with temp_cloud_file.open(mode="wt") as f:
        assert f.writable()
        assert not f.readable()


# ============================================================================
# Provider-specific tests
# ============================================================================


def test_s3_multipart_upload(rig):
    """Test that S3 multipart upload is triggered for large writes."""
    if rig.path_class.cloud_prefix != "s3://":
        pytest.skip("Not testing S3")

    path = rig.create_cloud_path("test_multipart.bin")
    # Write enough data to trigger multiple parts (> 64KB buffer)
    large_data = b"X" * (200 * 1024)  # 200 KB

    try:
        with path.open(mode="wb", buffer_size=64 * 1024) as f:
            f.write(large_data)

        # Verify data was uploaded correctly
        assert path.read_bytes() == large_data
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_azure_block_upload(rig):
    """Test that Azure block upload works."""
    if rig.path_class.cloud_prefix != "az://":
        pytest.skip("Not testing Azure")

    path = rig.create_cloud_path("test_blocks.bin")
    data = b"Block data " * 1000

    try:
        with path.open(mode="wb") as f:
            f.write(data)

        assert path.read_bytes() == data
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_gs_multipart_streaming_upload(rig):
    """Test that GCS streaming upload works (XML multipart under the hood)."""
    if rig.path_class.cloud_prefix != "gs://":
        pytest.skip("Not testing GCS")

    path = rig.create_cloud_path("test_resumable.bin")
    data = b"GCS data " * 1000

    try:
        with path.open(mode="wb") as f:
            f.write(data)

        assert path.read_bytes() == data
    finally:
        try:
            path.unlink()
        except Exception:
            pass


# ============================================================================
# Performance and efficiency tests
# ============================================================================


def test_small_buffer_many_reads(temp_cloud_binary_file):
    """Test reading with small buffer size."""
    with temp_cloud_binary_file.open(mode="rb", buffer_size=128) as f:
        data = f.read()
        assert data == BINARY_DATA


def test_large_buffer_few_reads(temp_cloud_binary_file):
    """Test reading with large buffer size."""
    with temp_cloud_binary_file.open(mode="rb", buffer_size=1024 * 1024) as f:
        data = f.read()
        assert data == BINARY_DATA


def test_sequential_reads(temp_cloud_binary_file):
    """Test sequential reading pattern."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        pos = 0
        while pos < len(BINARY_DATA):
            chunk = f.read(100)
            if not chunk:
                break
            assert chunk == BINARY_DATA[pos : pos + 100]
            pos += len(chunk)


def test_random_seeks(temp_cloud_binary_file):
    """Test random seek pattern."""
    positions = [0, 100, 50, 200, 10]

    with temp_cloud_binary_file.open(mode="rb") as f:
        for pos in positions:
            f.seek(pos)
            assert f.tell() == pos
            chunk = f.read(10)
            assert chunk == BINARY_DATA[pos : pos + 10]


# ============================================================================
# Additional coverage tests for error paths and edge cases
# ============================================================================


def test_readinto_on_closed_file(temp_cloud_binary_file):
    """Test readinto on closed file raises ValueError."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        pass

    buf = bytearray(100)
    with pytest.raises(ValueError, match="closed file"):
        f.readinto(buf)


def test_read_on_write_only_file(rig):
    """Test reading from write-only file raises error."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_write_only.bin")

    try:
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        with path.open(mode="wb") as f:
            # Try to read from write-only file
            with pytest.raises(io.UnsupportedOperation):
                f.read()

        path.client.file_cache_mode = original_mode
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_readinto_empty_buffer(temp_cloud_binary_file):
    """Test readinto with empty buffer returns 0."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        buf = bytearray(0)
        n = f.readinto(buf)
        assert n == 0


def test_seek_with_invalid_whence(temp_cloud_binary_file):
    """Test seek with invalid whence raises ValueError."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        with pytest.raises((ValueError, OSError)):
            f.seek(0, 999)  # Invalid whence value


def test_negative_seek_position(temp_cloud_binary_file):
    """Test seeking to negative position raises ValueError."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        with pytest.raises(ValueError, match="negative seek position"):
            f.seek(-10, io.SEEK_SET)


def test_seek_on_closed_file(temp_cloud_binary_file):
    """Test seek on closed file raises ValueError."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        pass

    with pytest.raises(ValueError, match="closed file"):
        f.seek(0)


def test_write_empty_chunks(rig):
    """Test that empty write chunks are handled correctly."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_empty_chunks.bin")

    try:
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        with path.open(mode="wb") as f:
            # Write empty data - should be no-op
            f.write(b"")
            # Write actual data
            f.write(b"real data")
            # Write more empty data
            f.write(b"")

        path.client.file_cache_mode = original_mode
        assert path.read_bytes() == b"real data"
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_write_error_cleanup(rig):
    """A failed part upload must abort rather than commit earlier parts."""
    if rig.path_class.cloud_prefix != "s3://":
        pytest.skip("S3-specific failure injection")

    path = rig.create_cloud_path("test_error_cleanup.bin")
    original_upload_part = path.client._upload_part
    original_abort = path.client._abort_multipart_upload
    abort_calls = []

    def fail_second_part(cloud_path, upload_id, part_number, data):
        if part_number == 2:
            raise RuntimeError("simulated part failure")
        return original_upload_part(cloud_path, upload_id, part_number, data)

    def record_abort(cloud_path, upload_id):
        abort_calls.append(upload_id)
        return original_abort(cloud_path, upload_id)

    try:
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming
        path.client._upload_part = fail_second_part
        path.client._abort_multipart_upload = record_abort

        stream = path.open(mode="wb")
        with pytest.raises(RuntimeError, match="simulated part failure"):
            stream.write(b"x" * (11 * 1024 * 1024))
        with pytest.raises(RuntimeError, match="simulated part failure"):
            stream.close()

        assert len(abort_calls) == 1
        assert not path.exists()
    finally:
        path.client._upload_part = original_upload_part
        path.client._abort_multipart_upload = original_abort
        path.client.file_cache_mode = original_mode
        try:
            path.unlink()
        except Exception:
            pass


def test_http_write_empty_file(rig):
    """Test HTTP write for empty file."""
    if rig.path_class.cloud_prefix not in ("http://", "https://"):
        pytest.skip("Test is specific to HTTP/HTTPS")

    path = rig.create_cloud_path("test_http_empty.bin")
    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming
    try:
        with path.open("wb"):
            pass
        path.client.file_cache_mode = original_mode
        assert path.read_bytes() == b""
    finally:
        path.client.file_cache_mode = original_mode
        path.unlink(missing_ok=True)


def test_seek_from_end_without_size(rig, monkeypatch):
    """Test SEEK_END when size cannot be determined."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://", "http://", "https://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_no_size.bin")
    path.write_bytes(b"test data")

    try:
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        # Monkeypatch _get_size to raise an error
        def mock_get_size():
            raise OSError("Cannot determine size")

        with path.open(mode="rb") as f:
            # CloudBufferedIO has a _buffer attribute that wraps the raw IO
            # Access the raw IO object through _buffer
            raw = f._buffer.raw if hasattr(f, "_buffer") else f.raw
            monkeypatch.setattr(raw, "_get_size", mock_get_size)
            monkeypatch.setattr(raw, "_size", None)

            # Try to seek from end - should raise error (either from mock or from handler)
            with pytest.raises(OSError):
                f.seek(-5, io.SEEK_END)

        path.client.file_cache_mode = original_mode
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_read_at_eof_returns_empty(temp_cloud_binary_file):
    """Test that reading at EOF returns empty bytes."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        # Seek to end
        f.seek(0, io.SEEK_END)
        # Try to read
        data = f.read(100)
        assert data == b""


def test_readinto_at_eof_returns_zero(temp_cloud_binary_file):
    """Test that readinto at EOF returns 0."""
    with temp_cloud_binary_file.open(mode="rb") as f:
        # Seek to end
        f.seek(0, io.SEEK_END)
        # Try to readinto
        buf = bytearray(100)
        n = f.readinto(buf)
        assert n == 0


def test_fspath_raises_in_streaming_mode(rig):
    """Test that fspath raises an error in streaming mode."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_fspath.txt")
    path.write_text("test data")

    try:
        # Set client to streaming mode
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        # Try to access fspath - should raise error
        from cloudpathlib.exceptions import CloudPathNotImplementedError

        with pytest.raises(
            CloudPathNotImplementedError, match="fspath is not available in streaming mode"
        ):
            _ = path.fspath

        # Also test __fspath__ directly
        with pytest.raises(
            CloudPathNotImplementedError, match="fspath is not available in streaming mode"
        ):
            _ = path.__fspath__()

        path.client.file_cache_mode = original_mode
    finally:
        try:
            path.unlink()
        except Exception:
            pass


# ============================================================================
# Step 8 regression tests — one test per bug from the plan
# ============================================================================


# H1 — finalize-error propagates (no silent data loss)
def test_finalize_error_propagates(rig):
    """A failed upload must raise out of the with-block; silent data loss is not allowed."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_finalize_error.bin")
    raw_io_class = path._cloud_meta.raw_io_class

    class _FailingRaw(raw_io_class):
        def _finalize_upload(self) -> None:
            raise RuntimeError("simulated upload failure")

    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming

    try:
        with pytest.raises(RuntimeError, match="simulated upload failure"):
            with CloudBufferedIO(
                raw_io_class=_FailingRaw,
                client=path.client,
                cloud_path=path,
                mode="wb",
            ) as f:
                f.write(b"data that should not survive")
    finally:
        path.client.file_cache_mode = original_mode
        try:
            path.unlink()
        except Exception:
            pass


# H2 — exclusive create raises for 'xb' and 'xt' when object already exists
def test_exclusive_create_xb_raises_when_exists(rig):
    """open('xb') must raise CloudPathFileExistsError when the object already exists."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    from cloudpathlib.exceptions import CloudPathFileExistsError

    path = rig.create_cloud_path("test_exclusive_create.bin")
    path.write_bytes(b"existing content")

    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming

    try:
        with pytest.raises(CloudPathFileExistsError):
            path.open("xb")
    finally:
        path.client.file_cache_mode = original_mode
        try:
            path.unlink()
        except Exception:
            pass


def test_exclusive_create_xt_raises_when_exists(rig):
    """open('xt') must also raise CloudPathFileExistsError (mode='x' alone was not enough)."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    from cloudpathlib.exceptions import CloudPathFileExistsError

    path = rig.create_cloud_path("test_exclusive_create_xt.txt")
    path.write_text("existing")

    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming

    try:
        with pytest.raises(CloudPathFileExistsError):
            path.open("xt")
    finally:
        path.client.file_cache_mode = original_mode
        try:
            path.unlink()
        except Exception:
            pass


# H2 — append/r+ fall back to cache (correct semantics over streaming)
def test_append_mode_uses_cache_fallback(rig):
    """Append mode with streaming file_cache_mode must fall back to the cached path."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_append_fallback.bin")
    path.write_bytes(b"hello ")

    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming

    try:
        with path.open("ab") as f:
            assert not isinstance(f, CloudBufferedIO), "append mode must use cache, not streaming"
            f.write(b"world")

        path.client.file_cache_mode = original_mode
        assert path.read_bytes() == b"hello world"
    finally:
        path.client.file_cache_mode = original_mode
        try:
            path.unlink()
        except Exception:
            pass


def test_append_mode_creates_missing_file(local_s3_rig):
    path = local_s3_rig.create_cloud_path("new-append.txt")
    path.client.file_cache_mode = FileCacheMode.streaming

    with path.open("a") as stream:
        stream.write("created")

    path.client.file_cache_mode = FileCacheMode.cloudpath_object
    assert path.read_text() == "created"


def test_rplus_mode_uses_cache_fallback(rig):
    """r+b mode with streaming file_cache_mode must fall back to the cached path."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path = rig.create_cloud_path("test_rplus_fallback.bin")
    path.write_bytes(b"hello world")

    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming

    try:
        with path.open("r+b") as f:
            assert not isinstance(f, CloudBufferedIO), "r+b must use cache, not streaming"
            f.seek(6)
            f.write(b"there")

        path.client.file_cache_mode = original_mode
        assert path.read_bytes() == b"hello there"
    finally:
        path.client.file_cache_mode = original_mode
        try:
            path.unlink()
        except Exception:
            pass


# H3 — S3 part-size floor: no non-final part < 5 MiB
def test_s3_no_small_non_final_parts(rig):
    """All non-final S3 multipart parts must be >= 5 MiB."""
    if rig.path_class.cloud_prefix != "s3://":
        pytest.skip("S3-specific test")

    from cloudpathlib.s3.s3_io import _S3StorageRaw

    path = rig.create_cloud_path("test_part_size.bin")
    data = b"X" * (12 * 1024 * 1024)  # 12 MiB → two 5 MiB parts + one 2 MiB final

    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming

    uploaded_parts = []
    real_upload_part = path.client._upload_part

    def spy_upload_part(cloud_path, upload_id, part_number, part_data):
        uploaded_parts.append(len(part_data))
        return real_upload_part(cloud_path, upload_id, part_number, part_data)

    path.client._upload_part = spy_upload_part

    try:
        with path.open("wb") as f:
            f.write(data)

        path.client.file_cache_mode = original_mode
        assert path.read_bytes() == data

        min_size = _S3StorageRaw._MIN_PART_SIZE
        for part_size in uploaded_parts[:-1]:  # all except last
            assert (
                part_size >= min_size
            ), f"Non-final part is {part_size} bytes, below 5 MiB minimum"
    finally:
        path.client._upload_part = real_upload_part
        path.client.file_cache_mode = original_mode
        try:
            path.unlink()
        except Exception:
            pass


# H3b — S3 aborts multipart upload when complete fails
def test_s3_abort_multipart_on_complete_failure(rig):
    """If _complete_multipart_upload fails, _abort_multipart_upload is attempted."""
    if rig.path_class.cloud_prefix != "s3://":
        pytest.skip("S3-specific test")

    path = rig.create_cloud_path("test_abort_on_complete_fail.bin")
    data = b"X" * (6 * 1024 * 1024)  # 6 MiB → one 5 MiB part + final part

    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming

    real_complete = path.client._complete_multipart_upload
    real_abort = path.client._abort_multipart_upload
    abort_calls = []

    def fail_complete(cloud_path, upload_id, parts):
        raise RuntimeError("complete failed")

    def spy_abort(cloud_path, upload_id):
        abort_calls.append(upload_id)
        return real_abort(cloud_path, upload_id)

    path.client._complete_multipart_upload = fail_complete
    path.client._abort_multipart_upload = spy_abort

    try:
        with pytest.raises(RuntimeError, match="complete failed"):
            with path.open("wb") as f:
                f.write(data)

        assert len(abort_calls) == 1
        assert not path.exists()
    finally:
        path.client._complete_multipart_upload = real_complete
        path.client._abort_multipart_upload = real_abort
        path.client.file_cache_mode = original_mode
        try:
            path.unlink()
        except Exception:
            pass


# H4 — concurrent writes to two paths on one client don't cross buffers
def test_concurrent_writes_dont_cross_buffers(rig):
    """Two simultaneous streaming writers must not share upload state."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    path_a = rig.create_cloud_path("test_concurrent_a.bin")
    path_b = rig.create_cloud_path("test_concurrent_b.bin")
    data_a = b"AAAA" * 1024
    data_b = b"BBBB" * 1024
    errors = []

    def write_path(path, data):
        try:
            original_mode = path.client.file_cache_mode
            path.client.file_cache_mode = FileCacheMode.streaming
            with path.open("wb") as f:
                f.write(data)
            path.client.file_cache_mode = original_mode
        except Exception as e:
            errors.append(e)

    t_a = threading.Thread(target=write_path, args=(path_a, data_a))
    t_b = threading.Thread(target=write_path, args=(path_b, data_b))
    t_a.start()
    t_b.start()
    t_a.join()
    t_b.join()

    assert not errors, f"Concurrent write errors: {errors}"

    try:
        assert path_a.read_bytes() == data_a, "path_a has wrong data (buffer cross)"
        assert path_b.read_bytes() == data_b, "path_b has wrong data (buffer cross)"
    finally:
        for p in (path_a, path_b):
            try:
                p.unlink()
            except Exception:
                pass


# M6/M7 — custom Client without raw_io_class still instantiates in cached mode
def test_custom_client_without_raw_io_class_instantiates(local_s3_rig, monkeypatch):
    """A cached custom provider need not implement the optional streaming hooks."""
    from cloudpathlib.cloudpath import CloudImplementation

    minimal = CloudImplementation()
    minimal.name = "minimal"
    minimal._client_class = local_s3_rig.client_class
    minimal._path_class = local_s3_rig.path_class
    minimal._raw_io_class = None
    monkeypatch.setattr(local_s3_rig.path_class, "_cloud_meta", minimal)

    path = local_s3_rig.create_cloud_path("no-raw-io.txt")
    path.write_text("cached")
    assert path.read_text() == "cached"

    path.client.file_cache_mode = FileCacheMode.streaming
    with pytest.raises(CloudPathNotImplementedError, match="Streaming I/O is not implemented"):
        path.open("r")


# M5 — HTTP range reads return the correct slice
def test_http_range_read_returns_correct_bytes(rig):
    """HTTP range reads must return exactly the requested byte slice."""
    if rig.path_class.cloud_prefix not in ("http://", "https://"):
        pytest.skip("HTTP/HTTPS-specific test")

    path = rig.create_cloud_path("test_range_slice.bin")
    content = b"0123456789abcdef"
    path.write_bytes(content)

    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming

    try:
        with path.open("rb", buffer_size=4) as f:
            chunk = f.read(4)
            assert chunk == b"0123", f"Expected first 4 bytes, got {chunk!r}"
            chunk2 = f.read(4)
            assert chunk2 == b"4567", f"Expected bytes 4-7, got {chunk2!r}"
    finally:
        path.client.file_cache_mode = original_mode
        try:
            path.unlink()
        except Exception:
            pass


@pytest.mark.parametrize("mode", ["", "rw", "rr", "r++", "rbt", "q"])
def test_streaming_open_rejects_invalid_modes_without_mutating(local_s3_rig, mode):
    path = local_s3_rig.create_cloud_path("invalid-mode.txt")
    path.write_text("preserve me")
    path.client.file_cache_mode = FileCacheMode.streaming

    with pytest.raises(ValueError):
        path.open(mode)

    path.client.file_cache_mode = FileCacheMode.cloudpath_object
    assert path.read_text() == "preserve me"


def test_streaming_open_rejects_non_string_mode(local_s3_rig):
    path = local_s3_rig.create_cloud_path("invalid-mode.txt")

    with pytest.raises(TypeError, match="mode must be a string"):
        path.open(None)


@pytest.mark.parametrize(
    "keyword,value,message",
    [
        ("encoding", "utf-8", "encoding"),
        ("errors", "ignore", "errors"),
        ("newline", "", "newline"),
    ],
)
def test_streaming_binary_mode_rejects_text_arguments(local_s3_rig, keyword, value, message):
    path = local_s3_rig.create_cloud_path("binary-arguments.bin")
    path.write_bytes(b"data")
    path.client.file_cache_mode = FileCacheMode.streaming

    with pytest.raises(ValueError, match=message):
        path.open("rb", **{keyword: value})


def test_streaming_text_mode_rejects_unbuffered_io(local_s3_rig):
    path = local_s3_rig.create_cloud_path("unbuffered.txt")
    path.write_text("data")
    path.client.file_cache_mode = FileCacheMode.streaming

    with pytest.raises(ValueError, match="unbuffered text"):
        path.open("r", buffering=0)


def test_streaming_text_mode_honors_line_buffering(local_s3_rig):
    path = local_s3_rig.create_cloud_path("line-buffered.txt")
    path.client.file_cache_mode = FileCacheMode.streaming

    with path.open("w", buffering=1) as stream:
        assert stream.line_buffering
        stream.write("line\n")


def test_streaming_binary_mode_supports_unbuffered_io(local_s3_rig):
    path = local_s3_rig.create_cloud_path("unbuffered.bin")
    path.write_bytes(b"data")
    path.client.file_cache_mode = FileCacheMode.streaming

    with path.open("rb", buffering=0) as stream:
        assert isinstance(stream, io.RawIOBase)
        assert stream.read() == b"data"


def test_s3_streaming_routes_extra_args_by_operation(s3_rig):
    if s3_rig.live_server:
        pytest.skip("Synthetic SDK argument-routing test")

    path = s3_rig.create_cloud_path("streaming-extra-args.bin")
    client = path.client
    original_extra_args = client.boto3_ul_extra_args
    original_create = client.client.create_multipart_upload
    original_upload = client.client.upload_part
    original_complete = client.client.complete_multipart_upload
    calls = {}

    def record_create(**kwargs):
        calls["create"] = kwargs.copy()
        return original_create(**kwargs)

    def record_upload(**kwargs):
        calls["upload"] = kwargs.copy()
        return original_upload(**kwargs)

    def record_complete(**kwargs):
        calls["complete"] = kwargs.copy()
        return original_complete(**kwargs)

    client.boto3_ul_extra_args = {
        "ChecksumCRC32": "whole-object-checksum",
        "SSECustomerAlgorithm": "AES256",
        "SSECustomerKey": "secret",
    }
    client.client.create_multipart_upload = record_create
    client.client.upload_part = record_upload
    client.client.complete_multipart_upload = record_complete
    client.file_cache_mode = FileCacheMode.streaming
    try:
        with path.open("wb") as stream:
            stream.write(b"x" * (6 * 1024 * 1024))

        assert "ChecksumCRC32" not in calls["create"]
        assert calls["create"]["SSECustomerKey"] == "secret"
        assert calls["upload"]["SSECustomerKey"] == "secret"
        assert calls["complete"]["ChecksumCRC32"] == "whole-object-checksum"
    finally:
        client.boto3_ul_extra_args = original_extra_args
        client.client.create_multipart_upload = original_create
        client.client.upload_part = original_upload
        client.client.complete_multipart_upload = original_complete
        path.unlink(missing_ok=True)


def test_s3_streaming_ignores_automatic_part_checksums(s3_rig, monkeypatch):
    path = s3_rig.create_cloud_path("automatic-checksum.bin")
    client = path.client
    original_extra_args = client.boto3_ul_extra_args

    monkeypatch.setattr(
        client.client,
        "upload_part",
        lambda **kwargs: {"ETag": '"etag"', "ChecksumCRC32": "checksum"},
    )
    try:
        client.boto3_ul_extra_args = {}
        part = client._upload_part(path, "upload", 1, b"data")
        assert part == {"PartNumber": 1, "ETag": '"etag"'}

        client.boto3_ul_extra_args = {"ChecksumAlgorithm": "CRC32"}
        part = client._upload_part(path, "upload", 1, b"data")
        assert part["ChecksumCRC32"] == "checksum"
    finally:
        client.boto3_ul_extra_args = original_extra_args


def test_gs_streaming_range_is_inclusive_and_forwards_options(gs_rig, monkeypatch):
    if gs_rig.live_server:
        pytest.skip("Synthetic SDK option-forwarding test")

    from tests.mock_clients.mock_gs import MockBlob

    path = gs_rig.create_cloud_path("range-options.bin")
    path.write_bytes(b"0123456789")
    calls = {}

    def record_download(self, start=None, end=None, **kwargs):
        calls.update(start=start, end=end, **kwargs)
        return b"2345"

    monkeypatch.setattr(MockBlob, "download_as_bytes", record_download)
    retry = object()
    original_kwargs = path.client.blob_kwargs
    path.client.blob_kwargs = {"timeout": 12, "retry": retry}
    try:
        assert path.client._range_download(path, 2, 5) == b"2345"
        assert calls == {"start": 2, "end": 5, "timeout": 12, "retry": retry}
    finally:
        path.client.blob_kwargs = original_kwargs


def test_http_streaming_rejects_servers_that_ignore_ranges(http_rig, monkeypatch):
    class FullResponse(io.BytesIO):
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    def ignore_range(request):
        assert request.headers["Range"] == "bytes=2-5"
        return FullResponse(b"0123456789")

    path = http_rig.create_cloud_path("ignored-range.bin")
    monkeypatch.setattr(path.client.opener, "open", ignore_range)

    with pytest.raises(OSError, match="ignored the Range header"):
        path.client._range_download(path, 2, 5)


def test_http_streaming_upload_uses_client_configuration(http_rig, monkeypatch):
    class CreatedResponse:
        status = 201

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    calls = {}

    def record_request(request):
        calls["request"] = request
        calls["body"] = request.data.read()
        return CreatedResponse()

    path = http_rig.create_cloud_path("configured.txt")
    path.client.write_file_http_method = "PATCH"
    monkeypatch.setattr(path.client.opener, "open", record_request)
    path.client._put_data(path, io.BytesIO(b"abc"), 3)

    request = calls["request"]
    assert request.method == "PATCH"
    assert request.headers["Content-type"] == "text/plain"
    assert request.headers["Content-length"] == "3"
    assert calls["body"] == b"abc"


def test_provider_part_sizes_grow_for_large_streams():
    from cloudpathlib.azure.azure_io import _AzureBlobStorageRaw
    from cloudpathlib.s3.s3_io import _S3StorageRaw

    assert (
        _S3StorageRaw._part_size_for_number(_S3StorageRaw._PARTS_PER_SIZE_TIER + 1)
        == 2 * _S3StorageRaw._MIN_PART_SIZE
    )

    assert (
        _AzureBlobStorageRaw._block_size_for_number(_AzureBlobStorageRaw._BLOCKS_PER_SIZE_TIER + 1)
        == 2 * _AzureBlobStorageRaw._BLOCK_SIZE
    )


def test_s3_invalid_object_state_is_not_eof(s3_rig):
    path = s3_rig.create_cloud_path("archived.bin")
    raw = path._cloud_meta.raw_io_class(path.client, path, "rb")
    assert not raw._is_eof_error(Exception("InvalidObjectState"))


def test_raw_read_errors_follow_file_object_semantics(local_s3_rig):
    class Raw(_CloudStorageRaw):
        def _upload_chunk(self, data):
            pass

        def _finalize_upload(self):
            pass

    path = local_s3_rig.create_cloud_path("raw-errors.bin")

    unreadable = Raw(path.client, path, "wb")
    with pytest.raises(io.UnsupportedOperation, match="not readable"):
        unreadable.readinto(bytearray(1))

    unwritable = Raw(path.client, path, "rb")
    with pytest.raises(io.UnsupportedOperation, match="not writable"):
        unwritable.write(b"x")

    unwritable.close()
    for operation in (
        lambda: unwritable.readinto(bytearray(1)),
        lambda: unwritable.seek(0),
        unwritable.tell,
        lambda: unwritable.write(b"x"),
    ):
        with pytest.raises(ValueError, match="closed file"):
            operation()


def test_raw_read_handles_unknown_size_and_provider_eof(local_s3_rig):
    class Raw(_CloudStorageRaw):
        eof = False
        empty = False

        def _get_size(self):
            raise OSError("size unavailable")

        def _range_get(self, start, end):
            if self.empty:
                return b""
            raise OSError("range unavailable")

        def _is_eof_error(self, error):
            return self.eof

        def _upload_chunk(self, data):
            pass

        def _finalize_upload(self):
            pass

    path = local_s3_rig.create_cloud_path("raw-eof.bin")
    raw = Raw(path.client, path, "rb")

    with pytest.raises(OSError, match="range unavailable"):
        raw.readinto(bytearray(1))

    raw.eof = True
    assert raw.readinto(bytearray(1)) == 0

    raw.eof = False
    raw.empty = True
    assert raw.readinto(bytearray(1)) == 0


def test_raw_write_failure_is_sticky_and_aborts_on_close(local_s3_rig):
    error = OSError("upload failed")

    class Raw(_CloudStorageRaw):
        aborted = False

        def _upload_chunk(self, data):
            raise error

        def _finalize_upload(self):
            pass

        def _abort_upload(self):
            self.aborted = True

    path = local_s3_rig.create_cloud_path("raw-upload-error.bin")
    raw = Raw(path.client, path, "wb")

    with pytest.raises(OSError, match="upload failed"):
        raw.write(b"first")
    with pytest.raises(OSError, match="upload failed"):
        raw.write(b"second")
    with pytest.raises(OSError, match="upload failed"):
        raw.close()

    assert raw.aborted
    assert raw.closed


@pytest.mark.parametrize("fail_during_write", [True, False])
def test_raw_abort_failure_does_not_mask_original_error(local_s3_rig, fail_during_write):
    original_error = OSError("original failure")

    class Raw(_CloudStorageRaw):
        def _upload_chunk(self, data):
            if fail_during_write:
                raise original_error

        def _finalize_upload(self):
            if not fail_during_write:
                raise original_error

        def _abort_upload(self):
            raise OSError("cleanup failure")

    path = local_s3_rig.create_cloud_path("raw-abort-error.bin")
    raw = Raw(path.client, path, "wb")

    if fail_during_write:
        with pytest.raises(OSError, match="original failure"):
            raw.write(b"data")
    else:
        raw.write(b"data")

    with pytest.raises(OSError, match="original failure"):
        raw.close()


# ============================================================================
# Regression tests — PR #535 review fixes
# ============================================================================

_STREAMING_PREFIXES = ("s3://", "az://", "gs://", "http://", "https://")


def _skip_if_no_streaming(rig):
    if rig.path_class.cloud_prefix not in _STREAMING_PREFIXES:
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")


@pytest.fixture
def streaming_rig(rig):
    """The rig with its default client switched to streaming mode for the test."""
    _skip_if_no_streaming(rig)
    client = rig.client_class._default_client
    original_mode = client.file_cache_mode
    client.file_cache_mode = FileCacheMode.streaming
    yield rig
    client.file_cache_mode = original_mode


def test_write_tell_tracks_position(streaming_rig):
    """tell() on streaming write streams must report total bytes written, not just
    the bytes pending in the buffer (write() previously never advanced the raw position)."""
    path = streaming_rig.create_cloud_path("test_write_tell.bin")

    try:
        # small buffer so most bytes reach the raw layer instead of sitting in the buffer
        with path.open("wb", buffer_size=64 * 1024) as f:
            assert f.tell() == 0
            f.write(b"x" * 200_000)  # larger than the buffer
            assert f.tell() == 200_000
            f.write(b"y" * 100)  # small write held in the buffer
            assert f.tell() == 200_100
        assert path.read_bytes() == b"x" * 200_000 + b"y" * 100
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_streaming_zipfile_write_roundtrip(streaming_rig):
    """Position-dependent writers like zipfile rely on tell(); a streaming write
    stream must produce a valid archive."""
    path = streaming_rig.create_cloud_path("test_streaming_archive.zip")
    big_member = b"data" * 50_000  # > the 64 KiB buffer below so bytes reach the raw layer

    try:
        with path.open("wb", buffer_size=64 * 1024) as f:
            with zipfile.ZipFile(f, "w") as zf:
                zf.writestr("a.txt", b"hello world")
                zf.writestr("b.bin", big_member)

        with zipfile.ZipFile(io.BytesIO(path.read_bytes())) as zf:
            assert zf.testzip() is None
            assert zf.read("a.txt") == b"hello world"
            assert zf.read("b.bin") == big_member
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_raw_write_stream_rejects_seek(streaming_rig):
    """seek() on a write-only raw stream must raise io.UnsupportedOperation instead of
    silently succeeding while writes keep appending."""
    path = streaming_rig.create_cloud_path("test_raw_seek_write.bin")

    try:
        with path.open("wb", buffering=0) as f:
            assert not f.seekable()
            f.write(b"data")
            with pytest.raises(io.UnsupportedOperation):
                f.seek(0)
        assert path.read_bytes() == b"data"
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_streaming_full_read_uses_single_range_request(streaming_rig, monkeypatch):
    """read() to EOF must fetch the remaining bytes in one ranged request (readall),
    not fall back to one request per 8 KiB default chunk."""
    rig = streaming_rig
    content = bytes(range(256)) * 2048  # 512 KiB

    # write in cached mode, then stream the read
    client = rig.client_class._default_client
    client.file_cache_mode = FileCacheMode.tmp_dir
    path = rig.create_cloud_path("test_readall.bin")
    path.write_bytes(content)
    client.file_cache_mode = FileCacheMode.streaming

    calls = []
    original_range_download = type(client)._range_download

    def counting_range_download(self, cloud_path, start, end):
        calls.append((start, end))
        return original_range_download(self, cloud_path, start, end)

    monkeypatch.setattr(type(client), "_range_download", counting_range_download)

    try:
        with path.open("rb") as f:
            data = f.read()
        assert data == content
        assert len(calls) <= 2, f"expected a single ranged request, got {calls}"
    finally:
        monkeypatch.undo()
        try:
            path.unlink()
        except Exception:
            pass


def test_streaming_size_fetch_failure_is_memoized(streaming_rig, monkeypatch):
    """A failing content-length lookup must be attempted at most once per stream,
    not re-issued before every chunk read."""
    rig = streaming_rig
    content = b"z" * (200 * 1024)

    client = rig.client_class._default_client
    client.file_cache_mode = FileCacheMode.tmp_dir
    path = rig.create_cloud_path("test_size_memo.bin")
    path.write_bytes(content)
    client.file_cache_mode = FileCacheMode.streaming

    calls = {"n": 0}

    def failing_get_content_length(self, cloud_path):
        calls["n"] += 1
        raise OSError("no size available")

    monkeypatch.setattr(type(client), "_get_content_length", failing_get_content_length)

    try:
        with path.open("rb", buffer_size=16 * 1024) as f:
            data = f.read()
        assert data == content
        assert calls["n"] == 1
    finally:
        monkeypatch.undo()
        try:
            path.unlink()
        except Exception:
            pass


def test_gs_range_download_transient_error_not_treated_as_eof(gs_rig, monkeypatch):
    """Errors that merely contain '416' in their message (request IDs, generation
    numbers) must propagate; only true 416 range errors read as EOF."""
    path = gs_rig.create_cloud_path("test_416_matching.bin")

    class FakeServiceUnavailable(Exception):
        code = 503

    class FakeRangeError(Exception):
        code = 416

    def make_stub(error):
        class StubBlob:
            def download_as_bytes(self, start=None, end=None, **kwargs):
                raise error

        class StubBucket:
            def blob(self, name):
                return StubBlob()

        return lambda name: StubBucket()

    # transient error whose message contains "416" must raise, not return EOF
    monkeypatch.setattr(
        path.client.client,
        "bucket",
        make_stub(FakeServiceUnavailable("503 GET /o/file?generation=1234164 backend error")),
    )
    with pytest.raises(FakeServiceUnavailable):
        path.client._range_download(path, 0, 9)

    # structured 416 still reads as EOF
    monkeypatch.setattr(
        path.client.client, "bucket", make_stub(FakeRangeError("range not satisfiable"))
    )
    assert path.client._range_download(path, 0, 9) == b""

    # exact reason phrase still reads as EOF (some layers do not expose a code)
    monkeypatch.setattr(
        path.client.client, "bucket", make_stub(Exception("Requested Range Not Satisfiable"))
    )
    assert path.client._range_download(path, 0, 9) == b""


def test_azure_block_ids_namespaced_per_upload(azure_rig):
    """Concurrent streaming writers to the same blob must stage blocks under distinct
    IDs so they cannot clobber each other's uncommitted blocks."""
    path = azure_rig.create_cloud_path("test_block_ids.bin")

    upload_a = path.client._initiate_multipart_upload(path)
    upload_b = path.client._initiate_multipart_upload(path)
    assert upload_a and upload_b and upload_a != upload_b

    try:
        part_a = path.client._upload_part(path, upload_a, 1, b"A" * 16)
        part_b = path.client._upload_part(path, upload_b, 1, b"B" * 16)
        assert part_a["block_id"] != part_b["block_id"]

        # committing B yields exactly B's data even though A staged the same part number
        path.client._complete_multipart_upload(path, upload_b, [part_b])
        assert path.read_bytes() == b"B" * 16
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_streaming_cross_client_copy(streaming_rig):
    """copy() between paths on different client instances must work in streaming mode
    (the cached implementation round-trips through fspath, which streaming forbids)."""
    rig = streaming_rig
    content = b"copy me" * 100

    client = rig.client_class._default_client
    client.file_cache_mode = FileCacheMode.tmp_dir
    src = rig.create_cloud_path("test_copy_src.bin")
    src.write_bytes(content)
    client.file_cache_mode = FileCacheMode.streaming

    other_client = rig.client_class(**rig.required_client_kwargs)
    dst = other_client.CloudPath(str(rig.create_cloud_path("test_copy_dst.bin")))
    assert src.client is not dst.client

    try:
        result = src.copy(dst)
        assert result.read_bytes() == content
    finally:
        for p in (src, dst):
            try:
                p.unlink()
            except Exception:
                pass


def test_http_rename_in_streaming_mode(rig):
    """rename()/replace() on HTTP paths must work in streaming mode without fspath."""
    if rig.path_class.cloud_prefix not in ("http://", "https://"):
        pytest.skip("HTTP/HTTPS-specific test")

    path = rig.create_cloud_path("test_rename_src.bin")
    path.write_bytes(b"move me")
    target = rig.create_cloud_path("test_rename_dst.bin")

    original_mode = path.client.file_cache_mode
    path.client.file_cache_mode = FileCacheMode.streaming
    try:
        result = path.rename(target)
        assert result.read_bytes() == b"move me"
        assert not path.exists()
    finally:
        path.client.file_cache_mode = original_mode
        for p in (path, target):
            try:
                p.unlink()
            except Exception:
                pass


def test_streaming_write_conflict_raises(streaming_rig):
    """A streaming write with force_overwrite_to_cloud=False must not clobber a
    version uploaded while the stream was open."""
    rig = streaming_rig
    path = rig.create_cloud_path("test_stream_conflict.bin")
    path.write_bytes(b"original")

    try:
        f = path.open("wb", force_overwrite_to_cloud=False)
        f.write(b"mine")

        # a concurrent writer replaces the object with a strictly newer version
        time.sleep(1.1)  # some providers report modification times in whole seconds
        path.write_bytes(b"concurrent")

        with pytest.raises(OverwriteNewerCloudError):
            f.close()

        assert path.read_bytes() == b"concurrent"
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_streaming_write_conflict_force_overwrites(streaming_rig):
    """force_overwrite_to_cloud=True skips the conflict check and wins."""
    rig = streaming_rig
    path = rig.create_cloud_path("test_stream_conflict_force.bin")
    path.write_bytes(b"original")

    try:
        f = path.open("wb", force_overwrite_to_cloud=True)
        f.write(b"mine")
        path.write_bytes(b"concurrent")
        f.close()

        assert path.read_bytes() == b"mine"
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_streaming_write_overwrite_unchanged_cloud_succeeds(streaming_rig):
    """Overwriting an object that did not change while the stream was open is a
    normal write and must not raise."""
    rig = streaming_rig
    path = rig.create_cloud_path("test_stream_overwrite_ok.bin")
    path.write_bytes(b"v1")

    try:
        with path.open("wb", force_overwrite_to_cloud=False) as f:
            f.write(b"v2")
        assert path.read_bytes() == b"v2"
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_negative_buffering_accepted(rig):
    """Any negative buffering value means 'use the default', matching builtins.open."""
    path = rig.create_cloud_path("test_neg_buffering.txt")

    try:
        # cached mode passes buffering through to the local filesystem open
        with path.open("w", buffering=-2) as f:
            f.write("cached")
        assert path.read_text() == "cached"

        if rig.path_class.cloud_prefix in _STREAMING_PREFIXES:
            original_mode = path.client.file_cache_mode
            path.client.file_cache_mode = FileCacheMode.streaming
            try:
                with path.open("rb", buffering=-2) as f:
                    assert f.read() == b"cached"
            finally:
                path.client.file_cache_mode = original_mode
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_streaming_not_found_errors_are_cloudpathlib_exceptions(rig):
    """Streaming hooks must raise cloudpathlib's exception types so callers catching
    CloudPathException (or CloudPathFileNotFoundError) see streaming errors too."""
    _skip_if_no_streaming(rig)
    missing = rig.create_cloud_path("definitely_missing_for_streaming.bin")

    with pytest.raises(CloudPathFileNotFoundError):
        missing.client._range_download(missing, 0, 9)

    with pytest.raises(CloudPathFileNotFoundError):
        missing.client._get_content_length(missing)


# ============================================================================
# Coverage-gap tests — raw-stream edge semantics, provider EOF mapping, guards
# ============================================================================


def test_raw_stream_edge_semantics(local_s3_rig):
    """Raw adapter edge cases follow file-object semantics."""
    path = local_s3_rig.create_cloud_path("raw-edges.bin")
    path.write_bytes(b"0123456789")
    raw = path._cloud_meta.raw_io_class(path.client, path, "rb")

    # empty destination buffer reads zero bytes
    assert raw.readinto(bytearray(0)) == 0

    # relative seek and whence validation at the raw layer
    raw.seek(4)
    assert raw.seek(2, io.SEEK_CUR) == 6
    with pytest.raises(ValueError, match="invalid whence"):
        raw.seek(0, 42)

    # readall from a mid-stream position, then again at EOF
    assert raw.readall() == b"6789"
    assert raw.readall() == b""
    raw.close()

    # write-only streams cannot readall; closed streams cannot readall
    writer = path._cloud_meta.raw_io_class(path.client, path, "wb")
    with pytest.raises(io.UnsupportedOperation):
        writer.readall()
    writer.write(b"replaced")
    writer.close()
    writer.close()  # double close is a no-op
    with pytest.raises(ValueError, match="closed file"):
        writer.readall()
    assert path.read_bytes() == b"replaced"


def test_multipart_part_limit_enforced(local_s3_rig):
    """Exceeding the provider's maximum part count raises a clear OSError."""
    path = local_s3_rig.create_cloud_path("part-limit.bin")

    class TinyParts(path._cloud_meta.raw_io_class):
        _INITIAL_PART_SIZE = 4
        _MAX_PART_SIZE = 4
        _MAX_PARTS = 2
        _PARTS_PER_SIZE_TIER = 1_000

    raw = TinyParts(path.client, path, "wb")
    raw.write(b"x" * 8)  # exactly two full parts — at the limit
    with pytest.raises(OSError, match="part limit"):
        raw.write(b"x" * 4)
    with pytest.raises(OSError, match="part limit"):
        raw.close()  # the write failure is sticky and aborts the upload


def test_buffered_io_direct_construction_guards(local_s3_rig):
    """Direct construction validates modes that the open() path never forwards."""
    path = local_s3_rig.create_cloud_path("direct-construction.bin")
    path.write_bytes(b"0123456789")
    raw_cls = path._cloud_meta.raw_io_class

    with pytest.raises(io.UnsupportedOperation, match="append and update"):
        CloudBufferedIO(raw_cls, path.client, path, mode="ab")
    with pytest.raises(io.UnsupportedOperation, match="append and update"):
        CloudTextIO(raw_cls, path.client, path, mode="a")

    # readinto1 delegates to the buffered reader
    with CloudBufferedIO(raw_cls, path.client, path, mode="rb") as f:
        buf = bytearray(4)
        assert f.readinto1(buf) == 4
        assert bytes(buf) == b"0123"


def test_streaming_text_exclusive_create(streaming_rig):
    """mode='x' in text form creates a new object via streaming and rejects existing ones."""
    from cloudpathlib.exceptions import CloudPathFileExistsError

    path = streaming_rig.create_cloud_path("test_x_create.txt")
    try:
        with path.open(mode="x") as f:
            f.write("created")
        assert path.read_text() == "created"
        with pytest.raises(CloudPathFileExistsError):
            path.open(mode="x")
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_range_download_past_eof_returns_empty(streaming_rig):
    """A range starting past EOF maps to EOF (empty bytes) on every provider."""
    path = streaming_rig.create_cloud_path("past_eof.bin")
    path.write_bytes(b"0123456789")
    try:
        assert path.client._range_download(path, 100, 199) == b""
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_raw_empty_write_is_noop(streaming_rig):
    """Writing b'' at the raw layer uploads nothing but still finalizes correctly."""
    path = streaming_rig.create_cloud_path("empty_chunk.bin")
    try:
        with path.open("wb", buffering=0) as f:
            assert f.write(b"") == 0
            f.write(b"payload")
            assert f.write(b"") == 0
        assert path.read_bytes() == b"payload"
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_s3_streaming_extra_args_uses_service_model(s3_rig):
    """When the boto3 client exposes a service model, allowed params come from it,
    and unknown params are dropped."""
    from types import SimpleNamespace

    client = s3_rig.client_class(**s3_rig.required_client_kwargs)
    client.boto3_ul_extra_args = {"StorageClass": "STANDARD_IA", "NotARealParam": "x"}

    shape = SimpleNamespace(members={"StorageClass": None})
    operation_model = SimpleNamespace(input_shape=shape)
    service_model = SimpleNamespace(operation_model=lambda name: operation_model)
    client.client = SimpleNamespace(meta=SimpleNamespace(service_model=service_model))

    assert client._streaming_extra_args("CreateMultipartUpload") == {"StorageClass": "STANDARD_IA"}


def test_s3_streaming_extra_args_fallback_matches_botocore(s3_rig):
    """The hard-coded fallback table must filter identically to botocore's real
    service model, so it cannot silently drop newly added parameters."""
    from types import SimpleNamespace

    botocore_session = pytest.importorskip("botocore.session")
    service_model = botocore_session.get_session().get_service_model("s3")

    for operation in (
        "CreateMultipartUpload",
        "UploadPart",
        "CompleteMultipartUpload",
        "PutObject",
    ):
        members = set(service_model.operation_model(operation).input_shape.members)
        extra_args = {name: "value" for name in sorted(members)}

        fallback_client = s3_rig.client_class(**s3_rig.required_client_kwargs)
        fallback_client.boto3_ul_extra_args = extra_args
        fallback_client.client = SimpleNamespace()  # no .meta -> fallback table

        real_client = s3_rig.client_class(**s3_rig.required_client_kwargs)
        real_client.boto3_ul_extra_args = extra_args
        real_client.client = SimpleNamespace(meta=SimpleNamespace(service_model=service_model))

        assert fallback_client._streaming_extra_args(
            operation
        ) == real_client._streaming_extra_args(
            operation
        ), f"fallback table diverges from botocore for {operation}"


def test_s3_streaming_content_encoding_threaded(s3_rig):
    """Encodings from content_type_method are added to streaming upload args."""
    import mimetypes

    client = s3_rig.client_class(
        content_type_method=mimetypes.guess_type, **s3_rig.required_client_kwargs
    )
    path = s3_rig.create_cloud_path("encoded.txt.gz", client=client)

    args = client._streaming_object_args("CreateMultipartUpload", path)
    assert args.get("ContentType") == "text/plain"
    assert args.get("ContentEncoding") == "gzip"


def test_azure_streaming_content_settings_branches(azure_rig):
    """Content settings resolve for absent, empty, and populated content type methods."""
    client_none = azure_rig.client_class(
        content_type_method=None, **azure_rig.required_client_kwargs
    )
    path = azure_rig.create_cloud_path("content-settings.bin", client=client_none)
    assert client_none._streaming_content_settings(path) is None

    client_empty = azure_rig.client_class(
        content_type_method=lambda name: (None, None), **azure_rig.required_client_kwargs
    )
    assert client_empty._streaming_content_settings(path) is None

    client_full = azure_rig.client_class(
        content_type_method=lambda name: ("text/plain", "gzip"),
        **azure_rig.required_client_kwargs,
    )
    settings = client_full._streaming_content_settings(path)
    assert settings.content_type == "text/plain"
    assert settings.content_encoding == "gzip"

    # abort is a documented no-op: uncommitted blocks simply expire server-side
    client_none._abort_multipart_upload(path, "upload-id")


def test_gs_multipart_upload_hooks(gs_rig):
    """GS streaming writes use the XML multipart API: unique upload IDs, ordered
    assembly of size-compliant parts, and cancellable uploads."""
    client = gs_rig.client_class(**gs_rig.required_client_kwargs)
    path = gs_rig.create_cloud_path("mpu-hooks.bin", client=client)

    upload_a = client._initiate_multipart_upload(path)
    upload_b = client._initiate_multipart_upload(path)
    assert upload_a and upload_b and upload_a != upload_b

    head = b"A" * (5 * 1024 * 1024)  # non-final parts must be at least 5 MiB
    tail = b"B" * 16
    part_1 = client._upload_part(path, upload_a, 1, head)
    part_2 = client._upload_part(path, upload_a, 2, tail)
    client._complete_multipart_upload(path, upload_a, [part_1, part_2])
    client._abort_multipart_upload(path, upload_b)

    try:
        assert path.read_bytes() == head + tail
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_gs_mpu_initiate_threads_content_type_and_encoding(gs_rig):
    """Initiate carries the content type and encoding from content_type_method."""
    from types import SimpleNamespace

    client = gs_rig.client_class(
        content_type_method=lambda name: ("text/plain", "gzip"),
        **gs_rig.required_client_kwargs,
    )
    path = gs_rig.create_cloud_path("mpu-headers.txt.gz", client=client)

    captured = {}

    class StubTransport:
        def request(self, method, url, data=None, headers=None, **kwargs):
            import requests

            captured["method"] = method
            captured["url"] = url
            captured["headers"] = {key.lower(): value for key, value in (headers or {}).items()}
            response = requests.Response()
            response.status_code = 200
            response._content = (
                b'<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b"<UploadId>stub-upload</UploadId></InitiateMultipartUploadResult>"
            )
            return response

    client.client = SimpleNamespace(
        _connection=SimpleNamespace(API_BASE_URL="https://storage.googleapis.com"),
        _http=StubTransport(),
    )

    upload_id = client._initiate_multipart_upload(path)
    assert upload_id == "stub-upload"
    assert captured["method"] == "POST"
    assert captured["url"].endswith("?uploads")
    assert captured["headers"]["content-type"] == "text/plain"
    assert captured["headers"]["content-encoding"] == "gzip"


def test_http_streaming_error_paths(http_rig, monkeypatch):
    """HTTP: unexpected range status, missing Content-Length, failing PUT status,
    and unsupported PUT method all map to clear errors."""
    import urllib.error

    path = http_rig.create_cloud_path("http_errors.bin")
    path.write_bytes(b"0123456789")

    class FakeResponse:
        def __init__(self, status, headers=None):
            self.status = status
            self.headers = headers if headers is not None else {}
            self.reason = "stub"

        def read(self, *args):
            return b""

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    # a non-206/200 success status for a range request is unexpected
    monkeypatch.setattr(path.client.opener, "open", lambda req: FakeResponse(204))
    with pytest.raises(OSError, match="Unexpected status"):
        path.client._range_download(path, 0, 3)

    # HEAD without Content-Length cannot size the stream
    monkeypatch.setattr(path.client.opener, "open", lambda req: FakeResponse(200))
    with pytest.raises(ValueError, match="Content-Length"):
        path.client._get_content_length(path)

    # a failing PUT status raises OSError
    monkeypatch.setattr(path.client.opener, "open", lambda req: FakeResponse(500))
    with pytest.raises(OSError, match="HTTP PUT failed"):
        path.client._put_data(path, io.BytesIO(b"x"), 1)

    # non-404 HTTP errors propagate from reads and size checks
    def raise_403(req):
        raise urllib.error.HTTPError("url", 403, "Forbidden", {}, None)

    monkeypatch.setattr(path.client.opener, "open", raise_403)
    with pytest.raises(urllib.error.HTTPError):
        path.client._range_download(path, 0, 3)
    with pytest.raises(urllib.error.HTTPError):
        path.client._get_content_length(path)

    # servers that reject the write method surface CloudPathNotImplementedError
    def raise_405(req):
        raise urllib.error.HTTPError("url", 405, "Method Not Allowed", {}, None)

    monkeypatch.setattr(path.client.opener, "open", raise_405)
    with pytest.raises(CloudPathNotImplementedError):
        path.client._put_data(path, io.BytesIO(b"x"), 1)

    monkeypatch.undo()
    try:
        path.unlink()
    except Exception:
        pass


def test_open_buffer_size_must_be_positive(streaming_rig):
    """buffer_size=0 is rejected up front."""
    path = streaming_rig.create_cloud_path("bad_buffer.bin")
    with pytest.raises(ValueError, match="buffer_size"):
        path.open("wb", buffer_size=0)


def test_open_directory_raises(local_s3_rig):
    """Opening a directory raises CloudPathIsADirectoryError in any cache mode."""
    from cloudpathlib.exceptions import CloudPathIsADirectoryError

    file_path = local_s3_rig.create_cloud_path("adir/inner.txt")
    file_path.write_text("x")
    dir_path = local_s3_rig.create_cloud_path("adir")

    with pytest.raises(CloudPathIsADirectoryError):
        dir_path.open("rb")


def test_streaming_parquet_metadata_and_column_read(streaming_rig):
    """Parquet readers work over a seekable streaming stream: the footer and a
    single column can be read without downloading the whole object."""
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    rig = streaming_rig
    client = rig.client_class._default_client
    client.file_cache_mode = FileCacheMode.tmp_dir
    path = rig.create_cloud_path("test_columns.parquet")
    table = pa.table({"a": list(range(10_000)), "b": ["x" * 20] * 10_000})
    sink = io.BytesIO()
    pq.write_table(table, sink)
    path.write_bytes(sink.getvalue())
    client.file_cache_mode = FileCacheMode.streaming

    try:
        with path.open("rb", buffer_size=64 * 1024) as f:
            parquet_file = pq.ParquetFile(f)
            assert parquet_file.metadata.num_rows == 10_000
            column = parquet_file.read(columns=["a"])
            assert column.column("a").to_pylist()[:3] == [0, 1, 2]
    finally:
        try:
            path.unlink()
        except Exception:
            pass


# ============================================================================
# streaming_max_concurrency — concurrent part uploads and read prefetch
# ============================================================================


def test_streaming_max_concurrency_validation(local_s3_rig):
    """The concurrency knob must be a positive integer."""
    with pytest.raises(ValueError, match="streaming_max_concurrency"):
        local_s3_rig.client_class(
            streaming_max_concurrency=0, **local_s3_rig.required_client_kwargs
        )


def test_concurrent_multipart_write_correctness(streaming_rig):
    """A multi-part streaming write with concurrency > 1 produces identical content,
    even when an early part finishes after later ones."""
    from cloudpathlib.cloud_io import _CloudMultipartStorageRaw

    rig = streaming_rig
    if not issubclass(rig.raw_io_class, _CloudMultipartStorageRaw):
        pytest.skip("provider does not use multipart streaming writes")

    client = rig.client_class(
        file_cache_mode=FileCacheMode.streaming,
        streaming_max_concurrency=4,
        **rig.required_client_kwargs,
    )
    path = rig.create_cloud_path("test_concurrent_parts.bin", client=client)
    data = bytes(range(256)) * (48 * 1024)  # 12 MiB -> two 5 MiB parts + final part

    part_numbers = []
    real_upload_part = client._upload_part

    def delaying_upload_part(cloud_path, upload_id, part_number, part_data):
        if part_number == 1:
            time.sleep(0.2)  # force part 1 to finish after later parts
        part_numbers.append(part_number)
        return real_upload_part(cloud_path, upload_id, part_number, part_data)

    client._upload_part = delaying_upload_part

    try:
        with path.open("wb") as f:
            f.write(data)
        assert sorted(part_numbers) == list(range(1, len(part_numbers) + 1))
        assert len(part_numbers) >= 2
        assert path.read_bytes() == data
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_concurrent_multipart_write_parallelism_observed(local_s3_rig):
    """With concurrency 2, two part uploads genuinely run at the same time."""
    path = local_s3_rig.create_cloud_path("test_parallel_parts.bin")
    client = local_s3_rig.client_class(
        file_cache_mode=FileCacheMode.streaming,
        streaming_max_concurrency=2,
        **local_s3_rig.required_client_kwargs,
    )
    path = local_s3_rig.create_cloud_path("test_parallel_parts.bin", client=client)

    barrier = threading.Barrier(2, timeout=30)
    overlapped = []
    real_upload_part = client._upload_part

    def rendezvous_upload_part(cloud_path, upload_id, part_number, part_data):
        if part_number <= 2:
            barrier.wait()  # only passes if both uploads are in flight simultaneously
            overlapped.append(part_number)
        return real_upload_part(cloud_path, upload_id, part_number, part_data)

    client._upload_part = rendezvous_upload_part

    data = b"Z" * (12 * 1024 * 1024)  # two 5 MiB parts + final part
    try:
        with path.open("wb") as f:
            f.write(data)
        assert sorted(overlapped) == [1, 2]
        assert path.read_bytes() == data
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_concurrent_multipart_write_failure_is_sticky_and_aborts(local_s3_rig):
    """A failing background part upload surfaces on a later write/close and aborts."""
    client = local_s3_rig.client_class(
        file_cache_mode=FileCacheMode.streaming,
        streaming_max_concurrency=2,
        **local_s3_rig.required_client_kwargs,
    )
    path = local_s3_rig.create_cloud_path("test_failing_part.bin", client=client)

    aborted = []
    real_abort = client._abort_multipart_upload

    def spy_abort(cloud_path, upload_id):
        aborted.append(upload_id)
        return real_abort(cloud_path, upload_id)

    def failing_upload_part(cloud_path, upload_id, part_number, part_data):
        raise OSError("part upload failed")

    client._upload_part = failing_upload_part
    client._abort_multipart_upload = spy_abort

    f = path.open("wb")
    with pytest.raises(OSError, match="part upload failed"):
        # keep writing until the background failure is harvested
        for _ in range(10):
            f.write(b"Q" * (6 * 1024 * 1024))
        f.close()
    with pytest.raises(OSError, match="part upload failed"):
        f.close()

    assert aborted, "failed upload was not aborted"
    assert not path.exists()


def test_read_prefetch_correctness_and_no_wasted_requests(streaming_rig):
    """Sequential reads with prefetch fetch each byte range exactly once and
    return identical data; seeking invalidates the prefetch window correctly."""
    rig = streaming_rig
    client = rig.client_class(
        file_cache_mode=FileCacheMode.streaming,
        streaming_max_concurrency=3,
        **rig.required_client_kwargs,
    )
    path = rig.create_cloud_path("test_prefetch.bin", client=client)
    chunk = 128 * 1024
    data = bytes(range(256)) * (4 * 1024)  # 1 MiB -> 8 chunks

    path.write_bytes(data)

    calls = []
    real_range_download = client._range_download

    def counting_range_download(cloud_path, start, end):
        calls.append((start, end))
        return real_range_download(cloud_path, start, end)

    client._range_download = counting_range_download

    try:
        with path.open("rb", buffer_size=chunk) as f:
            read_back = b""
            while True:
                piece = f.read1(chunk)
                if not piece:
                    break
                read_back = read_back + piece
        assert read_back == data
        starts = sorted(start for start, _ in calls)
        assert starts == list(range(0, len(data), chunk)), f"unexpected requests: {calls}"

        # seeking back re-reads correctly even though prefetched chunks are discarded
        with path.open("rb", buffer_size=chunk) as f:
            f.read1(chunk)
            f.seek(3 * chunk)
            assert f.read1(chunk) == data[3 * chunk : 4 * chunk]
            f.seek(0)
            assert f.read1(chunk) == data[:chunk]
    finally:
        try:
            path.unlink()
        except Exception:
            pass
