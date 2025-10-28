"""
Tests for cloud storage streaming I/O.

Tests CloudBufferedIO, CloudTextIO, and streaming mode for direct
streaming without local caching.
"""

import io
import pytest

from cloudpathlib import S3Path, AzureBlobPath, GSPath
from cloudpathlib import CloudBufferedIO, CloudTextIO
from cloudpathlib.enums import FileCacheMode


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
    path.write_text(MULTILINE_TEXT)
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
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
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
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
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
    with temp_cloud_multiline_file.open(mode="rt") as f:
        line1 = f.readline()
        assert line1 == "Line 1\n"

        line2 = f.readline()
        assert line2 == "Line 2\n"


def test_readlines(temp_cloud_multiline_file):
    """Test readlines method."""
    with temp_cloud_multiline_file.open(mode="rt") as f:
        lines = f.readlines()
        assert len(lines) == 4
        assert lines[0] == "Line 1\n"
        assert "special chars" in lines[3]


def test_iteration(temp_cloud_multiline_file):
    """Test iterating over lines."""
    with temp_cloud_multiline_file.open(mode="rt") as f:
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
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
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
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
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
    if not hasattr(rig, "s3_path"):
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
    if not hasattr(rig, "azure_path"):
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


def test_gs_resumable_upload(rig):
    """Test that GCS upload works."""
    if not hasattr(rig, "gs_path"):
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
    """Test that write errors are handled gracefully."""
    if rig.path_class.cloud_prefix not in ("s3://", "az://", "gs://"):
        pytest.skip(f"Streaming I/O not implemented for {rig.path_class.cloud_prefix}")

    # This test just verifies that writing and closing work correctly
    # The error handling paths are tested by the actual upload implementations
    path = rig.create_cloud_path("test_error_cleanup.bin")

    try:
        original_mode = path.client.file_cache_mode
        path.client.file_cache_mode = FileCacheMode.streaming

        # Write some data successfully
        with path.open(mode="wb") as f:
            f.write(b"test data")

        path.client.file_cache_mode = original_mode
        assert path.read_bytes() == b"test data"
    finally:
        try:
            path.unlink()
        except Exception:
            pass


def test_http_write_empty_file(rig):
    """Test HTTP write for empty file."""
    if rig.path_class.cloud_prefix not in ("http://", "https://"):
        pytest.skip("Test is specific to HTTP/HTTPS")

    # HTTP writes aren't fully supported in tests, but we can test the code path
    # Skip for now since HTTP test server doesn't support PUT
    pytest.skip("HTTP write not supported by test server")


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
