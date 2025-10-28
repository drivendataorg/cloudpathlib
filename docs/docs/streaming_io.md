# Streaming I/O

CloudPathLib provides high-performance streaming I/O capabilities for cloud storage that work seamlessly with Python's standard I/O interfaces and third-party libraries.

## Overview

By default, CloudPathLib downloads files to a local cache before opening them. While this works well for many use cases, it can be inefficient for:

- **Large files** that don't fit in memory or disk
- **Partial reads** where you only need to access part of a file
- **Sequential processing** where you read a file once and discard it
- **Write-only workflows** where you're generating data to upload

The streaming I/O system solves these problems by:

- Reading data directly from cloud storage using range requests
- Writing data directly to cloud storage using multipart/block uploads
- Providing standard Python file-like objects that work with any library
- Eliminating the need for local disk caching

## Quick Start

### Enable Streaming Mode

To use streaming I/O, set your client's `file_cache_mode` to `FileCacheMode.streaming`:

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

# Option 1: Set streaming mode on the client
client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/file.txt", client=client)

with path.open("rt") as f:
    for line in f:
        print(line.strip())

# Option 2: Change mode on existing client
client = S3Client()
client.file_cache_mode = FileCacheMode.streaming

path = S3Path("s3://bucket/file.txt", client=client)
with path.open("rt") as f:
    content = f.read()

# Option 3: Temporarily enable streaming
client = S3Client()
path = S3Path("s3://bucket/file.txt", client=client)

original_mode = path.client.file_cache_mode
path.client.file_cache_mode = FileCacheMode.streaming

with path.open("rt") as f:
    content = f.read()

path.client.file_cache_mode = original_mode  # Restore
```

### Basic Examples

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

# Create a client with streaming enabled
client = S3Client(file_cache_mode=FileCacheMode.streaming)

# Read a text file
path = S3Path("s3://bucket/file.txt", client=client)
with path.open("rt") as f:
    for line in f:
        print(line.strip())

# Write a binary file
path = S3Path("s3://bucket/output.bin", client=client)
with path.open("wb") as f:
    f.write(b"Hello, cloud!")

# Read binary data in chunks
path = S3Path("s3://bucket/large-file.bin", client=client)
with path.open("rb") as f:
    while chunk := f.read(8192):
        process(chunk)
```

## API Reference

!!! important "Always use `CloudPath.open()`"
    The recommended way to use streaming I/O is through `CloudPath.open()` with `FileCacheMode.streaming`. The `CloudBufferedIO` and `CloudTextIO` classes are implementation details returned by `open()` and should not be instantiated directly.

### `FileCacheMode` Enum

Controls how `CloudPath.open()` handles file caching:

- `FileCacheMode.cloudpath_object`: Default - cache files in CloudPath object
- `FileCacheMode.tmp_dir`: Cache files in temporary directory
- `FileCacheMode.persistent`: Cache files persistently
- `FileCacheMode.close_file`: Close file after reading
- **`FileCacheMode.streaming`**: Stream directly without caching

```python
from cloudpathlib.enums import FileCacheMode

# Set on client initialization
client = S3Client(file_cache_mode=FileCacheMode.streaming)

# Or change dynamically
client.file_cache_mode = FileCacheMode.streaming
```

### `CloudPath.open()`

Opens a cloud file in streaming mode when `file_cache_mode` is set to `FileCacheMode.streaming`.

```python
CloudPath.open(
    mode: str = "r",
    buffering: int = -1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    *,
    buffer_size: Optional[int] = None,
) -> Union[CloudBufferedIO, CloudTextIO, IO]
```

**Parameters:**

- `mode`: File mode - binary (`'rb'`, `'wb'`, etc.) or text (`'r'`, `'w'`, `'rt'`, `'wt'`, etc.)
- `buffering`: Buffer size (deprecated, use `buffer_size` instead)
- `encoding`: Text encoding (default: `"utf-8"`, text mode only)
- `errors`: Error handling strategy (default: `"strict"`, text mode only)
- `newline`: Newline handling (text mode only)
- `buffer_size`: Size of read/write buffer in bytes (default: 64 KiB)

**Returns:**

- `CloudBufferedIO` for binary modes (when streaming)
- `CloudTextIO` for text modes (when streaming)
- Standard file object (when not streaming)

### `CloudBufferedIO`

Binary file-like object implementing `io.BufferedIOBase`.

!!! note "Use `CloudPath.open()` instead"
    **Do not instantiate `CloudBufferedIO` directly.** Always use `CloudPath.open()` with the appropriate mode and `FileCacheMode.streaming` to get streaming file objects. The streaming I/O classes are implementation details that are returned by `open()`.

**Key Methods:**

- `read(size=-1)`: Read up to size bytes (all if size is -1)
- `read1(size=-1)`: Read up to size bytes with one underlying read call
- `readinto(b)`: Read bytes into a pre-allocated buffer
- `write(b)`: Write bytes
- `flush()`: Flush write buffer to cloud storage
- `seek(offset, whence=SEEK_SET)`: Change stream position
- `tell()`: Return current stream position
- `close()`: Close file and finalize upload

**Properties:**

- `name`: The cloud path
- `mode`: File mode (e.g., `"rb"`, `"wb"`)
- `closed`: Whether the file is closed

**Capability Flags:**

- `readable()`: Returns True for read modes
- `writable()`: Returns True for write modes
- `seekable()`: Returns True (random access supported)

### `CloudTextIO`

Text file-like object implementing `io.TextIOBase`.

!!! note "Use `CloudPath.open()` instead"
    **Do not instantiate `CloudTextIO` directly.** Always use `CloudPath.open()` with text mode (e.g., `"r"`, `"rt"`, `"w"`, `"wt"`) and `FileCacheMode.streaming` to get streaming text file objects. The streaming I/O classes are implementation details that are returned by `open()`.

**Key Methods:**

- `read(size=-1)`: Read up to size characters
- `readline(size=-1)`: Read one line
- `readlines(hint=-1)`: Read list of lines
- `write(s)`: Write string
- `writelines(lines)`: Write list of strings
- `flush()`: Flush write buffer
- `seek(offset, whence=SEEK_SET)`: Change position
- `tell()`: Return current position
- `close()`: Close file

**Properties:**

- `name`: The cloud path
- `mode`: File mode (e.g., `"rt"`, `"wt"`)
- `encoding`: Text encoding
- `errors`: Error handling strategy
- `newlines`: Newline(s) encountered
- `buffer`: Underlying binary buffer (CloudBufferedIO)
- `closed`: Whether the file is closed

**Iteration:**

CloudTextIO supports iteration:

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/file.txt", client=client)

with path.open("rt") as f:
    for line in f:
        process(line)
```

## Usage Examples

### Reading Large Files in Chunks

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/huge-file.csv", client=client)

# Process a large file without loading it entirely into memory
with path.open("rt") as f:
    header = f.readline()
    for line in f:
        process_csv_line(line)
```

### Partial File Reads

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/data.bin", client=client)

# Read just the header of a file
with path.open("rb") as f:
    header = f.read(1024)  # Read first 1KB
    parse_header(header)
    
    # Seek to specific position
    f.seek(10000)
    chunk = f.read(100)
```

### Streaming Uploads

```python
from cloudpathlib import AzureBlobPath, AzureBlobClient
from cloudpathlib.enums import FileCacheMode
import json

client = AzureBlobClient(file_cache_mode=FileCacheMode.streaming)
path = AzureBlobPath("az://container/output.json", client=client)

# Write data directly to cloud without local file
with path.open("wt") as f:
    f.write('{"items": [\n')
    for i, item in enumerate(generate_items()):
        if i > 0:
            f.write(',\n')
        f.write(json.dumps(item))
    f.write('\n]}')
```

### Using with pandas

```python
import pandas as pd
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)

# Read CSV directly from cloud
read_path = S3Path("s3://bucket/data.csv", client=client)
with read_path.open("rt") as f:
    df = pd.read_csv(f)

# Write CSV directly to cloud
write_path = S3Path("s3://bucket/output.csv", client=client)
with write_path.open("wt") as f:
    df.to_csv(f, index=False)
```

### Using with PIL/Pillow

```python
from PIL import Image
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)

# Read image
read_path = S3Path("s3://bucket/image.jpg", client=client)
with read_path.open("rb") as f:
    img = Image.open(f)
    img.show()

# Write image
write_path = S3Path("s3://bucket/output.png", client=client)
with write_path.open("wb") as f:
    img.save(f, format="PNG")
```

### Custom Buffer Size

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)

# Use larger buffer for better throughput on fast connections
path = S3Path("s3://bucket/large-file.bin", client=client)
with path.open("rb", buffer_size=1024*1024) as f:
    data = f.read()

# Use smaller buffer for memory-constrained environments
path = S3Path("s3://bucket/file.txt", client=client)
with path.open("rt", buffer_size=8192) as f:
    for line in f:
        process(line)
```

## Performance Considerations

### Buffer Size

The `buffer_size` parameter controls how much data is fetched from/written to cloud storage in each request:

- **Larger buffers** (256 KiB - 1 MiB): Better throughput, fewer requests, more memory
- **Smaller buffers** (8 KiB - 64 KiB): Lower memory usage, more requests, lower throughput
- **Default** (64 KiB): Good balance for most use cases

### Read Patterns

- **Sequential reads**: Optimal performance - data is fetched ahead as needed
- **Random seeks**: Each seek may trigger a new range request - less efficient
- **Small random reads**: Consider downloading the file to cache instead

### Write Patterns

- **Sequential writes**: Optimal - data is buffered and uploaded in chunks
- **Large writes**: Automatically split into multipart/block uploads
- **Many small writes**: Buffered and batched for efficiency

### Multipart/Block Uploads

For write operations, the streaming I/O system automatically handles:

- **S3**: Multipart uploads with parts uploaded as buffer fills
- **Azure**: Block blob uploads with blocks committed on close
- **GCS**: Resumable uploads with data uploaded on close

## Provider-Specific Behavior

### AWS S3

- Uses boto3 `get_object()` with `Range` header for reads
- Uses boto3 multipart upload API for writes
- Supports all S3-compatible storage (MinIO, Ceph, etc.)

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/file.txt", client=client)

with path.open("rt") as f:
    content = f.read()
```

### Azure Blob Storage

- Uses Azure SDK `download_blob()` with offset/length for reads
- Uses block blob staging and commit for writes
- Compatible with Azure Data Lake Storage Gen2

```python
from cloudpathlib import AzureBlobPath, AzureBlobClient
from cloudpathlib.enums import FileCacheMode

client = AzureBlobClient(file_cache_mode=FileCacheMode.streaming)
path = AzureBlobPath("az://container/file.txt", client=client)

with path.open("rt") as f:
    content = f.read()
```

### Google Cloud Storage

- Uses GCS SDK `download_as_bytes()` with start/end for reads
- Uses `upload_from_string()` for writes
- Supports GCS-specific features through client configuration

```python
from cloudpathlib import GSPath, GSClient
from cloudpathlib.enums import FileCacheMode

client = GSClient(file_cache_mode=FileCacheMode.streaming)
path = GSPath("gs://bucket/file.txt", client=client)

with path.open("rt") as f:
    content = f.read()
```

## Comparison with Cached Mode

| Feature | Streaming (`FileCacheMode.streaming`) | Cached (default) |
|---------|--------------------------------------|------------------|
| **Disk usage** | Minimal (only buffer) | Full file size |
| **Memory usage** | Configurable buffer | Varies |
| **Read performance** | Sequential: Good<br>Random: Moderate | Fast (local disk) |
| **Write performance** | Good (direct upload) | Fast write, slower close |
| **Partial reads** | Efficient | Downloads full file |
| **Large files** | Excellent | Limited by disk space |
| **Offline access** | No | Yes (after download) |
| **Compatibility** | Standard I/O interfaces | Standard I/O interfaces |

## Best Practices

### When to Use Streaming I/O

✅ **Good use cases:**

- Large files that don't fit in memory/disk
- Reading only part of a file (e.g., headers, metadata)
- Sequential processing (one-pass reads)
- Direct upload of generated content
- Integration with libraries that accept file-like objects

❌ **Consider caching instead:**

- Small files (< 10 MB)
- Frequent random access to same file
- Multiple passes over the same data
- Offline processing
- Maximum read performance required
- Libraries that require file paths (`.fspath` not available in streaming mode)

### Error Handling

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/file.txt", client=client)

try:
    with path.open("rt") as f:
        content = f.read()
except FileNotFoundError:
    print("File not found in cloud storage")
except PermissionError:
    print("Access denied")
except Exception as e:
    print(f"Error: {e}")
```

### Resource Management

Always use context managers to ensure proper cleanup:

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/file.txt", client=client)

# ✅ Good - file is automatically closed
with path.open("rt") as f:
    content = f.read()

# ❌ Bad - must remember to close manually
f = path.open("rt")
content = f.read()
f.close()  # Easy to forget!
```

### Streaming Mode Limitations

When using `FileCacheMode.streaming`, certain CloudPath features are not available because streaming mode doesn't create cached files on disk:

**Not Available:**
- `.fspath` property - Raises `CloudPathNotImplementedError`
- `.__fspath__()` method - Raises `CloudPathNotImplementedError`
- Passing CloudPath as `os.PathLike` to libraries that need file paths

**Workaround:**
Use `CloudPath.open()` and pass the file-like object to libraries that accept file handles instead of file paths.

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode
import pandas as pd

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/data.csv", client=client)

# ❌ This will raise an error in streaming mode
# df = pd.read_csv(path.fspath)

# ✅ Use this instead - pass the open file handle
with path.open("rt") as f:
    df = pd.read_csv(f)
```

## Compatibility

### Python I/O Interfaces

The streaming I/O classes are fully compatible with Python's I/O hierarchy:

```python
import io
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)

path = S3Path("s3://bucket/file.bin", client=client)
with path.open("rb") as f:
    assert isinstance(f, io.IOBase)
    assert isinstance(f, io.BufferedIOBase)

path = S3Path("s3://bucket/file.txt", client=client)
with path.open("rt") as f:
    assert isinstance(f, io.IOBase)
    assert isinstance(f, io.TextIOBase)
```

### Third-Party Libraries

Works with any library that accepts file-like objects:

- **Data processing**: pandas, NumPy, PyArrow
- **Images**: PIL/Pillow, OpenCV
- **Compression**: gzip, zipfile, tarfile
- **Serialization**: pickle, json, yaml
- **Scientific**: h5py, netCDF4

## Troubleshooting

### "File not found" errors

Ensure the file exists and you have read permissions:

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/file.txt", client=client)

if path.exists():
    with path.open("rt") as f:
        content = f.read()
```

### Slow performance

Try increasing buffer size:

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/file.txt", client=client)

# Larger buffer for faster networks
with path.open("rb", buffer_size=1024*1024) as f:
    data = f.read()
```

### Out of memory

Try smaller buffer size or process in chunks:

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/huge.bin", client=client)

# Process large file in chunks
with path.open("rb", buffer_size=8192) as f:
    while chunk := f.read(8192):
        process_chunk(chunk)
```

## Migration Guide

### From Cached to Streaming

Before:

```python
from cloudpathlib import S3Path

path = S3Path("s3://bucket/file.txt")
with path.open("rt") as f:  # Downloads to cache
    content = f.read()
```

After:

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

# Option 1: Set on client initialization
client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/file.txt", client=client)
with path.open("rt") as f:  # Streams directly
    content = f.read()

# Option 2: Change client mode
client = S3Client()
path = S3Path("s3://bucket/file.txt", client=client)

path.client.file_cache_mode = FileCacheMode.streaming
with path.open("rt") as f:  # Streams directly
    content = f.read()
```

## Advanced Topics

### Custom Clients

Pass custom clients with specific configurations:

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode
from botocore.config import Config

# Custom S3 client with retry configuration
client = S3Client(
    file_cache_mode=FileCacheMode.streaming,
    boto3_config=Config(
        retries={'max_attempts': 10, 'mode': 'adaptive'}
    )
)

path = S3Path("s3://bucket/file.txt", client=client)
with path.open("rt") as f:
    content = f.read()
```

### Multiple Files

Process multiple files efficiently:

```python
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
bucket = S3Path("s3://bucket/", client=client)

for file_path in bucket.glob("*.csv"):
    with file_path.open("rt") as f:
        process_csv(f)
```

### Encoding Detection

For files with unknown encoding:

```python
import chardet
from cloudpathlib import S3Path, S3Client
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/file.txt", client=client)

# Read a small sample to detect encoding
with path.open("rb") as f:
    sample = f.read(10000)
    detected = chardet.detect(sample)
    encoding = detected['encoding']

# Re-open with detected encoding
with path.open("rt", encoding=encoding) as f:
    content = f.read()
```

### Context Manager for Temporary Streaming

Use a context manager to temporarily enable streaming mode:

```python
from contextlib import contextmanager
from cloudpathlib import S3Client
from cloudpathlib.enums import FileCacheMode

@contextmanager
def streaming_mode(client):
    """Temporarily enable streaming mode on a client."""
    original_mode = client.file_cache_mode
    try:
        client.file_cache_mode = FileCacheMode.streaming
        yield client
    finally:
        client.file_cache_mode = original_mode

# Usage
client = S3Client()
path = S3Path("s3://bucket/file.txt", client=client)

with streaming_mode(client):
    with path.open("rt") as f:
        content = f.read()  # Uses streaming

# Back to cached mode
with path.open("rt") as f:
    content = f.read()  # Uses caching
```
