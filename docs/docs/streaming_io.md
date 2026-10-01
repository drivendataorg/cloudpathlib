# Streaming I/O

By default, `CloudPath.open()` downloads a file to the local cache before opening it and
uploads the whole cache file when a written handle is closed. With
`FileCacheMode.streaming`, `open()` instead returns a standard Python file object that
reads from cloud storage with ranged requests and writes to it with multipart uploads.
Nothing is written to disk, only the part of the object you read is downloaded, and
written data is uploaded while you write it.

## Enabling streaming

Set `file_cache_mode` on the client (or the `CLOUDPATHLIB_FILE_CACHE_MODE=streaming`
environment variable):

```python
from cloudpathlib import S3Client, S3Path
from cloudpathlib.enums import FileCacheMode

client = S3Client(file_cache_mode=FileCacheMode.streaming)
path = S3Path("s3://bucket/data.csv", client=client)

with path.open("r") as f:          # text, ranged reads
    for line in f:
        ...

with path.open("wb") as f:         # binary, multipart upload
    f.write(b"...")
```

Streaming is supported for S3, Azure Blob Storage, Google Cloud Storage, and HTTP/HTTPS,
as well as the `cloudpathlib.local` mock clients used in tests.

## What `open()` returns

The same objects the builtin `open()` returns, so any library that accepts a file object
works unchanged: `json`, `csv`, `zipfile`, `tarfile`, `pandas`, `pyarrow`, `PIL`, etc.

| Mode | Returns |
|------|---------|
| `rb`, `wb`, `xb` | `io.BufferedReader` / `io.BufferedWriter` over a provider raw stream |
| `r`, `w`, `x` (text) | `io.TextIOWrapper` over the buffered stream |
| binary with `buffering=0` | the raw `io.RawIOBase` stream itself |

Read streams are seekable, so formats that need random access (zip archives, parquet)
work: `pyarrow` seeks to the parquet footer and then fetches only the column chunks you
ask for.

```python
import pyarrow.parquet as pq

with path.open("rb") as f:
    table = pq.ParquetFile(f).read(columns=["user_id"])
```

Write streams are sequential: `seek()` on a write stream raises `io.UnsupportedOperation`.

## `buffering`

`buffering` means what it means for the builtin `open()`:

- `-1` (default): a 5 MiB buffer. Each refill of a read stream is one ranged request of
  that size (never past the end of the object), so a sequential scan of a 1 GiB object is
  about 205 requests.
- `N > 1`: the buffer size in bytes. Use a smaller buffer (64 KiB to 1 MiB) when you read
  small slices of large objects (parquet column reads, headers), and a larger one for
  sequential scans on fast networks.
- `1`: line buffering for text streams, with the default buffer size.
- `0`: unbuffered; binary only. Every `read()`/`write()` is a request (or a part-buffer
  append).

A full-object `read()` is always a single request regardless of the buffer size.

```python
with path.open("rb", buffering=16 * 1024 * 1024) as f:   # 16 MiB requests
    data = f.read()
```

## Concurrency

Each client has a `streaming_max_concurrency` setting (default 4) that bounds the number
of requests one open stream may have in flight:

- **Reads:** once two consecutive reads are sequential, the next byte ranges are fetched
  in the background while you consume the current one. A single read (for example of a
  header) or a seek does not trigger read-ahead.
- **Writes:** completed parts upload in the background while you keep writing; a
  `write()` blocks only when that many parts are already in flight, so buffered memory is
  bounded by roughly `streaming_max_concurrency` times the part size.

Pass `streaming_max_concurrency=1` for fully sequential I/O.

```python
client = S3Client(file_cache_mode=FileCacheMode.streaming, streaming_max_concurrency=8)
```

Streams are not thread-safe, like ordinary file objects: open one per thread. Any number
of streams may read the same object at once. Concurrent writers to the same object are
last-closed-wins; each writer's upload is isolated, so they cannot corrupt each other's
data.

## Writes

Object stores cannot append to or modify an existing object, so only whole-object writes
stream:

| Mode | Behaviour |
|------|-----------|
| `w`, `wb`, `x`, `xb` | Streamed: data is uploaded in parts as you write; the object appears on `close()`. |
| `a`, `ab`, `r+`, `w+`, ... | Fall back to the local cache: the object is downloaded, modified locally, re-uploaded on `close()`, and the cache file is then removed (as with `close_file`). Correct, but costs a full download and upload. |

Details of a streamed write:

- Data that fits in one part (5 MiB on S3 and GCS, 4 MiB on Azure) is uploaded with a
  single `PUT` on close. Larger writes use a multipart (S3, GCS XML API) or block (Azure)
  upload; part sizes grow for very large streams so the provider's part-count limit is
  never hit.
- If a write or the final upload fails, the multipart upload is aborted and the error
  propagates from `write()` or `close()`; no partial object is left behind. If the abort
  itself fails, a `RuntimeWarning` names the object so you can clean up.
- `force_overwrite_to_cloud` (and `CLOUDPATHLIB_FORCE_OVERWRITE_TO_CLOUD`) work as in
  cached mode: when the object changed while the stream was open and overwriting is not
  forced, `close()` raises `OverwriteNewerCloudError`. An `x` mode stream raises
  `CloudPathFileExistsError` if the object appeared while it was open, whatever the flag.
- The content type is guessed from the object name with the client's
  `content_type_method`, exactly as for cached uploads.
- HTTP servers have no multipart upload, so an HTTP write is one request on `close()`;
  the body is held in memory up to 5 MiB and spooled to a temporary file beyond that.

## Limitations

- **No `fspath`.** `os.fspath(path)` and `path.fspath` raise `CloudPathNotImplementedError`
  in streaming mode because there is no local file. Pass the open file object to libraries
  instead of the path; if a library requires a filesystem path, use a cached mode.
- **`copy`, `rename`, and `replace`** between different clients stream from one object to
  the other instead of going through the cache.
- **HTTP reads** need a server that honours `Range` requests (a `200` response is only
  accepted for reads from the start of the object). Servers that omit `Content-Length`
  work, but `seek(..., SEEK_END)` raises `CloudPathStreamingError`.
- **Size lookups.** A read stream fetches the object's size once (a metadata request). If
  that lookup fails (for example, a policy that allows `GET` but not `HEAD`), reading still
  works; only `SEEK_END` raises, with the lookup error as its cause.

## Errors

Streaming raises the same exception types as the rest of cloudpathlib:
`CloudPathFileNotFoundError` for a missing object, `CloudPathFileExistsError` for `x`
modes, `OverwriteNewerCloudError` for write conflicts, and `CloudPathStreamingError` (also
an `OSError`) when a request cannot be completed, such as exceeding the provider's
part-count limit or an HTTP server rejecting a write.

## Provider notes

| Provider | Reads | Writes |
|----------|-------|--------|
| S3 (and S3-compatible) | `GetObject` with `Range` | `PutObject` or multipart upload; `extra_args` are forwarded to each operation that accepts them |
| Azure Blob Storage | `download_blob(offset, length)` | `upload_blob` or staged blocks committed on close; block IDs are unique per stream |
| Google Cloud Storage | `download_as_bytes(start, end)` | `upload_from_file` or the XML API multipart upload (concurrent parts). Set an `AbortIncompleteMultipartUpload` lifecycle rule on the bucket to expire uploads orphaned by a crash |
| HTTP/HTTPS | `GET` with `Range` | one request using the client's `write_file_http_method` |

## Adding streaming to a custom client

A `Client` subclass gets streaming by implementing the hooks `_range_download`,
`_put_object`, and (for multipart uploads) `_initiate_multipart_upload`, `_upload_part`,
`_complete_multipart_upload`, and `_abort_multipart_upload`, then setting
`_streaming_raw_class` to `cloudpathlib.cloud_io._CloudMultipartStorageRaw` (or
`_CloudSpooledStorageRaw` for single-request uploads) and the `_multipart_*` part limits.
Subclasses of the built-in clients inherit all of this. A client that leaves
`_streaming_raw_class` as `None` raises `CloudPathNotImplementedError` from `open()` in
streaming mode and works normally in the cached modes.
