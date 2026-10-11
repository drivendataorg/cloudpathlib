# Concurrent cache access

A persistent cache can reuse downloaded files across sequential sessions and
processes. It does not coordinate concurrent access to the same cached file.
This applies both to `local_cache_dir` and to the
`CLOUDPATHLIB_LOCAL_CACHE_DIR` environment variable.

## Accessing the same file from multiple workers

Cloudpathlib does not lock cache files while downloading, reading, writing, or
uploading them. If two workers access the same cloud object through a shared
cache, one may encounter a partially downloaded file or an
`OverwriteNewerLocalError` while the other is still downloading. The behavior
can depend on the provider and operation. Setting
`force_overwrite_from_cloud=True` does not coordinate the workers.

For independent work, create a client and cache directory inside each task.
Tasks can then read the same cloud object without sharing its local cache.
The example below creates a fresh cache for each task invocation, so repeated
reads download again. This prevents tasks from using or clearing each other's
cache files. Concurrent writes to the same cloud object still need
coordination at the application level.

```python
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory

from cloudpathlib import S3Client


def read_in_worker(uri):
    with TemporaryDirectory() as cache_dir:
        client = S3Client(local_cache_dir=cache_dir, file_cache_mode="persistent")
        return client.CloudPath(uri).read_bytes()


if __name__ == "__main__":
    paths = ["s3://bucket/first.csv", "s3://bucket/first.csv"]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for data in executor.map(read_in_worker, paths):
            print(len(data))
```

The explicit `local_cache_dir` overrides `CLOUDPATHLIB_LOCAL_CACHE_DIR`, so each
task remains isolated even if that environment variable is set. The explicit
cache mode also overrides `CLOUDPATHLIB_FILE_CACHE_MODE`. The temporary
cache is removed after the read finishes. The same approach can be used with
GCS or Azure clients, or with a process pool: create the client and cache inside
the worker, and keep pool startup under the `__main__` guard.

## Reusing a shared persistent cache

Explicitly set `file_cache_mode="persistent"` when sharing a cache directory,
and coordinate any cache cleanup. Other cache modes can remove cached files
when a file, path, or client is closed or released.

Workers operating on different cloud objects do not contend for the same
cached file. Use separate caches for different providers, whose bucket or
container names may otherwise map to the same local paths.

If workers must share a cache for the same object, coordinate access outside
cloudpathlib, such as with a lock shared by all processes that every worker respects.
Hold the lock for the whole operation, including opening the file, using it,
and closing it so any upload finishes before another worker accesses the cache.
A lock that only protects client construction is insufficient.

See [Caching](caching.ipynb) for cache modes and cleanup behavior, and
[Streaming I/O](streaming_io.md) for reads that avoid the local cache. Append and
update operations in streaming mode still fall back to the cache.
