# Concurrent cache access

Parallelize by cloud object. Workers that read or write different cloud objects
can share one persistent cache. Give each thread its own `CloudPath`; one
`Client` can be shared across threads. Each process should construct its own
client pointed at the same `local_cache_dir` with `file_cache_mode="persistent"`.
This also applies when setting the cache directory with the
`CLOUDPATHLIB_LOCAL_CACHE_DIR` environment variable.

## Different objects with a shared persistent cache

Create a client once for a thread pool and pass different object URIs to the
workers. Each worker creates its own path using that client:

```python
from concurrent.futures import ThreadPoolExecutor

from cloudpathlib import S3Client


def read_in_worker(uri):
    return client.CloudPath(uri).read_bytes()


if __name__ == "__main__":
    client = S3Client(local_cache_dir="cache", file_cache_mode="persistent")
    paths = ["s3://bucket/first.csv", "s3://bucket/second.csv"]
    with ThreadPoolExecutor(max_workers=4) as executor:
        for data in executor.map(read_in_worker, paths):
            print(len(data))
```

For a process pool, pass URI strings and construct the client inside the worker.
The processes can reuse the same cache directory when accessing different objects:

```python
from concurrent.futures import ProcessPoolExecutor

from cloudpathlib import S3Client


def read_in_worker(uri):
    client = S3Client(local_cache_dir="cache", file_cache_mode="persistent")
    return client.CloudPath(uri).read_bytes()


if __name__ == "__main__":
    paths = ["s3://bucket/first.csv", "s3://bucket/second.csv"]
    with ProcessPoolExecutor(max_workers=4) as executor:
        for data in executor.map(read_in_worker, paths):
            print(len(data))
```

Do not pass a `Client` or `CloudPath` into the process pool. Pickling a `CloudPath`
removes its `_client`, so the child uses its process's default client, which may
differ from the parent's configured client. Constructing the client in the subprocess keeps its
credentials, endpoint, and cache settings explicit.

Explicitly set `file_cache_mode="persistent"` when sharing a cache directory,
and coordinate any cache cleanup. Other cache modes can remove cached files
when a file, path, or client is closed or released.

Different objects must also map to different local cache paths. The cache key is
the cloud path with its scheme removed; for HTTP, it is the URL path with the
host removed. Bucket or container names and keys can therefore collide across
providers, accounts, or endpoints, and HTTP URLs on different hosts can collide
when their URL paths match. Use separate cache directories for these namespaces.

## Accessing the same file from multiple workers

Cloudpathlib does not lock cache files while downloading, reading, writing, or
uploading them. If two workers access the same cloud object through a shared
cache, one may encounter a partially downloaded file or an
`OverwriteNewerLocalError` while the other is still downloading. The behavior
can depend on the provider and operation. Setting
`force_overwrite_from_cloud=True` does not coordinate the workers.

If multiple workers need the same object, use a separate cache per worker or
coordinate access with an external lock. For separate caches, create a client
and cache directory inside each task.
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

If workers must share a cache for the same object, coordinate access outside
cloudpathlib, such as with a lock shared by all processes that every worker respects.
Hold the lock for the whole operation, including opening the file, using it,
and closing it so any upload finishes before another worker accesses the cache.
A lock that only protects client construction is insufficient.

See [Caching](caching.ipynb) for cache modes and cleanup behavior, and
[Streaming I/O](streaming_io.md) for reads that avoid the local cache. Append and
update operations in streaming mode still fall back to the cache.
